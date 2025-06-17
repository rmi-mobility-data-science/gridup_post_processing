import os
import pandas as pd
import geopandas as gpd
import duckdb
from shapely import wkt
from dask import delayed, compute


# ---------------------------------------------
# Function Definitions
# ---------------------------------------------
def create_convex_hull(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    wkt_col: str,
    geom_col: str,
):
    """Create convex hull geometries for aggregation
       boundariesfrom WKT geometries

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Boundary table
        wkt_col (str): Column with wkt geometries
        geom_col (str): Column with geometries

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with
        convex hull geometries
    """
    query = f"""
    SELECT *,
           ST_ConvexHull(ST_GeomFromText({wkt_col})) AS {geom_col}
    FROM {input_table};
    """
    return con.sql(query)


def convert_geom_to_wkt(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    geom_col: str,
    wkt_col: str,
):
    """Convert the geometry column to WKT

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Input table
        geom_col (str): Column with geometries
        wkt_col (str): Column with wkt geometries

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with
        WKT geometries
    """
    query = f"""
    SELECT
        NAME,
        ST_AsText({geom_col}) AS {wkt_col}
    FROM {input_table};
    """
    return con.sql(query)


def convert_polygon_to_h3(
    con: duckdb.DuckDBPyConnection,
    input_table: str,
    wkt_col: str,
    h3_col: str,
):
    """Convert aggregation polygon geometries to
       H3 indexes at level 8

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Input table
        wkt_col (str): Column with wkt geometries
        h3_col (str): Column with h3 indexes

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with
        h3 indexes
    """
    query = f"""
    SELECT
        *,
        {wkt_col},
        UNNEST(h3_polygon_wkt_to_cells_string({wkt_col}, 8)) AS {h3_col}
    FROM {input_table}
    """
    return con.sql(query)


def extract_relevant_cells(
    con: duckdb.DuckDBPyConnection,
    load_curves_files: str,
    convex_hull_table: str,
    load_identity_col: str,
    h3_col: str,
):
    """Extract cells from load curves table that are
       part of aggregation boundaries convex hulls.

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        load_curves_files: Path to load curve parquet files
        convex_hull_table: Table containing H3 cells
        of convex hulls
        load_identity_col: Column name (in load_curves_table)
        with H3 cell IDs
        h3_col: Column name (in convex_hull_table)
        with H3 cell IDs

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation
        with relevant load cells
    """
    query = f"""
        SELECT * 
        FROM '{load_curves_files}'
        WHERE {load_identity_col} in (select {h3_col} from {convex_hull_table})
        """
    return con.sql(query)


def reproject_polygons(
    input_polygons: pd.DataFrame, wkt_col: str, original_crs: int, target_crs: int
) -> gpd.GeoDataFrame:
    """Convert from WGS84 to US National Atlas Equal Area

    Args:
        input_polygons (str): Input dataframe with polygon geometries
        wkt_col (str): Column with wkt geometries
        original_crs (int): Original coordinate reference system (CRS) EPSG code
        target_crs (int): Target coordinate reference system (CRS) EPSG code

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with reprojected geometries
        and calculated areas
    """
    input_polygons["geometry"] = input_polygons[wkt_col].apply(wkt.loads)
    output_polygons = gpd.GeoDataFrame(
        input_polygons, geometry="geometry", crs=original_crs
    )
    output_polygons = output_polygons.to_crs(epsg=target_crs)
    return output_polygons


def extract_unique_load_cells(
    con: duckdb.DuckDBPyConnection, input_table: str, cell_col: str
):
    """Extract unique load cells from the query result

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Input table with load cells
        cell_col (str): Column name for load cells

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation
        with unique cells
    """
    query = f"""
    SELECT DISTINCT {cell_col}
    FROM {input_table}
    """
    return con.sql(query)


def convert_h3_to_wkt(
    con: duckdb.DuckDBPyConnection, input_table: str, h3_col: str, h3_wkt: str
):
    """Convert H3 indexes to WKT

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Input table with H3 indexes
        h3_col (str): H3 column name
        h3_wkt (str): Name for the transformed WKT column

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation
        with H3 indexes converted to WKT
    """
    query = f"""
    SELECT
    *,
    h3_cell_to_boundary_wkt({h3_col}) AS {h3_wkt}
    FROM {input_table}
    """
    return con.sql(query)


def find_intersection_area(
    boundaries_gdf: gpd.GeoDataFrame, load_cells_gdf: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Find intersection areas between aggregation boundaries
       and load cells

    Args:
        boundaries_gdf (gpd.GeoDataFrame): Geodataframe of aggregation boundaries
        load_cells_gdf (gpd.GeoDataFrame): Geodataframe of load cells

    Returns:
        pd.DataFrame: DataFrame with intersection areas
        and the geometries removed
    """
    intersection = gpd.overlay(
        boundaries_gdf, load_cells_gdf, how="intersection", keep_geom_type=False
    )
    intersection["intersection_area"] = intersection.area
    intersection = intersection.drop(columns=["geometry"])
    return intersection


def distribute_weights(
    con: duckdb.DuckDBPyConnection, input_table: str, load_identity_col: str
):
    """Distribute weights focusing on cells that are
       part of more than one aggregation boundary

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): duckdb table with intersection areas
        load_identity_col (str): Column name for load identity

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with weights calculated
    """
    query = f"""
        WITH with_ratios AS (
            SELECT *,
                intersection_area / SUM(intersection_area) OVER (PARTITION BY {load_identity_col}) AS ratio
            FROM {input_table}
        )
        -- Aggregate over polygons that are part of a boundary multi-polygon
        SELECT
            {load_identity_col},
            NAME,
            SUM(ratio) AS ratio_sum
        FROM with_ratios
        GROUP BY {load_identity_col}, NAME
        ORDER BY {load_identity_col}, NAME
    """
    return con.sql(query)


def parallel_join(
    load_table_path: str, intersection_table_path: str, load_identity_col: str
) -> pd.DataFrame:
    """Join aggregation boundary table with load curves table for each boundary

    Args:
        load_table_path (str): Path to the load curves table
        intersection_table_path (str): Path to the intersection table
        load_identity_col: Column name for load identity

    Returns:
        pd.DataFrame: DataFrame containing the joined results
    """

    def process_group(group_val):
        # Create a new DuckDB connection inside each Dask process
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        # Load both tables
        con.execute(
            f"CREATE TEMP TABLE load_table AS SELECT * FROM '{load_table_path}'"
        )
        con.execute(
            f"CREATE TEMP TABLE intersection_table AS SELECT * FROM '{intersection_table_path}'"
        )

        # Filter and join
        query = f"""
        SELECT 
            load_table.*,
            intersection_table.* 
        FROM load_table
        INNER JOIN (
            SELECT * FROM intersection_table WHERE NAME = ?
        ) AS intersection_table
        ON load_table.{load_identity_col} = intersection_table.{load_identity_col}
        """
        result = con.execute(query, [group_val]).fetchdf()
        con.close()
        return result

    # This part is outside parallelism — create a temp connection to get unique groups
    con = duckdb.connect()
    con.execute(
        f"CREATE TEMP TABLE intersection_table AS SELECT * FROM '{intersection_table_path}'"
    )
    unique_agg_boundary_names = (
        con.sql("SELECT DISTINCT NAME FROM intersection_table").df()["NAME"].tolist()
    )
    con.close()

    # Run in parallel
    tasks = [delayed(process_group)(g) for g in unique_agg_boundary_names]
    results = compute(*tasks, scheduler="processes", num_workers=4)

    # Combine all results
    return pd.concat(results, ignore_index=True)


def generate_results(con, input_table: str):
    """Apply weight ratios and aggregate to generate results
       by scenario, name, year, hour and charge category

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection
        input_table (str): Table containing joined data

    Returns:
        duckdb.DuckDBPyRelation: DuckDB relation with detailed results.
        Detailed because charge category is also included
    """
    agg_cols = [str(i) for i in range(24)]
    weighted_sums = ",\n    ".join(
        [f'SUM("{col}" * ratio_sum) AS "{col}"' for col in agg_cols]
    )
    query = f"""
    SELECT
        NAME,
        charge_category,
        scenario,
        year,
        {weighted_sums},
        CEILING(SUM(ports * ratio_sum)) AS ports
    FROM {input_table}
    GROUP BY NAME, charge_category, scenario, year
    ORDER BY NAME, charge_category, scenario, year 
    """
    return con.sql(query)


# ---------------------------------------------
# Configuration
# ---------------------------------------------
INPUTS_PATH = os.path.join("data", "inputs")
OUTPUTS_PATH = os.path.join("data", "outputs", "census_places")
BOUNDARIES_DIR = os.path.join(INPUTS_PATH, "aggregation_boundaries")
BOUNDARIES_PATH = os.path.join(BOUNDARIES_DIR, "filtered_census_places.parquet")
LOAD_CURVES_FILES = os.path.join(INPUTS_PATH, "evolved_*/*.parquet")

# Column names
BOUNDARY_WKT_COL = "boundary_wkt"
BOUNDARY_GEOM_COL = "boundary_geom"
H3_COL = "h3_index"
H3_WKT = "h3_wkt"
LOAD_IDENTITY_COL = "geoid"

ORIGINAL_CRS = 4326  # WGS84

# ---------------------------------------------
# Load and prepare boundary data
# ---------------------------------------------
boundary_df = pd.read_parquet(BOUNDARIES_PATH)
if "geometry" in boundary_df.columns:
    boundary_df = boundary_df.drop(columns=["geometry"])
states = boundary_df["STATE"].unique().tolist()

for state in states:
    print(f"Processing state: {state}")
    state_outputs_path = os.path.join(OUTPUTS_PATH, state)
    if not os.path.isdir(state_outputs_path):
        os.makedirs(state_outputs_path, exist_ok=True)
    state_boundary_df = boundary_df[boundary_df["STATE"] == state].copy()
    # ---------------------------------------------
    # Set up DuckDB connection
    # ----------------------------------------------
    sc_con = duckdb.connect()
    sc_con.execute("INSTALL spatial; LOAD spatial;")
    sc_con.execute("INSTALL h3 FROM community; LOAD h3;")
    sc_con.register("aggregation_boundary_input", state_boundary_df)

    # ---------------------------------------------
    # Preprocessing pipeline
    # ---------------------------------------------
    convex_table = create_convex_hull(
        con=sc_con,
        input_table="aggregation_boundary_input",
        wkt_col=BOUNDARY_WKT_COL,
        geom_col=BOUNDARY_GEOM_COL,
    )

    wkt_table = convert_geom_to_wkt(
        con=sc_con,
        input_table="convex_table",
        geom_col=BOUNDARY_GEOM_COL,
        wkt_col=BOUNDARY_WKT_COL,
    )

    convex_hull_h3 = convert_polygon_to_h3(
        con=sc_con,
        input_table="wkt_table",
        wkt_col=BOUNDARY_WKT_COL,
        h3_col=H3_COL,
    )

    # ---------------------------------------------
    # Load curves setup
    # ---------------------------------------------
    # Relevant load cells are within convex hulls of aggregation boundaries
    relevant_load_cells = extract_relevant_cells(
        con=sc_con,
        load_curves_files=LOAD_CURVES_FILES,
        convex_hull_table="convex_hull_h3",
        load_identity_col=LOAD_IDENTITY_COL,
        h3_col=H3_COL,
    )
    relevant_load_cells.to_parquet(
        os.path.join(OUTPUTS_PATH, "relevant_load_cells.parquet")
    )

    # ---------------------------------------------
    # Prepare intersection table
    # ---------------------------------------------
    if state not in ["HI", "AK"]:
        target_crs = 5070  # Albers Equal Area Projection
    elif state == "HI":
        target_crs = 3563  # Hawaii Albers Equal Area Conic
    elif state == "AK":
        target_crs = 3338  # Alaska Albers Equal Area Conic
    boundary_transformed = reproject_polygons(
        input_polygons=state_boundary_df,
        wkt_col=BOUNDARY_WKT_COL,
        original_crs=ORIGINAL_CRS,
        target_crs=target_crs,
    )

    # Prepare  unique load cells as we care only about geographies
    sc_con.register("relevant_load_cells", relevant_load_cells)
    unique_load_cells = extract_unique_load_cells(
        con=sc_con, input_table="relevant_load_cells", cell_col=LOAD_IDENTITY_COL
    )
    unique_load_cells = convert_h3_to_wkt(
        con=sc_con,
        input_table="unique_load_cells",
        h3_col=LOAD_IDENTITY_COL,
        h3_wkt=H3_WKT,
    )
    unique_load_cells_df = unique_load_cells.fetchdf()
    unique_load_cells_transformed = reproject_polygons(
        input_polygons=unique_load_cells_df,
        wkt_col=H3_WKT,
        original_crs=ORIGINAL_CRS,
        target_crs=target_crs,
    )

    intersection_df = find_intersection_area(
        boundaries_gdf=boundary_transformed,
        load_cells_gdf=unique_load_cells_transformed,
    )

    sc_con.register("intersection_table", intersection_df)
    intersection_table_with_weights = distribute_weights(
        con=sc_con,
        input_table="intersection_table",
        load_identity_col=LOAD_IDENTITY_COL,
    )
    intersection_table_with_weights.to_parquet(
        os.path.join(OUTPUTS_PATH, "intersection_table.parquet")
    )

    # ---------------------------------------------
    # Parallel join & final aggregation
    # ---------------------------------------------
    joined_df = parallel_join(
        load_table_path=os.path.join(OUTPUTS_PATH, "relevant_load_cells.parquet"),
        intersection_table_path=os.path.join(
            OUTPUTS_PATH, "intersection_table.parquet"
        ),
        load_identity_col=LOAD_IDENTITY_COL,
    )

    full_results = generate_results(con=sc_con, input_table="joined_df")

    # ---------------------------------------------
    # Save results and remove temporary tables
    # ---------------------------------------------
    os.remove(os.path.join(OUTPUTS_PATH, "intersection_table.parquet"))
    os.remove(os.path.join(OUTPUTS_PATH, "relevant_load_cells.parquet"))
    full_results.to_parquet(os.path.join(state_outputs_path, "full_results.parquet"))
    sc_con.close()
