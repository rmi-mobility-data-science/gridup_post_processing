# ---------------------------------------------
# Import libraries and dependencies
# ---------------------------------------------
import os
import geopandas as gpd
import pandas as pd
from shapely import wkt


# ---------------------------------------------
# Functions
# ---------------------------------------------
def filter_utilities_by_area(
    input_gdf: gpd.geodataframe, target_crs: int, area_threshold: float
) -> gpd.geodataframe:
    """
    Filters a GeoDataFrame to include only geometries with an area greater than a specified threshold.
    Args:
        input_gdf (gpd.geodataframe): Input GeoDataFrame containing geometries to be filtered
        target_crs (int): The target coordinate reference system (CRS)
        area_threshold (float): Minimum area threshold for filtering geometries
    Returns:
        gpd.geodataframe: A filtered GeoDataFrame containing only
        geometries with an area greater than the specified threshold.
    """
    transformed_gdf = input_gdf.copy()
    transformed_gdf = input_gdf.to_crs(target_crs)
    transformed_gdf["area"] = transformed_gdf.geometry.area
    filtered_gdf = input_gdf[transformed_gdf["area"] > area_threshold]
    return filtered_gdf


# ---------------------------------------------
# Define paths and read inputs
# ---------------------------------------------
inputs_path = "inputs"
boundaries_directory = os.path.join(inputs_path, "aggregation_boundaries")
coordinate_system_epsg = 4326  # WGS 84
national_atlas_epsg = 2163  # National Atlas Equal Area Projection
area_threshold = 2220000


# ---------------------------------------------
# Main program
# ---------------------------------------------
# Read the aggregation boundary file
boundaries_path = os.path.join(
    boundaries_directory, "cleaned_service_territories.parquet"
)
aggregation_boundary_gdf = gpd.read_parquet(boundaries_path)


# Prepare the aggregation boundary DataFrame
aggregation_boundary_gdf = aggregation_boundary_gdf.drop(columns=["Name"])
aggregation_boundary_gdf.set_crs(coordinate_system_epsg, inplace=True)

aggregation_boundary_gdf = filter_utilities_by_area(
    aggregation_boundary_gdf, national_atlas_epsg, area_threshold
)

# Break multipolygons to polygons for all utility names
utilities = aggregation_boundary_gdf["NAME"].unique()
utilities_dict = dict.fromkeys(utilities, None)
for utility in utilities:
    cur_aggregation_boundary_gdf = aggregation_boundary_gdf.loc[
        aggregation_boundary_gdf.NAME == utility, :
    ]

    cur_aggregation_boundary_gdf = cur_aggregation_boundary_gdf.explode(
        ignore_index=True
    )

    cur_aggregation_boundary_gdf["is_valid"] = cur_aggregation_boundary_gdf.is_valid
    invalid_gdf = cur_aggregation_boundary_gdf[~cur_aggregation_boundary_gdf["is_valid"]]

    if invalid_gdf.shape[0] != 0:
        print(f"Invalid geometries found for utility '{utility}'. Skipping.")
        continue

    # Create a wkt column and drop the geometry column
    cur_aggregation_boundary_gdf["boundary_wkt"] = (
        cur_aggregation_boundary_gdf.geometry.to_wkt().astype(str)
    )

    cur_aggregation_boundary_gdf = cur_aggregation_boundary_gdf.drop(
        ["geometry"], axis=1
    )

    # Store the utility-specific DataFrame in the dictionary
    utilities_dict[utility] = cur_aggregation_boundary_gdf

# Save the resulting DataFrame to a CSV file
utilities_df = pd.concat(utilities_dict.values(), ignore_index=True)

boundaries_path = os.path.join(
    boundaries_directory, "utility_service_territories_polygons.parquet"
)
utilities_df.to_parquet(boundaries_path, index=False)


