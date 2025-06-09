import os
import cenpy
import pygris
import pandas as pd


# ---------------------------------------------
# Configuration
# ---------------------------------------------
POPULATION_THRESHOLD = 10_000
AREA_THRESHOLD = 2_220_000  # Minimum area in square meters (~3x H3 res 8 cell)
VALID_LSAD_CODES = [
    "25",
    "48",
    "70",
    "57",
    "43",
    "06",
    "28",
]  # Relevant legal/statistical place types
boundaries_directory = os.path.join("inputs", "aggregation_boundaries")


# ---------------------------------------------
# Step 1: Load State-Level FIPS Codes
# ---------------------------------------------
# Load state-level FIPS codes
# List of state FIPS codes and names (including DC)
fips_data = [
    ("01", "Alabama", "AL"),
    ("02", "Alaska", "AK"),
    ("04", "Arizona", "AZ"),
    ("05", "Arkansas", "AR"),
    ("06", "California", "CA"),
    ("08", "Colorado", "CO"),
    ("09", "Connecticut", "CT"),
    ("10", "Delaware", "DE"),
    ("11", "District of Columbia", "DC"),
    ("12", "Florida", "FL"),
    ("13", "Georgia", "GA"),
    ("15", "Hawaii", "HI"),
    ("16", "Idaho", "ID"),
    ("17", "Illinois", "IL"),
    ("18", "Indiana", "IN"),
    ("19", "Iowa", "IA"),
    ("20", "Kansas", "KS"),
    ("21", "Kentucky", "KY"),
    ("22", "Louisiana", "LA"),
    ("23", "Maine", "ME"),
    ("24", "Maryland", "MD"),
    ("25", "Massachusetts", "MA"),
    ("26", "Michigan", "MI"),
    ("27", "Minnesota", "MN"),
    ("28", "Mississippi", "MS"),
    ("29", "Missouri", "MO"),
    ("30", "Montana", "MT"),
    ("31", "Nebraska", "NE"),
    ("32", "Nevada", "NV"),
    ("33", "New Hampshire", "NH"),
    ("34", "New Jersey", "NJ"),
    ("35", "New Mexico", "NM"),
    ("36", "New York", "NY"),
    ("37", "North Carolina", "NC"),
    ("38", "North Dakota", "ND"),
    ("39", "Ohio", "OH"),
    ("40", "Oklahoma", "OK"),
    ("41", "Oregon", "OR"),
    ("42", "Pennsylvania", "PA"),
    ("44", "Rhode Island", "RI"),
    ("45", "South Carolina", "SC"),
    ("46", "South Dakota", "SD"),
    ("47", "Tennessee", "TN"),
    ("48", "Texas", "TX"),
    ("49", "Utah", "UT"),
    ("50", "Vermont", "VT"),
    ("51", "Virginia", "VA"),
    ("53", "Washington", "WA"),
    ("54", "West Virginia", "WV"),
    ("55", "Wisconsin", "WI"),
    ("56", "Wyoming", "WY"),
]
state_df = pd.DataFrame(fips_data, columns=["FIPS", "State Name", "Abbreviation"])


# ---------------------------------------------
# Step 2: Load ACS Place-Level Population Data
# ---------------------------------------------
# Set up connection to ACS 2023 5-Year Detailed Tables
conn = cenpy.remote.APIConnection("ACSDT5Y2023")

# Query population for all places in the selected state
states_dict = dict.fromkeys(state_df["Abbreviation"], None)
for row in state_df.itertuples():
    print(f"Processing state: {row.Abbreviation} ({row.FIPS})")
    state_fips = row.FIPS
    state_abbreviation = row.Abbreviation

    acs_cols = ["NAME", "B01003_001E"]  # Total population
    acs_data = conn.query(
        cols=acs_cols, geo_unit="place:*", geo_filter={"state": state_fips}
    )

    # Rename and convert population column
    acs_data = acs_data.rename(columns={"B01003_001E": "population"})
    acs_data["population"] = pd.to_numeric(acs_data["population"], errors="coerce")

    # Filter by population threshold
    acs_data = acs_data[acs_data["population"] > POPULATION_THRESHOLD].copy()

    # Construct GEOID and select relevant columns
    acs_data["GEOID"] = acs_data["state"] + acs_data["place"]
    acs_data = acs_data[["GEOID", "population"]]
    acs_data["state_abbreviation"] = state_abbreviation

    # ---------------------------------------------
    # Step 3: Load Spatial Boundaries
    # ---------------------------------------------
    spatial_data = pygris.places(state=state_fips, year=2023, cb=True)
    spatial_data.set_crs("EPSG:4269", inplace=True)  # Set CRS to NAD83
    spatial_data = spatial_data.to_crs("EPSG:4326")  # Convert to WGS84
    spatial_data["boundary_wkt"] = spatial_data.geometry.to_wkt().astype(str)

    # Merge ACS data with spatial boundaries
    merged = spatial_data.merge(acs_data, on="GEOID", how="inner")

    # ---------------------------------------------
    # Step 4: Filter by LSAD and Area
    # ---------------------------------------------
    filtered = merged[
        merged["LSAD"].isin(VALID_LSAD_CODES) & (merged["ALAND"] > AREA_THRESHOLD)
    ]

    # ---------------------------------------------
    # Final Result
    # ---------------------------------------------
    # You now have a GeoDataFrame `filtered` with:
    # - Reasonably populated places
    # - Relevant place types (cities, towns, CDPs, etc.)
    # - Excluded very small geographic areas

    # Optional preview
    filtered[["NAME", "GEOID", "population", "LSAD", "boundary_wkt", "geometry"]]
    # Explode multipolygons to polygons
    polygons_filtered = filtered.explode(ignore_index=True)

    polygons_filtered["is_valid"] = polygons_filtered.is_valid
    invalid_gdf = polygons_filtered[~polygons_filtered["is_valid"]]

    if invalid_gdf.shape[0] != 0:
        print(f"Invalid geometries found for state '{state_abbreviation}'. Skipping.")
        continue

    states_dict[state_abbreviation] = polygons_filtered


# ---------------------------------------------
# Step 5: Combine Results
# ---------------------------------------------
all_places = pd.concat(states_dict.values(), ignore_index=True)
# Save to a geo-parquet file
output_path = os.path.join(boundaries_directory, "filtered_places.parquet")
all_places.to_parquet(output_path, index=False)
