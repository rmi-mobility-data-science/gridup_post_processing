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
boundaries_directory = os.path.join("data", "inputs", "aggregation_boundaries")


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
    acs_cols = [
        "NAME",
        "B01003_001E",
        "B23025_005E",
        "B23025_003E",
        "B19001_002E",
        "B19001_003E",
        "B19001_004E",
        "B19001_005E",
        "B19001_006E",
        "B19001_007E",
        "B19001_008E",
        "B19001_009E",
        "B19001_010E",
        "B19001_011E",
        "B19001_012E",
        "B19001_013E",
        "B19001_014E",
        "B19001_015E",
        "B19001_016E",
        "B19001_017E",
        "B15003_022E",
        "B15003_023E",
        "B15003_025E",
        "B15003_001E",
    ]
    acs_data = conn.query(
        cols=acs_cols, geo_unit="place:*", geo_filter={"state": state_fips}
    )

    # Rename and convert population column
    acs_data = acs_data.rename(
        columns={
            "B01003_001E": "total_pop",
            "B23025_005E": "unemployed",
            "B23025_003E": "civilian_labor",
            "B19001_002E": "income_less_10k",
            "B19001_003E": "income_10k_15k",
            "B19001_004E": "income_15k_20k",
            "B19001_005E": "income_20k_25k",
            "B19001_006E": "income_25k_30k",
            "B19001_007E": "income_30k_35k",
            "B19001_008E": "income_35k_40k",
            "B19001_009E": "income_40k_45k",
            "B19001_010E": "income_45k_50k",
            "B19001_011E": "income_50k_60k",
            "B19001_012E": "income_60k_75k",
            "B19001_013E": "income_75k_100k",
            "B19001_014E": "income_100k_125k",
            "B19001_015E": "income_125k_150k",
            "B19001_016E": "income_150k_200k",
            "B19001_017E": "income_more_200k",
            "B15003_022E": "bachelors",
            "B15003_023E": "masters",
            "B15003_025E": "doctorate",
            "B15003_001E": "pop_above_25",
        }
    )
    acs_data[acs_data.columns.difference(["NAME", "state", "place"])] = acs_data[
        acs_data.columns.difference(["NAME", "state", "place"])
    ].apply(pd.to_numeric, errors="coerce")

    # Filter by population threshold
    acs_data = acs_data[acs_data["total_pop"] > POPULATION_THRESHOLD].copy()

    # Preprocess place names to remove unnecessary suffixes
    acs_data["NAME"] = acs_data["NAME"].str.replace(r"\s+(city|CDP)", "", regex=True)
    acs_data["NAME"] = acs_data["NAME"].apply(lambda x: x.split(",")[0])

    # Check if there are records with repetitive names
    if acs_data["NAME"].duplicated().any():
        print(f"Warning: Duplicate place names found in {state_abbreviation}.")
    else:
        print(f"No duplicate place names found in {state_abbreviation}.")

    # Construct GEOID and select relevant columns
    acs_data["GEOID"] = acs_data["state"] + acs_data["place"]

    # ---------------------------------------------
    # Step 3: Load Spatial Boundaries
    # ---------------------------------------------
    places_gdf = pygris.places(state=state_fips, year=2023, cb=True)
    places_gdf.set_crs("EPSG:4269", inplace=True)  # Set CRS to NAD83
    places_gdf = places_gdf.to_crs("EPSG:4326")  # Convert to WGS84
    places_gdf = places_gdf[["GEOID", "STUSPS", "LSAD", "ALAND", "geometry"]]

    # Merge ACS data with spatial boundaries
    merged = places_gdf.merge(acs_data, on="GEOID", how="inner")

    # ---------------------------------------------
    # Step 4: Filter by LSAD and Area
    # ---------------------------------------------
    filtered = merged[
        merged["LSAD"].isin(VALID_LSAD_CODES) & (merged["ALAND"] > AREA_THRESHOLD)
    ].copy()

    # ---------------------------------------------
    # Final Result
    # ---------------------------------------------
    # You now have a GeoDataFrame `filtered` with:
    # - Reasonably populated places
    # - Relevant place types (cities, towns, CDPs, etc.)
    # - Excluded very small geographic areas

    # Subset columns
    filtered.rename(columns={"STUSPS": "STATE"}, inplace=True)

    # Explode multipolygons to polygons
    # polygons_filtered = filtered.explode(ignore_index=True)
    filtered["boundary_wkt"] = filtered.geometry.to_wkt().astype(str)

    filtered["is_valid"] = filtered.is_valid
    invalid_gdf = filtered[~filtered["is_valid"]]

    if invalid_gdf.shape[0] != 0:
        print(f"Invalid geometries found for state '{state_abbreviation}'. Skipping.")
        continue

    states_dict[state_abbreviation] = filtered


# ---------------------------------------------
# Step 5: Combine Results
# ---------------------------------------------
all_places = pd.concat(states_dict.values(), ignore_index=True)
# Save to a parquet file
output_path = os.path.join(boundaries_directory, "filtered_census_places.parquet")
all_places.to_parquet(output_path, index=False)
