import cenpy
import pygris
import pandas as pd


# ---------------------------------------------
# Configuration
# ---------------------------------------------
STATE_FIPS = "36"  # New York
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


# ---------------------------------------------
# Step 1: Load ACS Place-Level Population Data
# ---------------------------------------------
# Set up connection to ACS 2023 5-Year Detailed Tables
conn = cenpy.remote.APIConnection("ACSDT5Y2023")

# Query population for all places in the selected state
acs_cols = ["NAME", "B01003_001E"]  # Total population
acs_data = conn.query(
    cols=acs_cols, geo_unit="place:*", geo_filter={"state": STATE_FIPS}
)

# Rename and convert population column
acs_data = acs_data.rename(columns={"B01003_001E": "population"})
acs_data["population"] = pd.to_numeric(acs_data["population"], errors="coerce")

# Filter by population threshold
acs_data = acs_data[acs_data["population"] > POPULATION_THRESHOLD]

# Construct GEOID and select relevant columns
acs_data["GEOID"] = acs_data["state"] + acs_data["place"]
acs_data = acs_data[["GEOID", "population"]]


# ---------------------------------------------
# Step 2: Load Spatial Boundaries
# ---------------------------------------------
spatial_data = pygris.places(state=STATE_FIPS, year=2023, cb=True)

# Merge ACS data with spatial boundaries
merged = spatial_data.merge(acs_data, on="GEOID", how="inner")


# ---------------------------------------------
# Step 3: Filter by LSAD and Area
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
print(filtered[["NAME", "GEOID", "population", "LSAD", "ALAND"]].head())
