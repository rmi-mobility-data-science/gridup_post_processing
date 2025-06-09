import cenpy
import pygris
import pandas as pd
import matplotlib.pyplot as plt

# Select state
state_fips = "36"
pop_threshold = 10000
area_threshold = 2220000  # Almost triple the size of h3 level 8 cells
LSAD_codes = ["25", "48", "70", "57", "43", "06", "28"]


# Set up cenpy connection to 2023 ACS 5-Year Data
conn = cenpy.remote.APIConnection("ACSDT5Y2023")

# Define variables: owner-occupied housing and mean commute time
cols = ["NAME", "B01003_001E"]  # Total population

# Query all places in the state
acs_data = conn.query(cols=cols, geo_unit="place:*", geo_filter={"state": state_fips})

# Rename columns
acs_data = acs_data.rename(columns={"B01003_001E": "tot_population"})

# Convert numeric columns
acs_data["tot_population"] = pd.to_numeric(acs_data["tot_population"])


# Filter based on the population threshold
sel_acs_data = acs_data[(acs_data["tot_population"] > pop_threshold)]

# Extract GEOID for merging
sel_acs_data["GEOID"] = sel_acs_data["state"] + sel_acs_data["place"]
sel_acs_data = sel_acs_data[["GEOID", "tot_population"]]

spatial_boundaries_places = pygris.places(state=state_fips, year=2023, cb=True)

# Merge with spatial data
spatial_boundaries_places = spatial_boundaries_places.merge(
    sel_acs_data, left_on="GEOID", right_on="GEOID", how="inner"
)

spatial_boundaries_places = spatial_boundaries_places[
    spatial_boundaries_places.LSAD.isin(LSAD_codes)
]

# Filter based on the area threshold
spatial_boundaries_places = spatial_boundaries_places[
    spatial_boundaries_places["ALAND"] > area_threshold
]

# Plot the data
# Get state boundary using pygris
state_boundary = pygris.states(year=2023, cb=True)
state_boundary = state_boundary[state_boundary["STATEFP"] == state_fips]

# Plot the state boundary with the selected places
fig, ax = plt.subplots(figsize=(12, 12))
state_boundary.to_crs(epsg=3857).plot(
    ax=ax, color="none", edgecolor="black", linewidth=1
)
spatial_boundaries_places.to_crs(epsg=3857).plot(
    ax=ax,
    column="tot_population",
    cmap="plasma",
    legend=True,
    legend_kwds={
        "label": "Population",
        "orientation": "horizontal",
        "shrink": 0.6,
        "pad": 0.02,
    },
    edgecolor="black",
    linewidth=0.5,
)

# Add title
ax.set_title("Selected Places with Population > 10,000", fontsize=16, pad=20)

# Remove axes
ax.set_axis_off()

# Improve layout
plt.tight_layout()
plt.show()
