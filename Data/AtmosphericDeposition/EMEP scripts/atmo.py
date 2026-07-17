# -*- coding: utf-8 -*-
"""
Atmospheric Deposition 2022
EMEP MSC-W modelled air concentrations and depositions
https://www.emep.int/mscw/mscw_ydata.html
Yearly values for year 2022 (2022 emissions)
"""

import numpy as np
import matplotlib.pyplot as plt
import xarray
import geopandas as gpd
from shapely.geometry import Polygon

# --- File paths ---
atmDepFile = 'EMEPData/EMEP01_rv5.3_year.2023met_2022emis.nc'
outFile = 'Data/AtmDep2022.geojson'

# --- Load dataset ---
ds = xarray.open_dataset(atmDepFile)

# --- Clip to Lithuania bounding box ---
lon_index = (ds.lon >= 22.6476) & (ds.lon <= 23.6417)
lat_index = (ds.lat >= 53.6779) & (ds.lat <= 54.4715)

longitudes_cell_center = ds.lon[lon_index]
latitudes_cell_center  = ds.lat[lat_index]

delta_lon = float(longitudes_cell_center[1] - longitudes_cell_center[0])
delta_lat = float(latitudes_cell_center[1]  - latitudes_cell_center[0])

longitudes_cell_borders = np.append(
    longitudes_cell_center - delta_lon / 2,
    [longitudes_cell_center[-1] + delta_lon / 2]
)
latitudes_cell_borders = np.append(
    latitudes_cell_center - delta_lat / 2,
    [latitudes_cell_center[-1] + delta_lat / 2]
)

# --- Variables to extract ---
# (output_name, EMEP_variable_name)
variables = [
    ("precipitation", "WDEP_PREC"),
    ("NH4_dry",       "DDEP_RDN_m2Grid"),
    ("NH4_wet",       "WDEP_RDN"),
    ("NO3_dry",       "DDEP_OXN_m2Grid"),
    ("NO3_wet",       "WDEP_OXN"),
]

# --- Build fishnet grid (one polygon per EMEP cell) ---
lon, lat = np.meshgrid(longitudes_cell_center, latitudes_cell_center)

longitudes_rect = np.moveaxis(
    np.array([lon - delta_lon/2, lon + delta_lon/2,
              lon + delta_lon/2, lon - delta_lon/2]), 0, -1)
latitudes_rect = np.moveaxis(
    np.array([lat - delta_lat/2, lat - delta_lat/2,
              lat + delta_lat/2, lat + delta_lat/2]), 0, -1)

longitudes_rect = longitudes_rect.reshape(-1, longitudes_rect.shape[2])
latitudes_rect  = latitudes_rect.reshape(-1, latitudes_rect.shape[2])

fishnet = np.dstack([longitudes_rect, latitudes_rect])
fishnet_polygons = [Polygon(p) for p in fishnet]

# --- Build GeoDataFrame and attach deposition values ---
gdf = gpd.GeoDataFrame(geometry=fishnet_polygons, crs="EPSG:4326")

for name, emep_var in variables:
    df = ds[emep_var]
    gdf[name] = np.array(df[0][lat_index, :][:, lon_index]).flatten()

# --- Reproject to LKS-94 (Lithuanian national CRS) and save ---
gdf = gdf.to_crs('EPSG:2180')
gdf['Shape_Area'] = gdf.area
gdf.to_file(outFile, driver='GeoJSON')
print(f"Saved: {outFile}")

# --- Quick plot of last variable ---
df_last = ds[variables[-1][1]]
fig, ax = plt.subplots(figsize=(20, 10))
ax.pcolormesh(
    longitudes_cell_borders,
    latitudes_cell_borders,
    df_last[0][lat_index, :][:, lon_index],
    shading='flat'
)
ax.grid()
ax.set_title(variables[-1][0])
plt.tight_layout()
plt.show()

# --- Close dataset ---
ds.close()