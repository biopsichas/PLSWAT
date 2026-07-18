# PLSWAT: River Modeling System for Poland

[![R](https://img.shields.io/badge/R-4.x-276DC3?logo=r)](https://www.r-project.org/)
[![Python](https://img.shields.io/badge/Python-3.8.6-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostGIS](https://img.shields.io/badge/PostgreSQL-PostGIS-336791?logo=postgresql)](https://postgis.net/)
[![SWAT+](https://img.shields.io/badge/Model-SWAT%2B-2E7D32)](https://swat.tamu.edu/software/plus/)

## Table of Contents
- [Project Overview](#project-overview)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Data Directory Structure](#data-directory-structure)
- [Key Functionalities](#key-functionalities)
- [Usage](#usage)
- [Data Population Overview](#data-population-overview)
- [Safety Warning](#-safety-warning)

## Project Overview

**PLSWAT** is a comprehensive river modeling repository designed to automate the generation, population, and execution of **SWAT+** modeling setups for Nemunas basin in Poland (~2000 km²). It is an adaptation of the **LTSWAT** (Lithuanian River Modeling System, [GitHub](https://github.com/biopsichas/LTSWAT) & [Documentation](https://doi.org/10.5281/zenodo.11092543)), using standardized workflows to ensure consistent water quality and quantity modeling — particularly in transboundary catchments.

At the core of the project, `main.R` automates the creation, transformation, and ingestion of spatial and tabular data into a PostgreSQL/PostGIS database required for running SWAT+ models. It processes shapefiles, CSVs, and rasters to ensure the database schema aligns with the project's requirements.

The script acts as an **ETL (Extract, Transform, Load) pipeline**: it connects to a source database (Lithuanian SWAT model parameters) and a target database, mapping and harmonizing data between the two.

## Repository Structure

| Path | Description |
| --- | --- |
| `Data/` | All primary input datasets — Polish-specific geospatial (DEM, soil, land use) and thematic (meteorological, point source) data. Also required as input for `main_GIS_PL.py`, the primary script used to populate the PostgreSQL/PostGIS database. |
| `main.R`, `connect.R`, `function.R` | Core execution logic. These R scripts manage database connections and drive the modeling workflow. |

## Prerequisites

**R environment** with the following packages:

| Package | Purpose |
| --- | --- |
| [`sf`](https://r-spatial.github.io/sf/) | Geospatial data manipulation |
| [`DBI`](https://dbi.r-dbi.org/) | Database connectivity |
| `bit64` | Handling 64-bit integer IDs |
| [`terra`](https://rspatial.github.io/terra/) / [`exactextractr`](https://isciences.gitlab.io/exactextractr/) | Raster processing |
| *custom utilities* | Loaded via `source("connect.R")` |

**Database:** A PostgreSQL instance with the **PostGIS** extension enabled.

## Data Directory Structure

To run `main.R` successfully, organize your `Data/` directory as follows. Each folder holds the shapefiles, rasters, and CSVs required for database ingestion.

```text
📁 PLSWAT/                                        (Main root repository)
├── 📁 Data/                                      (Required input directory for main.R)
│   ├── 📁 AtmosphericDeposition/                 (GeoJSON and precipitation inputs)
│   ├── 📁 BufferZones/                           (Shapefiles for buffer strip analysis)
│   ├── 📁 Catchments/                            (River network / watershed .shp and .csv files)
│   ├── 📁 Counties/                              (Administrative boundaries and raster IDs)
│   ├── 📁 DEM/                                   (Digital Elevation Model files)
│   ├── 📁 Drainage/                              (Managed drainage raster and lookup files)
│   ├── 📁 HRU/                                   (Hydrological Response Units & elevation)
│   ├── 📁 Lakes/                                 (Lake/reservoir spatial and physical data)
│   ├── 📁 LU/                                    (Land use rasters .tif and lookup tables)
│   ├── 📁 Meteo/                                 (Meteorological station and climate data)
│   ├── 📁 Obs/                                   (Observed data for calibration/validation)
│   └── 📁 Soil/                                  (Soil parameter lookup tables)
├── 📄 .gitattributes                             (Git configuration)
├── 📄 .gitignore                                 (File exclusion patterns)
├── 📄 .Renviron                                  (Connection details to PostgreSQL database)
├── 📄 PLSWAT.Rproj                               (R Project file)
├── 📄 connect.R                                  (Database connection scripts)
├── 📄 function.R                                 (Custom utility functions)
└── 📄 main.R                                     (Core ETL processing script)
```

## Key Functionalities

The script performs the following operations sequentially:

1. **Database Connection** — Establishes connections to the source (`lt_con`) and target (`pl_con`) databases.
2. **Spatial Data Processing** — Reads shapefiles, reprojects to **EPSG:2180**, handles geometry, and calculates spatial attributes (centroids, area, etc.).
3. **Table Ingestion** — Loads processed data into the database, defining table constraints (primary keys, geometry types) for:
   - **Catchments & Rivers** — watershed boundaries, river networks, and outlet definitions
   - **Land Use & Soil** — lookup tables connecting raster IDs to SWAT codes
   - **Atmospheric Deposition** — spatial data for precipitation and N/P deposition
   - **HRU & Management** — buffer zones, elevation data, livestock units, and yield calculations

## Usage

1. Ensure your `Data/` directory contains all necessary source files (see [Data Directory Structure](#data-directory-structure)).
2. Verify database credentials in `connect.R`, which should be sourced from `.Renviron` in the same folder.
3. Run `main.R` in an R environment.

> **Tip:** It's recommended to run sections incrementally, or use the commented-out `compare_columns` functions to validate schema compatibility before finalizing writes to the database.

## Data Population Overview

The table below summarizes the source and integration logic for each dataset processed by `main.R`. The script populates the SWAT modeling database (`pl_con`) by harmonizing local spatial/tabular files with parameters inherited from the legacy Lithuanian modeling system (`lt_con`).

**Data source key:**
- **Data folder** — Raw files (shapefiles, CSVs, GeoTIFFs) in the project's local `Data/` directory.
- **Empty dataframe** — Tables initialized as empty placeholders for future modeling runs or user-defined inputs.
- **`lt_con`** — Tables fetched directly from the legacy Lithuanian SWAT database, providing baseline parameters and model constants.

| Table Name | Schema | Population Source |
| --- | --- | --- |
| `catchments` | `catchments` | Data folder (`WatershedsPL_corrected2.shp`) |
| `riversegments` | `catchments` | Data folder (`RiversPLcorrected.shp`) |
| `outlets` | `catchments` | Data folder (`outlets.csv`) |
| `watersheds` | `catchments` | Data folder (`watersheds.csv`) |
| `catchm_wshed` | `catchments` | Data folder (`catchm_wshed.csv`) |
| `watertransfer` | `catchments` | Empty dataframe |
| `landuse_swat_raster_lookup` | `landuse` | Data folder (`LU/lookup.csv`) |
| `landusegroup_raster_lookup` | `landuse` | Empty dataframe |
| `soil_swat_raster_lookup` | `soil` | Data folder (`soil/Lookup_soil.csv`) |
| `usersoilpl` | `swat2012` | Data folder + `lt_con` |
| `drainage_raster_lookup` | `mel_dr10lt` | Empty dataframe |
| `atmdep2022` | `atm_deposition` | Data folder (`AtmDep2022.geojson`) |
| `buffzones_stat` | `hru` | Data folder (`BufferZones/bufferstrips.shp`) |
| `node_elevation` | `hru` | Data folder (`HRU/node_elevation.shp`) |
| `counties_raster_lookup` | `counties` | Data folder (`Counties/counties.shp`) |
| `counties` | `counties` | Data folder (`Counties/counties.shp`) |
| `polygons` | `bufferstrips` | Data folder (`BufferZones/bufferstrips.shp`) |
| `plant_data` | `fert` | `lt_con` |
| `plants_luclass` | `fert` | `lt_con` |
| `lakesreservoirs` | `hru` | Empty dataframe |
| `precip_by_catch` | `hru` | Empty dataframe |
| `gri_water_catchments` | `rivers` | Empty dataframe |
| `segment_depth` | `rivers` | Empty dataframe |
| `segmentmanningcoef` | `rivers` | Empty dataframe |
| `statpcp` | `obs` | `lt_con` |
| `catchments_regions` | `hru` | Empty dataframe |
| `ps_catchment` | `point_sources_zero` | Empty dataframe |
| `livestock_data` | `management` | Data folder (`LU/lugroups.tif`) |
| `yield_data` | `management` | `lt_con` + Data folder (`Counties/counties.shp`) |

## ⚠️ Safety Warning

The script contains commands such as `DBI::dbRemoveTable`, which **permanently delete data** from the target database schema. Exercise caution when running these — double-check the target connection (`pl_con` vs. `lt_con`) and consider taking a database backup before executing destructive operations.
