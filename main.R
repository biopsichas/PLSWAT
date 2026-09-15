## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 0) Connecting to databases, loading functions and packages -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

source("connect.R")
library(bit64)

## Get SWAT LT info
lt_info <- get_db_info(lt_con)

## Get SWAT PL info
pl_info <- get_db_info(pl_con)

# vacum_db(pl_con) ## Super dangerous because it wipes pit all the database

data_path <- "Data/"

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 1) catchments.catchments -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

## Prepare the catchment data with the same structure as the LT table
catchments_pl <- st_read(paste0(data_path, "Catchments/WatershedsPL_corrected2.shp"), quiet = TRUE) |>
  st_transform(2180) |>
  rename(lakegid = lakeid,
         kadastroid_lake= kadastro_1) |>
  mutate(catchmentid = as.integer64(id),
         type = as.integer64(type),
         segmentid = as.integer64(segmentid),
         flowto = as.integer64(flowto),
         segmentto = as.integer64(segmentto),
         outletid = as.integer64(outletid),
         inletid = as.integer64(NA),
         wbriver_code = NA_character_,
         wlake_wb = NA_character_,
         shape = geometry,
         x = st_coordinates(st_centroid(geometry))[, 1],
         y = st_coordinates(st_centroid(geometry))[, 2],
         lon = st_coordinates(st_transform(st_centroid(geometry), 4326))[, 1],
         lat = st_coordinates(st_transform(st_centroid(geometry), 4326))[, 2],
         area = st_area(geometry) |> as.numeric())|>
  sf::st_drop_geometry() |>
  st_set_geometry("shape") |>
  select(all_of(c(
    "catchmentid", "shape", "type", "segmentid", "lakegid", "kadastroid", "kadastroid_lake",
    "flowto", "segmentto", "outletid", "addedarea", "inflowarea", "wbriver_code", "wlake_wb",
    "inletid", "x", "y", "lon", "lat", "area"
  )))

## Converting ids from long to short
all_ids <- unique(catchments_pl$catchmentid) |> sort()
lookup_tbl <- tibble(old_id = c(all_ids, -1), new_id = c(seq_along(all_ids), -1))

id_cols <- c("catchmentid", "segmentid", "flowto", "segmentto")
catchments_pl <- catchments_pl |>
  mutate(across(all_of(id_cols), ~ as.integer64(replace_with_lookup(.x))))

# catchments_lt <- load_table(lt_con, "catchments", "catchments")
# compare_columns(catchments_lt, catchments_pl)

table_constraint <- "
  catchmentid BIGINT PRIMARY KEY,
  shape geometry(MultiPolygon, 2180),
  type BIGINT,
  segmentid BIGINT,
  lakegid TEXT,
  kadastroid TEXT,
  kadastroid_lake TEXT,
  flowto BIGINT,
  segmentto BIGINT,
  outletid BIGINT,
  addedarea DOUBLE PRECISION,
  inflowarea DOUBLE PRECISION,
  wbriver_code TEXT,
  wlake_wb TEXT,
  inletid BIGINT,
  x DOUBLE PRECISION,
  y DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  lat DOUBLE PRECISION,
  area DOUBLE PRECISION
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "catchments", table = "catchments"))
write_in_table(pl_con, "catchments", "catchments", catchments_pl, table_constraint = table_constraint)


## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 2) catchments.riversegments -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

rivers_pl <- st_read(paste0(data_path, "Catchments/RiversPLcorrected.shp"), quiet = TRUE) |>
  st_transform(2180) |>
  rename(segmentid = id,
         flowto = RiverTo,
         shape = geometry,
         skip_catchment = skip_catch,
         length = SHAPE_Leng,
         kadastroid_lake = kadastro_1) |>
  mutate(nodefrom = as.integer(segmentid),
         nodeto = as.integer(flowto),
         segmentid = as.integer64(segmentid),
         flowto = as.integer64(flowto),
         skip_catchment = as.logical(skip_catchment),
         kadastroid = NA_character_,
         wbriver_code = NA_character_) |>
  select(any_of(c(
    "segmentid", "flowto", "shape", "skip_catchment", "kadastroid",
    "kadastroid_lake", "wbriver_code", "wlake_wb", "length", "nodefrom",
    "nodeto"
  )))

id_cols <- c("segmentid", "flowto", "nodefrom", "nodeto")

rivers_pl <- rivers_pl |>
  mutate(across(all_of(id_cols), ~ replace_with_lookup(.x))) |>
  mutate(across(all_of(c("segmentid", "flowto")),~ as.integer64(.x)))

# rivers_pl <- load_table(pl_con, "catchments", "riversegments")
# compare_columns(rivers_lt, rivers_pl)

table_constraint <- "
  segmentid BIGINT PRIMARY KEY,
  flowto BIGINT,
  shape geometry(MultiLineString, 2180),
  skip_catchment BOOLEAN,
  kadastroid TEXT,
  kadastroid_lake TEXT,
  wbriver_code TEXT,
  wlake_wb TEXT,
  length DOUBLE PRECISION,
  nodefrom BIGINT,
  nodeto BIGINT
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "catchments", table = "riversegments"))
write_in_table(pl_con, "catchments", "riversegments", rivers_pl, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 3) catchments.outlets -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

outlet_pl <- read.csv(paste0(data_path, "Catchments/outlets.csv"), header = FALSE) |>
  `names<-`(c("index", "id", "outlettype", "outletname")) |>
  mutate(index = as.integer64(index),
         id = as.integer64(id),
         outlettype = as.integer64(outlettype))

# outlet_lt <- load_table(lt_con, "catchments", "outlets")
# compare_columns(outlet_lt, outlet_pl)

table_constraint <- "
  index BIGINT,
  id BIGINT PRIMARY KEY,
  outlettype BIGINT,
  outletname TEXT
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "catchments", table = "outlets"))
write_in_table(pl_con, "catchments", "outlets", outlet_pl, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 4) catchments.watersheds -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

watersheds_pl <- read.csv(paste0(data_path, "Catchments/watersheds.csv"), header = FALSE) |>
  `names<-`(c("watershedid", "basinname", "watershedname", "composite")) |>
  mutate(watershedid = as.integer64(watershedid))

# watersheds_lt <- load_table(lt_con, "catchments", "watersheds")
# compare_columns(watersheds_lt, watersheds_pl)

table_constraint <- "
  watershedid BIGINT PRIMARY KEY,
  basinname TEXT,
  watershedname TEXT,
  composite TEXT
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "catchments", table = "watersheds"))
write_in_table(pl_con, "catchments", "watersheds", watersheds_pl, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 5) catchments.catchm_wshed -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

catchm_wshed_pl <- read.csv(paste0(data_path, "Catchments/catchm_wshed.csv"), header = FALSE) |>
  `names<-`(c("catchmentid", "wshid")) |>
  mutate(catchmentid = as.integer64(catchmentid),
         watershedid = as.integer64(wshid)) |>
  select(-wshid)

catchm_wshed_pl <- catchm_wshed_pl |>
  mutate(across(all_of("catchmentid"), ~ as.integer64(replace_with_lookup(.x))))

table_constraint <- "
  catchmentid BIGINT,
  watershedid BIGINT
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "catchments", table = "catchm_wshed"))
write_in_table(pl_con, "catchments", "catchm_wshed", catchm_wshed_pl, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 6) catchments.watertransfer -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

transfer_pl <- data.frame(
  id = integer64(0),
  name = character(0),
  outletid = integer64(0),
  flowto = integer64(0),
  multiplier = numeric(0)
)

# transfer_lt <- load_table(lt_con, "catchments", "watertransfer")
# compare_columns(transfer_lt, transfer_pl)

table_constraint <- "
  id BIGINT PRIMARY KEY,
  name TEXT,
  outletid  BIGINT,
  flowto BIGINT,
  multiplier DOUBLE PRECISION
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "catchments", table = "watertransfer"))
write_in_table(pl_con, "catchments", "watertransfer", transfer_pl, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 7) landuse.landuse_swat_raster_lookup -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

lu_pl_lookup <- read.csv(paste0(data_path, "LU/lookup.csv"), header = TRUE) |>
  rename(swatcode = lu_SWATcode,
         raster_id = gridCode) |>
  select(swatcode, raster_id) |>
  mutate(swatcode = toupper(swatcode),
         raster_id = as.integer64(raster_id))

# lu_lt_landuse_swat_raster_lookup <- load_table(lt_con, "landuse", "landuse_swat_raster_lookup")
# compare_columns(lu_lt_landuse_swat_raster_lookup, lu_pl_lookup)

table_constraint <- "
  swatcode TEXT,
  raster_id BIGINT
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "landuse", table = "landuse_swat_raster_lookup"))
write_in_table(pl_con, "landuse", "landuse_swat_raster_lookup", lu_pl_lookup,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 8) landuse.landusegroup_raster_lookup -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

lu_pl_landusegroup_raster_lookup <- data.frame(
  luclass = c("Agricultural", "Barren", "Forest", "Pasture", "Urban", "Water", "Wetland"),
  raster_id = 1:7
) |>
  mutate(raster_id = as.integer64(raster_id))

# lu_lt_landusegroup_raster_lookup <- load_table(lt_con, "landuse", "landusegroup_raster_lookup")
# compare_columns(lu_lt_landusegroup_raster_lookup, lu_pl_landusegroup_raster_lookup)

table_constraint <- "
  luclass TEXT,
  raster_id BIGINT
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "landuse",
#                                    table = "landusegroup_raster_lookup"))
write_in_table(pl_con, "landuse", "landusegroup_raster_lookup",
               lu_pl_landusegroup_raster_lookup,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 9) soil.soil_swat_raster_lookup -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

## Prepare the soil data with the same structure as the LT table from the raw
## data supplied by Ignacy.
# soil <- sf::read_sf(paste0(data_path, "Soil/soil100th_1_catchent/soil100th_1_catchent.shp")) |>
#   group_by(Aggreg1) |>
#   summarise() |>
#   arrange(Aggreg1) |>
#   mutate(raster_id = row_number())
#
# library(terra)
# r_template <- rast(paste0(data_path, "DEM/dem.tif"))
# r_rasterized <- rasterize(soil, r_template, field = "raster_id")
# writeRaster(r_rasterized, paste0(data_path, "Soil/soil100th_v1.tif"),
#             datatype = "INT1U", gdal = c("COMPRESS=DEFLATE", "PREDICTOR=2"), overwrite = TRUE)
#
# soil_link <- soil |>
#   st_drop_geometry() |>
#   rename(swatcode = Aggreg1)
#
# library(readxl)
# soil_data <- readxl::read_excel(paste0(data_path, "Soil/soil100th_1_catchent/final_usersoil_PL.xlsx"))
# soil_lookup_lookup <- left_join(soil_link, soil_data, by = c("swatcode" = "snam")) |>
#   rename(id = raster_id,
#          snam = swatcode) |>
#   select(all_of(c(
#     "nlayers", "snam", "hydgrp", "anion_excl", "sol_zmx", "sol_crk", "sol_bd1", "sol_z1", "sol_awc1", "sol_cbn1", "sol_k1",
#     "clay1", "sand1", "silt1", "rock1", "sol_alb1", "sol_ec1", "usle_k1", "sol_bd2", "sol_z2", "sol_awc2", "sol_cbn2",
#     "sol_k2", "clay2", "sand2", "silt2", "rock2", "sol_ec2", "usle_k2", "sol_bd3", "sol_z3", "sol_awc3", "sol_cbn3",
#     "sol_k3", "clay3", "sand3", "silt3", "rock3", "sol_ec3", "usle_k3", "sol_no31", "sol_no32", "sol_no33", "sol_orgn1",
#     "sol_orgn2", "sol_labp1", "sol_orgn3", "sol_labp2", "sol_labp3", "sol_orgp1", "sol_orgp2", "prerco_sub", "sol_orgp3", "sol_alb2", "sol_ph1",
#     "id", "sol_alb3"
#   ))) |>
#   mutate(snam = ifelse(is.na(snam), "JEZ", snam)) |>
#   mutate(across(where(is.integer), as.numeric))
#
# unique(soil_lookup_lookup$snam)[!soil_lookup_lookup$snam %in% unique(soil_link$swatcode)]
# unique(soil_link$swatcode)[!unique(soil_link$swatcode) %in% soil_lookup_lookup$snam]
#
# write.csv(soil_lookup_lookup, paste0(data_path, "Soil/Lookup_soil.csv"), row.names = FALSE)

## Load the soil lookup table and prepare it for insertion into the PL database
soil_pl_lookup <- read.csv(paste0(data_path, "soil/Lookup_soil.csv"), header = TRUE) |>
  mutate(across(where(is.integer), as.numeric))|>
  mutate(id = as.integer(id),
         snam = gsub("/", "-", snam))

soil_swat_raster_lookup_pl <- soil_pl_lookup |>
  rename(swatcode = snam, raster_id = id) |>
  mutate(raster_id = as.integer64(raster_id)) |>
  select(swatcode, raster_id)

# soil_swat_raster_lookup <- load_table(lt_con, "soil", "soil_swat_raster_lookup")
# compare_columns(soil_swat_raster_lookup, soil_swat_raster_lookup_pl)

table_constraint <- "
  swatcode TEXT,
  raster_id BIGINT
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "soil", table = "soil_swat_raster_lookup"))
write_in_table(pl_con, "soil", "soil_swat_raster_lookup", soil_swat_raster_lookup_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 10) swat2012.usersoilpl -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

usersoil_lt <- load_table(lt_con, "swat2012", "usersoillt")
usersoil_pl <- load_table(pl_con, "swat2012", "usersoilpl")
# compare_columns(usersoil_lt, soil_pl_lookup)
# names(usersoil_lt)[!names(usersoil_lt) %in% names(usersoil_pl)]
# names(usersoil_pl)[!names(usersoil_pl) %in% names(usersoil_lt)]

soil_pl_lookup[soil_pl_lookup$snam == "JEZ", c(
  "nlayers", "hydgrp", "anion_excl", "sol_zmx", "sol_crk",
  "sol_bd1", "sol_z1", "sol_awc1", "sol_cbn1", "sol_k1", "clay1", "sand1",
  "silt1", "rock1", "sol_alb1", "sol_ec1", "usle_k1",
  "sol_bd2", "sol_z2", "sol_awc2", "sol_cbn2", "sol_k2", "clay2", "sand2",
  "silt2", "rock2", "sol_ec2", "usle_k2",
  "sol_bd3", "sol_z3", "sol_awc3", "sol_cbn3", "sol_k3", "clay3", "sand3",
  "silt3", "rock3", "sol_ec3", "usle_k3",
  "sol_no31", "sol_no32", "sol_no33",
  "sol_orgn1", "sol_orgn2", "sol_orgn3",
  "sol_labp1", "sol_labp2", "sol_labp3",
  "sol_orgp1", "sol_orgp2", "sol_orgp3",
  "prerco_sub", "sol_alb2", "sol_ph1", "sol_alb3"
)] <- list(
  3, "C", 0.5, 1359.744, 0.5,
  1.279544, 365.3619, 0.1360489, 3.799722, 131.5778, 15.07685, 57.36058,
  27.56257, 5.538032, 0.1414166, 0, 0.313336,
  1.307702, 925.4653, 0.1201969, 1.771131, 108.4053, 15.28346, 60.30565,
  24.41092, 5.642797, 0, 0.326817,
  1.201043, 1359.744, 0.1116929, 0.4014221, 75.38837, 15.69175, 52.47609,
  22.52983, 5.137738, 0, 0.3432196,
  12.39735, 4.677668, 2.612997,
  1239.744, 467.7761, 261.2997,
  89.91165, 240.161, 159.7698,
  344.6786, 240.161, 263.3669,
  10, 0.1414166, 5.238372, 0.1414166
)

table_constraint <- "
  nlayers BIGINT,
  snam TEXT ,
  hydgrp TEXT,
  anion_excl DOUBLE PRECISION,
  sol_zmx DOUBLE PRECISION,
  sol_crk DOUBLE PRECISION,
  sol_bd1 DOUBLE PRECISION,
  sol_z1 DOUBLE PRECISION,
  sol_awc1 DOUBLE PRECISION,
  sol_cbn1 DOUBLE PRECISION,
  sol_k1 DOUBLE PRECISION,
  clay1 DOUBLE PRECISION,
  sand1 DOUBLE PRECISION,
  silt1 DOUBLE PRECISION,
  rock1 DOUBLE PRECISION,
  sol_alb1 DOUBLE PRECISION,
  sol_ec1 DOUBLE PRECISION,
  usle_k1 DOUBLE PRECISION,
  sol_bd2 DOUBLE PRECISION,
  sol_z2 DOUBLE PRECISION,
  sol_awc2 DOUBLE PRECISION,
  sol_cbn2 DOUBLE PRECISION,
  sol_k2 DOUBLE PRECISION,
  clay2 DOUBLE PRECISION,
  sand2 DOUBLE PRECISION,
  silt2 DOUBLE PRECISION,
  rock2 DOUBLE PRECISION,
  sol_ec2 DOUBLE PRECISION,
  usle_k2 DOUBLE PRECISION,
  sol_alb2 DOUBLE PRECISION,
  sol_bd3 DOUBLE PRECISION,
  sol_z3 DOUBLE PRECISION,
  sol_awc3 DOUBLE PRECISION,
  sol_cbn3 DOUBLE PRECISION,
  sol_k3 DOUBLE PRECISION,
  clay3 DOUBLE PRECISION,
  sand3 DOUBLE PRECISION,
  silt3 DOUBLE PRECISION,
  rock3 DOUBLE PRECISION,
  usle_k3 DOUBLE PRECISION,
  sol_alb3 DOUBLE PRECISION,
  sol_ec3 DOUBLE PRECISION,
  sol_no31 DOUBLE PRECISION,
  sol_no32 DOUBLE PRECISION,
  sol_no33 DOUBLE PRECISION,
  sol_orgn1 DOUBLE PRECISION,
  sol_orgn2 DOUBLE PRECISION,
  sol_labp1 DOUBLE PRECISION,
  sol_orgn3 DOUBLE PRECISION,
  sol_labp2 DOUBLE PRECISION,
  sol_labp3 DOUBLE PRECISION,
  sol_orgp1 DOUBLE PRECISION,
  sol_orgp2 DOUBLE PRECISION,
  prerco_sub DOUBLE PRECISION,
  sol_orgp3 DOUBLE PRECISION,
  sol_ph1 DOUBLE PRECISION,
  id INTEGER
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "swat2012", table = "usersoilpl"))
write_in_table(pl_con, "swat2012", "usersoilpl", soil_pl_lookup, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 11) mel_dr10lt.drainage_raster_lookup -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

drainage_raster_lookup_pl <- data.frame(
  gkodas = c("hm0", "hm0g"),
  raster_id = 1:2
) |>
mutate(raster_id = as.integer64(raster_id))

# hru_drainage_lt <- load_table(lt_con, "hru", "hrudrainage")
# drainage_raster_lookup_lt <- load_table(lt_con, "mel_dr10lt", "drainage_raster_lookup")
# compare_columns(drainage_raster_lookup_lt, drainage_raster_lookup_pl)

table_constraint <- "
  gkodas TEXT,
  raster_id  BIGINT
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "mel_dr10lt", table = "drainage_raster_lookup"))
write_in_table(pl_con, "mel_dr10lt", "drainage_raster_lookup", drainage_raster_lookup_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 12) atm_deposition.atmdep2022 -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

gdf <- st_read(paste0(data_path, "AtmosphericDeposition/AtmDep2022.geojson")) |>
  st_transform(2180) |>
  mutate(oid = row_number(),
         shape = geometry)
st_agr(gdf) <- "constant"
gdf <- gdf |> rename(nh4_dry = NH4_dry,
                     nh4_wet = NH4_wet,
                     no3_dry = NO3_dry,
                     no3_wet = NO3_wet,
                     shape_area = Shape_Area)|>
  sf::st_drop_geometry() |>
  st_set_geometry("shape") |>
  select(any_of(c("oid", "precipitation", "nh4_dry", "nh4_wet", "no3_dry",
                  "no3_wet", "shape_area", "shape")))


atmdep2022_lt <- load_table(lt_con, "atm_deposition", "atmdep2018")
compare_columns(atmdep2022_lt, gdf)

table_constraint <- "
  oid BIGINT PRIMARY KEY,
  precipitation DOUBLE PRECISION,
  nh4_dry DOUBLE PRECISION,
  nh4_wet DOUBLE PRECISION,
  no3_dry DOUBLE PRECISION,
  no3_wet DOUBLE PRECISION,
  shape_area DOUBLE PRECISION,
  shape geometry(Polygon, 2180)
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "atm_deposition", table = "atmdep2022"))
write_in_table(pl_con, "atm_deposition", "atmdep2022", gdf, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 13) hru.buffzones_stat -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

gdf <- st_read(paste0(data_path, "BufferZones/bufferstrips.shp")) |>
  # 1. Rename standard attributes first
  rename(
    catchmentid = catchmenti,
    shape_length = shape_leng
  ) |>
  st_set_agr("constant") |>
  st_set_geometry("geometry") |>
  rename(shape = geometry) |>
  select(any_of(c("catchmentid", "shape", "shape_area", "shape_length")))

# buffzones_stat_lt <- load_table(lt_con, "hru", "buffzones_stat")
# compare_columns(buffzones_stat_lt, gdf)

table_constraint <- "
  catchmentid BIGINT PRIMARY KEY,
  shape_area DOUBLE PRECISION,
  shape_length DOUBLE PRECISION,
  shape geometry(MultiPolygon, 2180)
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "hru", table = "buffzones_stat"))
write_in_table(pl_con, "hru", "buffzones_stat", gdf, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 14) hru.node_elevation -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

gdf <- st_read(paste0(data_path, "HRU/node_elevation.shp")) |>
  mutate(node_id = as.integer64(node_id)) |>
  st_set_geometry("shape") |>
  select(any_of(c("node_id", "shape", "from_dem")))

# node_elevation_lt <- load_table(lt_con, "hru", "node_elevation")
# compare_columns(node_elevation_lt, gdf)

tablel_constraint<- "
  node_id BIGINT,
  from_dem DOUBLE PRECISION,
  shape geometry(Point, 2180)
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "hru", table = "node_elevation"))
write_in_table(pl_con, "hru", "node_elevation", gdf, table_constraint = tablel_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 15) counties.counties_raster_lookup -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

gdf <- st_read(paste0(data_path, "Counties/counties.shp")) |>
  mutate(kodas = as.character(kodas),
         raster_id = as.integer64(raster_id)) |>
  st_drop_geometry() |>
  select(any_of(c("kodas", "raster_id")))

# counties_raster_lookup_lt <- load_table(lt_con, "counties", "counties_raster_lookup")
# compare_columns(counties_raster_lookup_lt, gdf)

table_constraint <- "
  kodas TEXT,
  raster_id BIGINT
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "counties", table = "counties_raster_lookup"))
write_in_table(pl_con, "counties", "counties_raster_lookup", gdf, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 16) counties.counties -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

gdf <- st_read(paste0(data_path, "Counties/counties.shp")) |>
  mutate(kodas = as.character(kodas)) |>
  rename(gid = raster_id) |>
  st_set_geometry("shape") |>
  select(gid, kodas, shape)

# counties_lt <- load_table(lt_con, "counties", "counties")
# compare_columns(counties_lt, gdf)

table_constraint <- "
  gid BIGINT PRIMARY KEY,
  kodas TEXT,
  shape geometry(MultiPolygon, 2180)
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "counties", table = "counties"))
write_in_table(pl_con, "counties", "counties", gdf, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 17) bufferstrips.polygons -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

gdf <- st_read(paste0(data_path, "BufferZones/bufferstrips.shp")) |>
  # 1. Rename standard attributes first
  rename(
    catchmentid = catchmenti,
    shape_length = shape_leng
  ) |>
  st_set_agr("constant") |>
  st_set_geometry("geometry") |>
  rename(shape = geometry) |>
  select(c("catchmentid", "shape", "shape_area", "shape_length"))

# bufferstrips_lt <- load_table(lt_con, "bufferstrips", "polygons")

table_constraint <- "
  catchmentid BIGINT PRIMARY KEY,
  shape_area DOUBLE PRECISION,
  shape_length DOUBLE PRECISION,
  shape geometry(MultiPolygon, 2180)
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "bufferstrips", table = "polygons"))
write_in_table(pl_con, "bufferstrips", "polygons", gdf, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 18) fert.plant_data -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

plant_data_lt <- load_table(lt_con, "fert", "plant_data")

table_constraint <- "
  id INTEGER PRIMARY KEY,
  swat_id VARCHAR(10),
  plant_name VARCHAR(255),
  method NUMERIC,
  std_yield DOUBLE PRECISION,
  n_demand DOUBLE PRECISION,
  p_demand DOUBLE PRECISION,
  preplant_n DOUBLE PRECISION,
  preplant_p DOUBLE PRECISION
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "fert", table = "plant_data"))
write_in_table(pl_con, "fert", "plant_data", plant_data_lt, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 19) fert.plants_luclass -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

plants_luclass_lt <- load_table(lt_con, "fert", "plants_luclass")

table_constraint <- "
  id INTEGER PRIMARY KEY,
  swat_id VARCHAR(10),
  luclass VARCHAR(100),
  phuinitial DOUBLE PRECISION,
  plantcode VARCHAR(50)
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "fert", table = "plants_luclass"))
write_in_table(pl_con, "fert", "plants_luclass", plants_luclass_lt,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 20) hru.lakesreservoirs -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

lakesreservoirs_pl <- data.frame(
  lakes_reserv_id = integer(0),
  catchmentid = integer64(0),
  shape = character(0),
  snpl = numeric(0),
  savl = numeric(0),
  vnpl = numeric(0),
  vavl = numeric(0),
  gylis = numeric(0),
  totarea = numeric(0),
  shape_area = numeric(0),
  segmentid = integer64(0)
)

# lakesreservoirs_lt <- load_table(lt_con, "hru", "lakesreservoirs")
# lakesreservoirs_lt_sf <- fix_sf_geometry(lakesreservoirs_lt |> filter(shape_area > 10), "shape", 3346, "Polygon")
# mapview::mapview(lakesreservoirs_lt_sf)
# compare_columns(lakesreservoirs_lt, lakesreservoirs_pl)

table_constraint <- "
    lakes_reserv_id INTEGER PRIMARY KEY,
    catchmentid BIGINT,
    shape geometry(MultiPolygon, 2180),
    snpl DOUBLE PRECISION,
    savl DOUBLE PRECISION,
    vnpl DOUBLE PRECISION,
    vavl DOUBLE PRECISION,
    gylis DOUBLE PRECISION,
    totarea DOUBLE PRECISION,
    shape_area DOUBLE PRECISION,
    segmentid BIGINT
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "hru", table = "lakesreservoirs"))
write_in_table(pl_con, "hru", "lakesreservoirs", lakesreservoirs_pl, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 21) hru.precip_by_catch -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

# precip_by_catch_pl <- data.frame(
#   catchmentid = integer64(0),
#   raster_id = integer64(0),
#   count = numeric(0),
#   mean = numeric(0))

pcp_rast <- rast(paste0(data_path, "Meteo/pcp_avg1997_2020.tif"))
catchments_pl_proj <- st_transform(catchments_pl, crs(pcp_rast))
precip_by_catch_pl <- exact_extract(
  pcp_rast,
  catchments_pl_proj,
  fun      = c("count", "mean"),
  append_cols = "catchmentid"   # pulls catchmentid from the sf object
) |>
  mutate(raster_id = row_number()) |>
  select(catchmentid, raster_id, count, mean) |>
  mutate(raster_id = as.integer64(raster_id),
         count = as.integer(count))

# precip_by_catch_lt <- load_table(lt_con, "hru", "precip_by_catch")
# compare_columns(precip_by_catch_lt, precip_by_catch_pl)

table_constraint <- "
    catchmentid BIGINT,
    raster_id BIGINT PRIMARY KEY,
    count BIGINT,
    mean DOUBLE PRECISION
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "hru", table = "precip_by_catch"))
write_in_table(pl_con, "hru", "precip_by_catch", precip_by_catch_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 22) rivers.gri_water_catchments -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

gri_water_catchments_pl <- data.frame(
  catchmentid = integer64(0),
  segmentid = integer64(0),
  shape = character(0),
  area = numeric(0),
  perimeter = numeric(0),
  width = numeric(0),
  lwidth = numeric(0)
)

# gri_water_catchments_lt <- load_table(lt_con, "rivers", "gri_water_catchments")
# gri_water_catchments_lt_sf <- fix_sf_geometry(gri_water_catchments_lt, "shape", 3346, "Point")
# mapview::mapview(gri_water_catchments_lt_sf)
# compare_columns(gri_water_catchments_lt, gri_water_catchments_pl)

table_constraint <- "
    catchmentid BIGINT PRIMARY KEY,
    segmentid BIGINT,
    shape GEOMETRY(POINT, 2180),
    area DOUBLE PRECISION,
    perimeter DOUBLE PRECISION,
    width DOUBLE PRECISION,
    lwidth DOUBLE PRECISION
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "rivers", table = "gri_water_catchments"))
write_in_table(pl_con, "rivers", "gri_water_catchments", gri_water_catchments_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 23) rivers.segment_depth -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

segment_depth_pl <- data.frame(
  count = numeric(0),
  mean = numeric(0),
  std = numeric(0),
  min = numeric(0),
  max = numeric(0),
  raster_id = integer(0),
  catchmentid = integer64(0)
)

# segment_depth_lt <- load_table(lt_con, "rivers", "segment_depth")
# compare_columns(segment_depth_lt, segment_depth_pl)

table_constraint <- "
    count DOUBLE PRECISION,
    mean DOUBLE PRECISION,
    std DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    raster_id INTEGER,
    catchmentid BIGINT
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "rivers", table = "segment_depth"))
write_in_table(pl_con, "rivers", "segment_depth", segment_depth_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 24) rivers.segmentmanningcoef -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

segmentmanningcoef_pl <- data.frame(
  catchmentid = integer64(0),
  segmentid = integer64(0),
  avg_mann = numeric(0),
  length = numeric(0),
  n = numeric(0),
  shape = character(0),
  rel_length = numeric(0)
)

# segmentmanningcoef_lt <- load_table(lt_con, "rivers", "segmentmanningcoef")
# segmentmanningcoef_lt_sf <- fix_sf_geometry(segmentmanningcoef_lt, "shape", 3346, "MULTILINESTRING")
# mapview::mapview(segmentmanningcoef_lt_sf)
# compare_columns(segmentmanningcoef_lt, segmentmanningcoef_pl)

table_constraint <- "
    catchmentid BIGINT,
    segmentid BIGINT,
    avg_mann DOUBLE PRECISION,
    length DOUBLE PRECISION,
    n DOUBLE PRECISION,
    shape GEOMETRY(MULTILINESTRING, 2180),
    rel_length DOUBLE PRECISION
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "rivers", table = "segmentmanningcoef"))
write_in_table(pl_con, "rivers", "segmentmanningcoef", segmentmanningcoef_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 25) obs.statpcp -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

statpcp_lt <- load_table(lt_con, "obs", "statpcp")
statpcp_pl <- statpcp_lt |> filter(statid == 8) |>
  st_as_sf(coords = c("xpr", "ypr"), crs = 3346) |>
  st_transform(2180) |>
  (\(d) {
    coords <- st_coordinates(d)
    d$xpr <- coords[, 1]
    d$ypr <- coords[, 2]
    return(d)
  })() |>
  st_drop_geometry()

table_constraint <- "
    ogc_fid INTEGER PRIMARY KEY,
    join_count INTEGER,
    target_fid INTEGER,
    statid INTEGER,
    statname VARCHAR(100),
    precip DOUBLE PRECISION,
    id INTEGER,
    name VARCHAR(50),
    elevation DOUBLE PRECISION,
    xpr DOUBLE PRECISION,
    ypr DOUBLE PRECISION
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "obs", table = "statpcp"))
write_in_table(pl_con, "obs", "statpcp", statpcp_pl, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 26) hru.catchments_regions -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

catchments_regions_pl <- data.frame(
  catchmentid = integer64(0),
  code = character(0),
  wshcode = character(0)
)

# catchments_regions_lt <- load_table(lt_con, "hru", "catchments_regions")
# compare_columns(catchments_regions_lt, catchments_regions_pl)

table_constraint <- "
    catchmentid BIGINT PRIMARY KEY,
    code VARCHAR(50),
    wshcode VARCHAR(50)
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "hru", table = "catchments_regions"))
write_in_table(pl_con, "hru", "catchments_regions", catchments_regions_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 27) point_sources_zero.ps_catchment ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

point_sources_pl <- data.frame(
  psid = integer64(0),
  catchmentid = integer64(0),
  psname = character(0),
  shape = structure(character(0), class = "pq_geometry")
)

# point_sources_lt <- load_table(lt_con, "point_sources", "ps_catchment")
# point_sources_zero_lt <- load_table(lt_con, "point_sources_zero", "ps_catchment")
# compare_columns(point_sources_pl, point_sources_zero_lt)

table_constraint <- "
    psid BIGINT PRIMARY KEY,
    catchmentid BIGINT,
    psname TEXT,
    shape GEOMETRY(POINT, 2180)
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "point_sources_zero", table = "ps_catchment"))
write_in_table(pl_con, "point_sources_zero", "ps_catchment", point_sources_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 28) management.livestock_data ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

gdf <- st_read(paste0(data_path, "Counties/counties.shp")) |>
  mutate(kodas = as.character(kodas),
         raster_id = as.integer(raster_id))

## Load raster and calculate livestock units based on agricultural area
lulc_raster <- rast(paste0(data_path, "LU/lugroups.tif"))
extracted_data <- exact_extract(lulc_raster, gdf, include_cols = "kodas")
livestock_data_pl <- bind_rows(extracted_data) |>
  group_by(kodas, value) |> # 'value' represents the raster cell values (raster_id)
  summarise(area_pixels = sum(coverage_fraction, na.rm = TRUE), .groups = "drop") |>
  filter (value == 1) |>
  mutate(agricultural_area = (area_pixels * res(lulc_raster)[1] * res(lulc_raster)[2])/10000,
         livestock_units = 146.2 + (0.2101 * agricultural_area)) |>
  select(kodas, livestock_units, agricultural_area)

# management.livestock_data_v2025 <- load_table(lt_con, "management", "livestock_data_v2025")
# compare_columns(management.livestock_data_v2025, livestock_data_pl)

table_constraint <- "
    kodas BIGINT PRIMARY KEY,
    livestock_units DOUBLE PRECISION,
    agricultural_area DOUBLE PRECISION
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "management", table = "livestock_data"))
write_in_table(pl_con, "management", "livestock_data", livestock_data_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 29) management.yield_data ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

## Load yield data from the legacy database
management.yield_data_v2025 <- load_table(lt_con, "management", "yield_data_v2025")
landuse.landuse_swat_raster_lookup <- load_table(pl_con, "landuse", "landuse_swat_raster_lookup")

## Create a data frame with all combinations of swat_id and kodas
plant_yields <- management.yield_data_v2025 |>
  select(-kodas) |>
  group_by(swat_id) |>
  slice_head(n = 1) |>
  ungroup()

plant_yields_pl <- gdf["kodas"] |>
  st_drop_geometry() |>
  cross_join(plant_yields)

# compare_columns(management.yield_data_v2025, plant_yields_pl)

table_constraint <- "
    kodas TEXT,
    swat_id TEXT,
    planned_yield DOUBLE PRECISION
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "management", table = "yield_data"))
write_in_table(pl_con, "management", "yield_data", plant_yields_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 30) management.plantmng ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

management.plantmng <- load_table(lt_con, "management", "plantmng")

table_constraint <- "
    id BIGINT PRIMARY KEY,
    swat_id TEXT,
    opnpk DOUBLE PRECISION,
    optype TEXT,
    hu DOUBLE PRECISION,
    value DOUBLE PRECISION,
    value2 DOUBLE PRECISION,
    value_s TEXT
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "management", table = "plantmng"))
write_in_table(pl_con, "management", "plantmng", management.plantmng,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 31) hru.fertCoefByCatchm ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

hru.fertCoefByCatchm <- load_table(lt_con, "hru", "fertcoefbycatchm")

catchmentdata.catchments_coarse <- load_table(pl_con, "catchments",
                                              "catchments_coarse", exclude_geom =T)
df <- data.frame(catchmentid = catchmentdata.catchments_coarse$id, fertcoeff = 1)

# hru.fertCoefByCatchm <- load_table(lt_con, "hru", "fertcoefbycatchm")
# compare_columns(hru.fertCoefByCatchm, df)

tabel_constraint  <- "
    catchmentid BIGINT PRIMARY KEY,
    fertcoeff DOUBLE PRECISION
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "hru", table = "fertcoefbycatchm"))
write_in_table(pl_con, "hru", "fertcoefbycatchm", df, table_constraint = tabel_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 32) fert.forest_biomass ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

fert.forest_biomass<- load_table(lt_con, "fert", "forest_biomass")

tabel_constraint  <- "
    id BIGINT PRIMARY KEY,
    swat_id TEXT,
    biomass DOUBLE PRECISION
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "fert", table = "forest_biomass"))
write_in_table(pl_con, "fert", "forest_biomass", fert.forest_biomass,
               table_constraint = tabel_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 33) lup.lup_table ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

catchmentdata.catchments_coarse <- load_table(pl_con, "catchments", "catchments") |>
  fix_sf_geometry("shape", 2180, "Polygon")

lulc_raster <- rast(paste0(data_path, "LU/lugroups.tif"))
extracted_data <- exact_extract(lulc_raster, catchmentdata.catchments_coarse, include_cols = "catchmentid")
raster_summary <- bind_rows(extracted_data) |>
  group_by(catchmentid, value) |> # 'value' represents the raster cell values (raster_id)
  summarise(area_pixels = sum(coverage_fraction, na.rm = TRUE), .groups = "drop") |>
  mutate(area_m2 = (area_pixels * res(lulc_raster)[1] * res(lulc_raster)[2]))

landusegroup_raster_lookup <- load_table(pl_con, "landuse", "landusegroup_raster_lookup")

lup.lup_table1_pl <- raster_summary |>
  left_join(landusegroup_raster_lookup |> mutate(raster_id = as.numeric(raster_id )),
            by = c("value" = "raster_id")) |>
  mutate(luclass = tolower(luclass)) |>
  mutate(luclass = ifelse(is.na(luclass), "barren", luclass)) |>
  select(catchmentid, luclass, area_m2) |>
  pivot_wider(
    id_cols = catchmentid,
    names_from = luclass,
    values_from = area_m2
  ) |>
  select(c("catchmentid","agricultural","barren","forest","pasture","urban",
    "water","wetland"
  ))

# lup.lup_table1 <- load_table(lt_con, "lup", "lup_table1")
# compare_columns(lup.lup_table1, lup.lup_table1_pl)

table_constraint <- "
    catchmentid BIGINT PRIMARY KEY,
    agricultural DOUBLE PRECISION,
    barren DOUBLE PRECISION,
    forest DOUBLE PRECISION,
    pasture DOUBLE PRECISION,
    urban DOUBLE PRECISION,
    water DOUBLE PRECISION,
    wetland DOUBLE PRECISION
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "lup", table = "lup_table1"))
# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "lup", table = "lup_table2"))
write_in_table(pl_con, "lup", "lup_table1", lup.lup_table1_pl, table_constraint = table_constraint)
write_in_table(pl_con, "lup", "lup_table2", lup.lup_table1_pl, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 34) management.plant_plt ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

plant_plt <- load_table(lt_con, "management", "plant_plt")

table_constraints  <- "
    index BIGINT PRIMARY KEY,
    id BIGINT,
    name TEXT,
    plnt_typ TEXT,
    gro_trig TEXT,
    nfix_co TEXT,
    days_mat DOUBLE PRECISION,
    bm_e DOUBLE PRECISION,
    harv_idx DOUBLE PRECISION,
    lai_pot DOUBLE PRECISION,
    frac_hu1 DOUBLE PRECISION,
    lai_max1 DOUBLE PRECISION,
    frac_hu2 DOUBLE PRECISION,
    lai_max2 DOUBLE PRECISION,
    hu_lai_decl DOUBLE PRECISION,
    dlai_rate DOUBLE PRECISION,
    can_ht_max DOUBLE PRECISION,
    rt_dp_max DOUBLE PRECISION,
    tmp_opt DOUBLE PRECISION,
    tmp_base BIGINT,
    frac_n_yld DOUBLE PRECISION,
    frac_p_yld DOUBLE PRECISION,
    frac_n_em DOUBLE PRECISION,
    frac_n_50 DOUBLE PRECISION,
    frac_n_mat DOUBLE PRECISION,
    frac_p_em DOUBLE PRECISION,
    frac_p_50 DOUBLE PRECISION,
    frac_p_mat DOUBLE PRECISION,
    harv_idx_ws DOUBLE PRECISION,
    usle_c_min DOUBLE PRECISION,
    stcon_max DOUBLE PRECISION,
    vpd DOUBLE PRECISION,
    frac_stcon DOUBLE PRECISION,
    ru_vpd DOUBLE PRECISION,
    co2_hi BIGINT,
    bm_e_hi DOUBLE PRECISION,
    plnt_decomp DOUBLE PRECISION,
    lai_min DOUBLE PRECISION,
    bm_tree_acc DOUBLE PRECISION,
    yrs_mat TEXT,
    bm_tree_max BIGINT,
    ext_co DOUBLE PRECISION,
    leaf_tov_mn DOUBLE PRECISION,
    leaf_tov_mx DOUBLE PRECISION,
    bm_dieoff DOUBLE PRECISION,
    rt_st_beg DOUBLE PRECISION,
    rt_st_end DOUBLE PRECISION,
    plnt_pop1 DOUBLE PRECISION,
    frac_lai1 DOUBLE PRECISION,
    plnt_pop2 DOUBLE PRECISION,
    frac_lai2 DOUBLE PRECISION,
    frac_sw_gro DOUBLE PRECISION,
    wnd_live DOUBLE PRECISION,
    wnd_dead DOUBLE PRECISION,
    wnd_flat DOUBLE PRECISION,
    description TEXT
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "management", table = "plant_plt"))
write_in_table(pl_con, "management", "plant_plt", plant_plt, table_constraint = table_constraints)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 35) management.urban_urb ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

urban_urb <- load_table(lt_con, "management", "urban_urb")

table_constraint <- "
    index BIGINT PRIMARY KEY,
    id BIGINT,
    name TEXT,
    frac_imp DOUBLE PRECISION,
    frac_dc_imp DOUBLE PRECISION,
    curb_den DOUBLE PRECISION,
    urb_wash DOUBLE PRECISION,
    dirt_max DOUBLE PRECISION,
    t_halfmax DOUBLE PRECISION,
    conc_totn DOUBLE PRECISION,
    conc_totp DOUBLE PRECISION,
    conc_no3n DOUBLE PRECISION,
    urb_cn DOUBLE PRECISION,
    description TEXT
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "management", table = "urban_urb"))
write_in_table(pl_con, "management", "urban_urb", urban_urb, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 36) management.cntable_lum ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

cntable_lum <- load_table(lt_con, "management", "cntable_lum")

table_constraint  <- "
    index BIGINT PRIMARY KEY,
    id BIGINT,
    name TEXT,
    cn_a BIGINT,
    cn_b BIGINT,
    cn_c BIGINT,
    cn_d BIGINT,
    description TEXT,
    treat TEXT,
    cond_cov TEXT
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "management", table = "cntable_lum"))
write_in_table(pl_con, "management", "cntable_lum", cntable_lum, table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 37) point_sources_zero.small_catch ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

small_catch_pl <- data.frame(
  id = integer64(0),
  year = integer64(0),
  name = character(0),
  type = integer64(0),
  nutrient = character(0),
  concentration = numeric(0),
  unit = character(0),
  discharge = numeric(0),
  y = numeric(0),
  x = numeric(0),
  catchmentid = integer64(0),
  shape = structure(character(0), class = "pq_geometry")
)

# small_catch <- load_table(lt_con, "point_sources_zero", "small_catch")
# compare_columns(small_catch, small_catch_pl)

table_constraint  <- "
    id BIGINT PRIMARY KEY,
    year BIGINT,
    name TEXT,
    type BIGINT,
    nutrient TEXT,
    concentration DOUBLE PRECISION,
    unit TEXT,
    discharge DOUBLE PRECISION,
    y DOUBLE PRECISION,
    x DOUBLE PRECISION,
    catchmentid BIGINT,
    shape GEOMETRY(POINT, 2180)
"

DBI::dbRemoveTable(pl_con, DBI::Id(schema = "point_sources_zero", table = "small_catch"))
write_in_table(pl_con, "point_sources_zero", "small_catch", small_catch_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 38) management.landuse_properties ------
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

landuse_properties <- load_table(lt_con, "management", "landuse_properties")

table_constraint  <- "
    swat_id TEXT PRIMARY KEY,
    ov_mann_name TEXT,
    cons_prac_name TEXT
"

# DBI::dbRemoveTable(pl_con, DBI::Id(schema = "management", table = "landuse_properties"))
write_in_table(pl_con, "management", "landuse_properties", landuse_properties,
               table_constraint = table_constraint)


