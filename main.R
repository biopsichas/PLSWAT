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
## 1) catchmentdata.catchments_coarse table -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

c_lt <- load_table(lt_con, "catchments", "catchments")
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
  select(any_of(names(c_lt)))

compare_columns(c_lt, catchments_pl)

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
write_in_table(pl_con, "catchments", "catchments", catchments_pl, table_constraint = table_constraint)



c_pl_test <- load_table(pl_con, "catchments", "catchments")
## Prepare the catchment data with the same structure as the LT table
c_pl <- st_read(paste0(data_path, "Catchments/WatershedsPL_corrected2.shp"),
                quiet = TRUE) |>
  st_transform(2180) |>
  rename(lakegid = lakeid,
         kadastroid_lake= kadastro_1) |>
  mutate(id = as.integer64(id),
         type = as.integer64(type),
         segmentid = as.integer64(segmentid),
         flowto = as.integer64(flowto),
         segmentto = as.integer64(segmentto),
         outletid = as.integer64(outletid),
         shape = geometry) |>
  sf::st_drop_geometry() |>
  select(names(c_lt))

## Compare names with the LT table
# c_lt <- load_table(lt_con, "catchments", "catchments")
compare_columns(c_pl_test, c_pl)

table_constraint <- "
  id BIGINT PRIMARY KEY,
  addedarea DOUBLE PRECISION,
  area DOUBLE PRECISION,
  flowto BIGINT,
  inflowarea DOUBLE PRECISION,
  kadastroid TEXT,
  kadastroid_lake TEXT,
  lakegid TEXT,
  outletid BIGINT,
  segmentid BIGINT,
  segmentto BIGINT,
  shape geometry(MultiPolygon, 2180),
  type BIGINT
"

## If needed, remove the existing table before writing the new on
DBI::dbRemoveTable(pl_con, DBI::Id(schema = "catchmentdata", table = "catchments_coarse"))
write_in_table(pl_con, "catchmentdata", "catchments_coarse", c_pl,
               table_constraint = table_constraint)

## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
## 2) catchmentdata.segments_coarse table -----
## >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

r_lt <- load_table(lt_con, "catchmentdata", "segments_coarse")

r_pl <- st_read("data/ForSvajunasCorrected/RiversPLcorrected.shp", quiet = TRUE) |>
  st_transform(2180) |>
  rename(riverto = RiverTo,
         length = SHAPE_Leng,
         skip_catchment = skip_catch,
         kadastroid_lake = kadastro_1,
         wbriver_code = wbriver_co) |>
  mutate(id = as.integer64(id),
         riverto = as.integer64(riverto),
         skip_catchment = as.logical(skip_catchment),
         geometry2 = geometry) |>
  st_drop_geometry() |>
  rename(geometry = geometry2) |>
  select(names(r_lt))

compare_columns(r_lt, r_pl)

table_constraint_r <- "
  id BIGINT PRIMARY KEY,
  riverto BIGINT,
  length DOUBLE PRECISION,
  geometry geometry(MultiLineString, 2180),
  skip_catchment BOOLEAN,
  kadastroid TEXT,
  kadastroid_lake TEXT,
  wbriver_code TEXT,
  wlake_wb TEXT
"

write_in_table(pl_con, "catchmentdata", "segments_coarse", r_pl, table_constraint = table_constraint_r)

o_lt <- load_table(lt_con, "catchmentdata", "outlets")

o_pl <- read.csv("data/ForSvajunasCorrected/outlets.csv", header = FALSE) |>
  `names<-`(names(o_lt)) |>
  mutate(index = as.integer64(index),
         id = as.integer64(id),
         outlettype = as.integer64(outlettype))

compare_columns(o_lt, o_pl)

table_constraint_o <- "
  index BIGINT,
  id BIGINT PRIMARY KEY,
  outlettype BIGINT,
  outletname TEXT
"

write_in_table(pl_con, "catchmentdata", "outlets", o_pl, table_constraint = table_constraint_o)

w_lt <- load_table(lt_con, "catchmentdata", "watersheds")
w_pl <- read.csv("data/ForSvajunasCorrected/watersheds.csv", header = FALSE) |>
  `names<-`(names(w_lt)) |>
  mutate(watershedid = as.integer64(watershedid))

str(w_lt)
str(w_pl)

compare_columns(w_lt, w_pl)

table_constraint_w <- "
  watershedid BIGINT PRIMARY KEY,
  basinname TEXT,
  watershedname TEXT,
  composite TEXT
"
write_in_table(pl_con, "catchmentdata", "watersheds", w_pl, table_constraint = table_constraint_w)


cw_lt <- load_table(lt_con, "catchmentdata", "coarse_catchm_wsh")
cw_pl <- read.csv("data/ForSvajunasCorrected/catchm_wshed.csv", header = FALSE) |>
  `names<-`(names(cw_lt)) |>
  mutate(catchmentid = as.integer64(catchmentid),
         wshid  = as.integer64(wshid))

str(cw_lt)
str(cw_pl)

compare_columns(cw_lt, cw_pl)

table_constraint_cw <- "
  catchmentid BIGINT,
  wshid BIGINT
"

write_in_table(pl_con, "catchmentdata", "coarse_catchm_wsh", cw_pl, table_constraint = table_constraint_cw)


##==============================================================================
write_in_table(pl_con, "catchments", "catchments_coarse", c_pl, table_constraint = table_constraint)
write_in_table(pl_con, "catchments", "segments_coarse", r_pl, table_constraint = table_constraint_r)
write_in_table(pl_con, "catchments", "outlets", o_pl, table_constraint = table_constraint_o)
write_in_table(pl_con, "catchments", "watersheds", w_pl, table_constraint = table_constraint_w)


cw_pl <- read.csv("data/ForSvajunasCorrected/catchm_wshed.csv", header = FALSE) |>
  `names<-`(names(cw_lt)) |>
  mutate(catchmentid = as.integer64(catchmentid),
         watershedid = as.integer64(wshid)) |>
  select(-wshid)
table_constraint_cw <- "
  catchmentid BIGINT,
  watershedid BIGINT
"
write_in_table(pl_con, "catchments", "catchm_wshed", cw_pl, table_constraint = table_constraint_cw)

##==============================================================================

## read landuse table
lu_lt_landuse_swat_raster_lookup <- load_table(lt_con, "landuse", "landuse_swat_raster_lookup")

lu_pl_lookup <- read.csv("data/Tables/lookup_lu10th_v1.csv", header = TRUE) |>
  rename(swatcode = lu_SWATcode,
         raster_id = gridCode) |>
  select(swatcode, raster_id) |>
  mutate(swatcode = toupper(swatcode),
         raster_id = as.integer64(raster_id))

compare_columns(lu_lt_landuse_swat_raster_lookup, lu_pl_lookup)

table_constraint_lu <- "
  swatcode TEXT,
  raster_id BIGINT
"
write_in_table(pl_con, "landuse", "landuse_swat_raster_lookup", lu_pl_lookup, table_constraint = table_constraint_lu)

##==============================================================================

lu_lt_landusegroup_raster_lookup <- load_table(lt_con, "landuse", "landusegroup_raster_lookup")

table_constraint_lugroups <- "
  luclass TEXT,
  raster_id BIGINT
"

write_in_table(pl_con, "landuse", "landusegroup_raster_lookup", lu_lt_landusegroup_raster_lookup, table_constraint = table_constraint_lugroups)

##==============================================================================

soil_pl_lookup <- read.csv("data/Tables/Lookup_soil.csv", header = TRUE)

soil_swat_raster_lookup_pl <- soil_pl_lookup |>
  rename(swatcode = SNAM,
         raster_id = soilRastID) |>
  mutate(raster_id = as.integer64(raster_id)) |>
  select(swatcode, raster_id)

table_constraint_soil <- "
  swatcode TEXT,
  raster_id BIGINT
"

soil_swat_raster_lookup <- load_table(lt_con, "soil", "soil_swat_raster_lookup")
compare_columns(soil_swat_raster_lookup, soil_swat_raster_lookup_pl)

write_in_table(pl_con, "soil", "soil_swat_raster_lookup", soil_swat_raster_lookup_pl, table_constraint = table_constraint_soil)

##==============================================================================

usersoil_lt <- load_table(lt_con, "swat2012", "usersoillt")
write.csv(usersoil_lt, file = "usersoil_lt.csv", row.names = FALSE)

usersoil_pl <- soil_pl_lookup |>
  rename_with(tolower) |>
  select(-c("soilrastid", "muid", "seqn", "s5id", "cmppct")) |>
  filter(!is.na(nlayers)) |>
  mutate(sol_ec3    = 0,
         sol_no31   = 12.39735,
         sol_no32   = 4.677668,
         sol_no33   = 2.612997,
         sol_orgn1  = 1239.744,
         sol_orgn2  = 467.7761,
         sol_orgn3  = 261.2997,
         sol_labp1  = 89.91165,
         sol_labp2  = 240.161,
         sol_labp3  = 159.7698,
         sol_orgp1  = 344.6786,
         sol_orgp2  = 240.161,
         sol_orgp3  = 263.3669,
         prerco_sub = 10,
         sol_ph1    = 5.238372) |>
  bind_rows(usersoil_lt |> filter(snam=="W") |> mutate(snam = "water") |> select(-id)) |>
  mutate(id = row_number()) |>
  select(any_of(names(usersoil_lt )))|>
  mutate(across(where(is.integer), as.numeric))|>
  mutate(id = as.integer(id)) |>
  group_by(snam) |>
  slice(1) |>
  ungroup()



compare_columns(usersoil_lt, usersoil_pl)

names(usersoil_lt)[!names(usersoil_lt) %in% names(usersoil_pl)]
names(usersoil_pl)[!names(usersoil_pl) %in% names(usersoil_lt)]

table_constraint_usersoil <- "
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

write_in_table(pl_con, "swat2012", "usersoilpl", usersoil_pl, table_constraint = table_constraint_usersoil)
DBI::dbRemoveTable(pl_con, DBI::Id(schema = "swat2012", table = "usersoilpl"))
##==============================================================================

catchments_lt <- load_table(lt_con, "catchments", "catchments")

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
  select(all_of(names(catchments_lt)))

compare_columns(catchments_lt, catchments_pl)

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
write_in_table(pl_con, "catchments", "catchments", catchments_pl, table_constraint = table_constraint)

##==============================================================================
rivers_lt <- load_table(lt_con, "catchments", "riversegments")
r_pl <- st_read("data/ForSvajunasCorrected/RiversPLcorrected.shp", quiet = TRUE) |>
  st_transform(2180) |>
  rename(segmentid = id,
         flowto = RiverTo,
         shape = geometry,
         skip_catchment = skip_catch,
         length = SHAPE_Leng,
         kadastroid_lake = kadastro_1) |>
  mutate(segmentid = as.integer64(segmentid),
         flowto = as.integer64(flowto),
         skip_catchment = as.logical(skip_catchment),
         kadastroid = NA_character_) |>
  select(any_of(names(rivers_lt)))

compare_columns(rivers_lt, r_pl)

table_constraint_r <- "
  segmentid BIGINT PRIMARY KEY,
  flowto BIGINT,
  skip_catchment BOOLEAN,
  kadastroid TEXT,
  kadastroid_lake TEXT,
  wlake_wb TEXT,
  length DOUBLE PRECISION,
  shape geometry(MultiLineString, 2180)
"

write_in_table(pl_con, "catchments", "riversegments", r_pl, table_constraint = table_constraint_r)
##==============================================================================
transfer_lt <- load_table(lt_con, "catchments", "watertransfer")
transfer_lt_empty <- transfer_lt[0, ]

table_constraint_transfer <- "
  id BIGINT PRIMARY KEY,
  name TEXT,
  outletid  BIGINT,
  flowto BIGINT,
  multiplier DOUBLE PRECISION
"

write_in_table(pl_con, "catchments", "watertransfer", transfer_lt_empty, table_constraint = table_constraint_transfer)
##==============================================================================

hru_drainage_lt <- load_table(lt_con, "hru", "hrudrainage")
mel_dr10lt.drainage_raster_lookup_lt <- load_table(lt_con, "mel_dr10lt", "drainage_raster_lookup")

table_constraint_drain <- "
  gkodas TEXT,
  raster_id  BIGINT
"
write_in_table(pl_con, "mel_dr10lt", "drainage_raster_lookup",
               mel_dr10lt.drainage_raster_lookup_lt, table_constraint = table_constraint_drain)

##==============================================================================
hru_atmdep_catchm_lt <- load_table(lt_con, "hru", "atmdep_catchm")
atmdep2022_lt <- load_table(lt_con, "atm_deposition", "atmdep2018")

gdf <- st_read("Data/AtmDep2022.geojson") |>
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
  select(any_of(names(atmdep2022_lt)))

table_constraint_atmdep <- "
  oid BIGINT PRIMARY KEY,
  precipitation DOUBLE PRECISION,
  nh4_dry DOUBLE PRECISION,
  nh4_wet DOUBLE PRECISION,
  no3_dry DOUBLE PRECISION,
  no3_wet DOUBLE PRECISION,
  shape_area DOUBLE PRECISION,
  shape geometry(Polygon, 2180)
"

write_in_table(pl_con, "atm_deposition", "atmdep2022", gdf, table_constraint = table_constraint_atmdep)
##==============================================================================
buffzones_stat_lt <- load_table(lt_con, "hru", "buffzones_stat")

gdf <- st_read("data/Buffer/bufferstrips.shp") |>
  # 1. Rename standard attributes first
  rename(
    catchmentid = catchmenti,
    shape_length = shape_leng
  ) |>
  st_set_agr("constant") |>
  st_set_geometry("geometry") |>
  rename(shape = geometry) |>
  select(any_of(names(buffzones_stat_lt)))

table_constraint_buffer <- "
  catchmentid BIGINT PRIMARY KEY,
  shape_area DOUBLE PRECISION,
  shape_length DOUBLE PRECISION,
  shape geometry(MultiPolygon, 2180)
"

write_in_table(pl_con, "hru", "buffzones_stat", gdf, table_constraint = table_constraint_buffer)
##==============================================================================
node_elevation_lt <- load_table(lt_con, "hru", "node_elevation")

gdf <- st_read("data/HRU/node_elevation.shp") |>
  mutate(node_id = as.integer64(node_id)) |>
  st_set_geometry("shape") |>
  select(any_of(names(node_elevation_lt )))

tablel_constraint_node_elevation <- "
  node_id BIGINT,
  from_dem DOUBLE PRECISION,
  shape geometry(Point, 2180)
"

write_in_table(pl_con, "hru", "node_elevation", gdf, table_constraint = tablel_constraint_node_elevation)

##==============================================================================
counties_raster_lookup_lt <- load_table(lt_con, "counties", "counties_raster_lookup")

gdf <- st_read("data/Counties/counties.shp") |>
  mutate(kodas = as.character(kodas),
         raster_id = as.integer64(raster_id)) |>
  st_drop_geometry() |>
  select(any_of(names(counties_raster_lookup_lt)))

table_constraint_counties <- "
  kodas TEXT,
  raster_id BIGINT
"
write_in_table(pl_con, "counties", "counties_raster_lookup", gdf, table_constraint = table_constraint_counties)

##==============================================================================
counties_lt <- load_table(lt_con, "counties", "counties")

gdf <- st_read("data/Counties/counties.shp") |>
  mutate(kodas = as.character(kodas)) |>
  rename(gid = raster_id) |>
  st_set_geometry("shape") |>
  select(gid, kodas, shape)

table_constraint_counties <- "
  gid BIGINT PRIMARY KEY,
  kodas TEXT,
  shape geometry(MultiPolygon, 2180)
"

write_in_table(pl_con, "counties", "counties", gdf, table_constraint = table_constraint_counties)

##==============================================================================
bufferstrips_lt <- load_table(lt_con, "bufferstrips", "polygons")

gdf <- st_read("data/Buffer/bufferstrips.shp") |>
  # 1. Rename standard attributes first
  rename(
    catchmentid = catchmenti,
    shape_length = shape_leng
  ) |>
  st_set_agr("constant") |>
  st_set_geometry("geometry") |>
  rename(shape = geometry) |>
  select(any_of(names(buffzones_stat_lt)))

table_constraint_buffer <- "
  catchmentid BIGINT PRIMARY KEY,
  shape_area DOUBLE PRECISION,
  shape_length DOUBLE PRECISION,
  shape geometry(MultiPolygon, 2180)
"

write_in_table(pl_con, "bufferstrips", "polygons", gdf, table_constraint = table_constraint_buffer)

##==============================================================================
plant_data_lt <- load_table(lt_con, "fert", "plant_data")

table_constraint_plant_data <- "
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

write_in_table(pl_con, "fert", "plant_data", plant_data_lt, table_constraint = table_constraint_plant_data)


##==============================================================================
plants_luclass_lt <- load_table(lt_con, "fert", "plants_luclass")

table_constraint_plants_luclass <- "
  id INTEGER PRIMARY KEY,
  swat_id VARCHAR(10),
  luclass VARCHAR(100),
  phuinitial DOUBLE PRECISION,
  plantcode VARCHAR(50)
"

write_in_table(pl_con, "fert", "plants_luclass", plants_luclass_lt, table_constraint = table_constraint_plants_luclass)
##==============================================================================

lakesreservoirs_lt <- load_table(lt_con, "hru", "lakesreservoirs")
lakesreservoirs_lt_sf <- fix_sf_geometry(lakesreservoirs_lt |> filter(shape_area > 10), "shape", 3346, "Polygon")
# mapview::mapview(lakesreservoirs_lt_sf)

lakesreservoirs_empty <- lakesreservoirs_lt[0, ]

table_constraint_lakes_reservoirs <- "
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

write_in_table(pl_con, "hru", "lakesreservoirs", lakesreservoirs_empty, table_constraint = table_constraint_lakes_reservoirs)
##==============================================================================
precip_by_catch_lt <- load_table(lt_con, "hru", "precip_by_catch")
precip_by_catch_empty <- precip_by_catch_lt[0, ]

table_constraint_precip_catchment <- "
    catchmentid BIGINT,
    raster_id BIGINT PRIMARY KEY,
    count BIGINT,
    mean DOUBLE PRECISION
"

write_in_table(pl_con, "hru", "precip_by_catch", precip_by_catch_empty, table_constraint = table_constraint_precip_catchment)
##==============================================================================
gri_water_catchments_lt <- load_table(lt_con, "rivers", "gri_water_catchments")

gri_water_catchments_lt_sf <- fix_sf_geometry(gri_water_catchments_lt, "shape", 3346, "Point")
mapview::mapview(gri_water_catchments_lt_sf)

gri_water_catchments_empty <- gri_water_catchments_lt[0, ]
str(gri_water_catchments_lt)

table_constraint_water_catchments <- "
    catchmentid BIGINT PRIMARY KEY,
    segmentid BIGINT,
    shape GEOMETRY(POINT, 2180),
    area DOUBLE PRECISION,
    perimeter DOUBLE PRECISION,
    width DOUBLE PRECISION,
    lwidth DOUBLE PRECISION
"

write_in_table(pl_con, "rivers", "gri_water_catchments", gri_water_catchments_empty, table_constraint = table_constraint_water_catchments)
##==============================================================================
segment_depth_lt <- load_table(lt_con, "rivers", "segment_depth")
str(segment_depth_lt)

segment_depth_empty <- segment_depth_lt[0, ]

table_constraint_segment_depth <- "
    count DOUBLE PRECISION,
    mean DOUBLE PRECISION,
    std DOUBLE PRECISION,
    min DOUBLE PRECISION,
    max DOUBLE PRECISION,
    raster_id INTEGER,
    catchmentid BIGINT
"
write_in_table(pl_con, "rivers", "segment_depth", segment_depth_empty, table_constraint = table_constraint_segment_depth)

##==============================================================================
segmentmanningcoef_lt <- load_table(lt_con, "rivers", "segmentmanningcoef")
segmentmanningcoef_lt_sf <- fix_sf_geometry(segmentmanningcoef_lt, "shape", 3346, "MULTILINESTRING")
mapview::mapview(segmentmanningcoef_lt_sf)

segmentmanningcoef_empty <- segmentmanningcoef_lt[0, ]

table_constraint_segment_manning <- "
    catchmentid BIGINT,
    segmentid BIGINT,
    avg_mann DOUBLE PRECISION,
    length DOUBLE PRECISION,
    n DOUBLE PRECISION,
    shape GEOMETRY(MULTILINESTRING, 2180),
    rel_length DOUBLE PRECISION
"
write_in_table(pl_con, "rivers", "segmentmanningcoef", segmentmanningcoef_empty, table_constraint = table_constraint_segment_manning)
##==============================================================================
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

str(statpcp_pl)

table_constraint_statpcp_pl <- "
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

write_in_table(pl_con, "obs", "statpcp", statpcp_pl, table_constraint = table_constraint_statpcp_pl)
##==============================================================================
catchments_regions_lt <- load_table(lt_con, "hru", "catchments_regions")
catchments_regions_empty <- catchments_regions_lt[0, ]

table_constraint_catchments_regions <- "
    catchmentid BIGINT PRIMARY KEY,
    code VARCHAR(50),
    wshcode VARCHAR(50)
"

write_in_table(pl_con, "hru", "catchments_regions", catchments_regions_empty, table_constraint = table_constraint_catchments_regions)
##==============================================================================
point_sources_zero_lt <- load_table(lt_con, "point_sources_zero", "ps_catchment")

point_sources_lt <- load_table(lt_con, "point_sources", "ps_catchment")
str(point_sources_zero_lt)

table_constraint_point_sources <- "
    psid BIGINT PRIMARY KEY,
    catchmentid BIGINT,
    psname TEXT,
    shape GEOMETRY(POINT, 2180)
"

write_in_table(pl_con, "point_sources_zero", "ps_catchment", point_sources_zero_lt, table_constraint = table_constraint_point_sources)
##==============================================================================
management.livestock_data_v2025 <- load_table(lt_con, "management", "livestock_data_v2025")


gdf <- st_read("data/Counties/counties.shp") |>
  mutate(kodas = as.character(kodas),
         raster_id = as.integer(raster_id))

## Load raster

lulc_raster <- rast("data/LU/lugroups.tif")
extracted_data <- exact_extract(lulc_raster, gdf, include_cols = "kodas")
raster_summary <- bind_rows(extracted_data) |>
  group_by(kodas, value) |> # 'value' represents the raster cell values (raster_id)
  summarise(area_pixels = sum(coverage_fraction, na.rm = TRUE), .groups = "drop") |>
  filter (value == 1) |>
  mutate(agricultural_area = (area_pixels * res(lulc_raster)[1] * res(lulc_raster)[2])/10000,
         livestock_units = 146.2 + (0.2101 * agricultural_area)) |>
  select(kodas, livestock_units, agricultural_area)

table_constraint_management <- "
    kodas BIGINT PRIMARY KEY,
    livestock_units DOUBLE PRECISION,
    agricultural_area DOUBLE PRECISION
"

write_in_table(pl_con, "management", "livestock_data", raster_summary, table_constraint = table_constraint_management)
##==============================================================================
management.yield_data_v2025 <- load_table(lt_con, "management", "yield_data_v2025")
landuse.landuse_swat_raster_lookup <- load_table(pl_con, "landuse", "landuse_swat_raster_lookup")

plant_yields <- management.yield_data_v2025 |>
  select(-kodas) |>
  group_by(swat_id) |>
  slice_head(n = 1) |>
  ungroup()
all_combinations <- gdf["kodas"] |>
  mutate(kodas = as.integer64(kodas)) |>
  st_drop_geometry() |>
  cross_join(plant_yields)

table_constraint_yield <- "
    kodas BIGINT,
    swat_id TEXT,
    planned_yield DOUBLE PRECISION
"

write_in_table(pl_con, "management", "yield_data", all_combinations, table_constraint = table_constraint_yield)
##==============================================================================

management.plantmng <- load_table(lt_con, "management", "plantmng")

tabel_constraint_plantmng <- "
    id BIGINT PRIMARY KEY,
    swat_id TEXT,
    opnpk DOUBLE PRECISION,
    optype TEXT,
    hu DOUBLE PRECISION,
    value DOUBLE PRECISION,
    value2 DOUBLE PRECISION,
    value_s TEXT
"

write_in_table(pl_con, "management", "plantmng", management.plantmng, table_constraint = tabel_constraint_plantmng)
##==============================================================================

hru.fertCoefByCatchm <- load_table(lt_con, "hru", "fertcoefbycatchm")
catchmentdata.catchments_coarse <- load_table(pl_con, "catchments", "catchments_coarse", exclude_geom =T)
df <- data.frame(catchmentid = catchmentdata.catchments_coarse$id, fertcoeff = 1)

tabel_constraint_fertCoefByCatchm  <- "
    catchmentid BIGINT PRIMARY KEY,
    fertcoeff DOUBLE PRECISION
"

write_in_table(pl_con, "hru", "fertcoefbycatchm", df, table_constraint = tabel_constraint_fertCoefByCatchm)
##==============================================================================
fert.forest_biomass<- load_table(lt_con, "fert", "forest_biomass")

tabel_constraint_forest_biomass  <- "
    id BIGINT PRIMARY KEY,
    swat_id TEXT,
    biomass DOUBLE PRECISION
"
write_in_table(pl_con, "fert", "forest_biomass", fert.forest_biomass, table_constraint = tabel_constraint_forest_biomass)
##==============================================================================
lup.lup_table1 <- load_table(lt_con, "lup", "lup_table1")
catchmentdata.catchments_coarse <- load_table(pl_con, "catchments", "catchments") |>
  fix_sf_geometry("shape", 2180, "Polygon")

lulc_raster <- rast("data/LU/lugroups.tif")
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
  select(names(lup.lup_table1))

table_constraint_lup_table1  <- "
    catchmentid BIGINT PRIMARY KEY,
    agricultural DOUBLE PRECISION,
    barren DOUBLE PRECISION,
    forest DOUBLE PRECISION,
    pasture DOUBLE PRECISION,
    urban DOUBLE PRECISION,
    water DOUBLE PRECISION,
    wetland DOUBLE PRECISION
"

write_in_table(pl_con, "lup", "lup_table1", lup.lup_table1_pl, table_constraint = table_constraint_lup_table1)
write_in_table(pl_con, "lup", "lup_table2", lup.lup_table1_pl, table_constraint = table_constraint_lup_table1)

##==============================================================================
plant_plt <- load_table(lt_con, "management", "plant_plt")

table_constraints_plant_plt  <- "
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

write_in_table(pl_con, "management", "plant_plt", plant_plt, table_constraint = table_constraints_plant_plt)
##==============================================================================
urban_urb <- load_table(lt_con, "management", "urban_urb")

table_constraint_urban_urb  <- "
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

write_in_table(pl_con, "management", "urban_urb", urban_urb, table_constraint = table_constraint_urban_urb)
##==============================================================================

cntable_lum <- load_table(lt_con, "management", "cntable_lum")

table_constraint_cntable_lum  <- "
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

write_in_table(pl_con, "management", "cntable_lum", cntable_lum, table_constraint = table_constraint_cntable_lum)
##==============================================================================
small_catch <- load_table(lt_con, "point_sources_zero", "small_catch")

table_constraint_small_catch  <- "
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

write_in_table(pl_con, "point_sources_zero", "small_catch", small_catch, table_constraint = table_constraint_small_catch)
##==============================================================================
landuse_properties <- load_table(lt_con, "management", "landuse_properties")

table_constraint_landuse_properties  <- "
    swat_id TEXT PRIMARY KEY,
    ov_mann_name TEXT,
    cons_prac_name TEXT
"
write_in_table(pl_con, "management", "landuse_properties", landuse_properties, table_constraint = table_constraint_landuse_properties)
##==============================================================================
landuse_properties <- load_table(lt_con, "nutrients_sol", "landuse_properties")








DBI::dbRemoveTable(pl_con, DBI::Id(schema = "management", table = "yield_data"))


