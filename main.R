source("connect.R")
library(bit64)

## Get SWAT LT info
lt_info <- get_db_info(lt_con)

## Get SWAT PL info
pl_info <- get_db_info(pl_con)

## Make sure it is empty
vacum_db(pl_con)
pl_info <- get_db_info(pl_con)

## Load
c_lt <- load_table(lt_con, "catchmentdata", "catchments_coarse")
c_pl <- st_read("data/ForSvajunasCorrected/WatershedsPL_corrected2.shp", quiet = TRUE) |>
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

# Compare names
compare_columns(c_lt, c_pl)

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

write_in_table(pl_con, "catchmentdata", "catchments_coarse", c_pl, table_constraint = table_constraint)


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
