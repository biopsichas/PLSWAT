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
df <- load_table(lt_con, "catchmentdata", "catchments_coarse")
df_reach <- load_table(lt_con, "catchmentdata", "segments_coarse")

reach <- st_read("data/catchment/RiversPL.shp", quiet = TRUE) |>
  st_transform(2180)
catchments <- st_read("data/catchment/watershedsPL.shp", quiet = TRUE) |>
  st_transform(2180)


str(df)
str(catchments)

names(df)
names(catchments)

names(df_reach)
names(reach)



# Assuming your `catchments` is an sf object
catchments_transformed <- catchments %>%
  # 1. Rename columns to match your df names
  rename(
    id = id,  # already matches
    shape = geometry,  # sf geometry column renamed
    type = Join_Count,  # arbitrary mapping — adjust if needed
    segmentid = OBJECTID,
    lakegid = RiverTo,  # or NA if not applicable
    kadastroid = ID_HYD_10,
    kadastroid_lake = skip_catch,  # or NA
    flowto = TARGET_FID,
    segmentto = TARGET_FID,
    outletid = OBJECTID_1,
    area = Shape_Area,
    addedarea = POW,
    inflowarea = Shape_Leng
  ) %>%
  # 2. Add any missing columns present in df but not in catchments
  mutate(
    # Fill NA where necessary
    type = as.integer64(type),
    flowto = as.integer64(flowto),
    segmentto = as.integer64(segmentto),
    outletid = as.integer64(outletid),
    addedarea = as.numeric(addedarea),
    inflowarea = as.numeric(inflowarea),
    lakegid = as.character(lakegid),
    kadastroid = as.character(kadastroid),
    kadastroid_lake = as.character(kadastroid_lake)
  ) %>%
  # 3. Convert geometry to WKB (to match 'pq_geometry' in df)
  mutate(shape = st_as_binary(shape)) %>%
  # 4. Drop sf class to return a normal data.frame
  st_drop_geometry() %>%
  as.data.frame()

# Check result
str(catchments_transformed)


str(df)
str(catchments)

str(df)

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
  shape geometry(MultiPolygon, 3346),
  type BIGINT
"

dbGetQuery(lt_con, "
  SELECT column_name, udt_name, data_type
  FROM information_schema.columns
  WHERE table_schema = 'catchmentdata' AND table_name = 'catchments_coarse';
")
dbGetQuery(lt_con, "
  SELECT f_table_schema, f_table_name, f_geometry_column, type, srid
  FROM geometry_columns
  WHERE f_table_schema = 'catchmentdata' AND f_table_name = 'catchments_coarse';
")

write_in_table(pl_con, "catchmentdata", "catchments_coarse", df, table_constraint = table_constraint)
pl_info <- get_db_info(pl_con)

df_pl <- load_table(pl_con, "catchmentdata", "catchments_coarse")
str(df_pl)


df3 <- st_read(pl_con, query = "SELECT * FROM catchmentdata.catchments_coarse")
df4 <- st_read(lt_con, query = "SELECT * FROM catchmentdata.catchments_coarse")
mapview::mapview(df3$shape)

