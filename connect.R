library(DBI)
library(mapview)
library(sf)
library(dplyr)
library(rlang)
library(rpostgis)
source("function.R")

## Defining SWAT LT database connection
lt_con <- tryCatch(
  dbConnect(
    RPostgres::Postgres(),
    dbname   = Sys.getenv("LT_DB_NAME"),
    host     = Sys.getenv("LT_DB_HOST"),
    port     = Sys.getenv("LT_DB_PORT"),
    user     = Sys.getenv("LT_DB_USER"),
    password = Sys.getenv("LT_DB_PASSWORD")
  ),
  error = function(e) e
)
## Check if successful
print(lt_con)

## Defining SWAT PL database connection
pl_con <- tryCatch(
  dbConnect(
    RPostgres::Postgres(),
    dbname   = Sys.getenv("PL_DB_NAME"),
    host     = Sys.getenv("PL_DB_HOST"),
    port     = Sys.getenv("PL_DB_PORT"),
    user     = Sys.getenv("PL_DB_USER"),
    password = Sys.getenv("PL_DB_PASSWORD")
  ),
  error = function(e) e
)
## Check if successful
print(pl_con)
