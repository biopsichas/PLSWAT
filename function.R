# Funtion for loading tables
load_table <- function(con, schema, table, exclude_geom = FALSE) {
  # Input validation
  stopifnot(is.character(schema), length(schema) == 1)
  stopifnot(is.character(table), length(table) == 1)
  stopifnot(is.logical(exclude_geom), length(exclude_geom) == 1)

  if (exclude_geom) {
    all_fields <- dbListFields(con, Id(schema = schema, table = table))
    keep_fields <- setdiff(all_fields, c("shape", "geometry"))
    if (length(keep_fields) == 0) {
      stop("All fields excluded, nothing to select.")
    }
    sel_fields <- paste(DBI::dbQuoteIdentifier(con, keep_fields), collapse = ", ")
    message("ⓘ Geometry columns excluded from selection.")
  } else {
    sel_fields <- "*"
  }
  # Quote schema.table properly
  tbl_id <- DBI::dbQuoteIdentifier(con, Id(schema = schema, table = table))

  # Build query
  query <- paste0("SELECT ", sel_fields, " FROM ", tbl_id)

  df <- DBI::dbGetQuery(con, query)
  message("✔ Data is loaded from the database")

  return(df)
}

# Function to get database schema info
get_db_info <- function(con, print_info = TRUE){
  # 1. Get all schemas
  schemas <- dbGetQuery(con, "
  SELECT schema_name
  FROM information_schema.schemata
  WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
")

  # 2. Get all tables
  tables <- dbGetQuery(con, "
  SELECT table_schema, table_name
  FROM information_schema.tables
  WHERE table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema')
")

  # 3. Get all columns and their types
  columns <- dbGetQuery(con, "
  SELECT table_schema, table_name, column_name, data_type
  FROM information_schema.columns
  WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
")

  # 4. Get constraints (PK, FK, UNIQUE, CHECK)
  constraints <- dbGetQuery(con, "
  SELECT
      tc.table_schema,
      tc.table_name,
      kcu.column_name,
      tc.constraint_type
  FROM
      information_schema.table_constraints tc
  LEFT JOIN
      information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
  WHERE
      tc.table_schema NOT IN ('pg_catalog', 'information_schema')
")

  # 5. Get nullability + defaults
  nulls <- dbGetQuery(con, "
  SELECT
      table_schema,
      table_name,
      column_name,
      is_nullable,
      column_default
  FROM
      information_schema.columns
  WHERE
      table_schema NOT IN ('pg_catalog', 'information_schema')
")

  # 6. # Get row counts for all tables
  row_counts <- dbGetQuery(con, "
  SELECT table_schema, table_name,
         (xpath('/row/c/text()', query_to_xml(format('SELECT count(*) AS c FROM %I.%I', table_schema, table_name), false, true, '')))[1]::text::bigint AS row_count
  FROM information_schema.tables
  WHERE table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema');
")

  # 7. Merge everything
  schema_map <- columns |>
    left_join(nulls, by = c("table_schema", "table_name", "column_name")) |>
    left_join(constraints, by = c("table_schema", "table_name", "column_name")) |>
    left_join(row_counts, by = c("table_schema", "table_name")) |>
    arrange(table_schema, table_name, column_name) |>
    select(table_schema, table_name, row_count, column_name, data_type, is_nullable, column_default, constraint_type)


  # 8. collapse multiple constraint types into one column
  schema_map <- schema_map %>%
    group_by(table_schema, table_name, row_count, column_name, data_type, is_nullable, column_default) %>%
    summarise(constraints = paste(unique(constraint_type[!is.na(constraint_type)]), collapse = ", "),
              .groups = "drop")
  # 9. Print summary info if requested
  if(print_info){
    for(s in unique(schema_map$table_schema)){
      cat(paste0("Schema: ", s, " has |> \n"))
      the_tables <- unique(schema_map$table_name[schema_map$table_schema == s])
      for(t in the_tables){
        cat(paste0("  Table: ", t, " has these columns |> ", paste(schema_map$column_name[schema_map$table_name == t & schema_map$table_schema == s], collapse = ", "), " \n"))
      }
    }
  }
  # 10. If no user-defined schemas, inform the user
  if (nrow(schema_map) == 0) {
    message("ⓘ There are no user-defined schemas in the database.")
  }

  return(schema_map)
}

# Function to write data frame into a table with options for schema creation, overwrite, append, and custom constraints
write_in_table <- function(con, schema, table, df,
                           overwrite = FALSE,
                           append = FALSE,
                           table_constraint = NULL) {
  # Input validation
  stopifnot(is.character(schema), length(schema) == 1)
  stopifnot(is.character(table), length(table) == 1)
  stopifnot(is.data.frame(df))

  # Check schema existence
  schema_exists <- dbGetQuery(con, sprintf("
    SELECT EXISTS(
      SELECT 1
      FROM information_schema.schemata
      WHERE schema_name = '%s'
    ) AS schema_exists;", schema))$schema_exists

  # Create schema if missing
  if (!schema_exists) {
    dbExecute(con, sprintf("CREATE SCHEMA %s;", DBI::dbQuoteIdentifier(con, schema)))
    message(sprintf("✔ Schema '%s' created.", schema))
  }

  # Initialize variable
  colname <- NULL

  # If table_constraint is provided → create table manually
  if (!is.null(table_constraint)) {
    sql <- sprintf(
      "CREATE TABLE IF NOT EXISTS %s.%s (%s);",
      DBI::dbQuoteIdentifier(con, schema),
      DBI::dbQuoteIdentifier(con, table),
      table_constraint
    )

    # Regex to capture: column name + geometry(type, crs)
    matches <- regmatches(
      table_constraint,
      gregexpr("([a-zA-Z0-9_]+)\\s+geometry\\(([^)]*)\\)", table_constraint, perl = TRUE)
    )[[1]]

    # Extract parts
    colname <- gsub(" geometry\\(.*", "", matches)
    # Extract geometry type and CRS
    geom_type <- sub(".*geometry\\(([^,]+),.*", "\\1", matches)
    geom_crs  <- as.integer(sub(".*,(\\s*[0-9]+)\\)$", "\\1", matches))

    if(length(colname) == 0){
      message("ⓘ️ No geometry column found in table_constraint.")
    } else {
      message(paste("ⓘ Geometry definition found:", colname, geom_type, "epsg=", geom_crs, ". It will be used to set geometry type and CRS."))
      if(length(colname) != 1){
        stop("⚠ Multiple geometry columns found in table_constraint. Fix it and try again")
      }
      ## Fix geometry column in df if needed
      # df <- df |>
      #   mutate(!!sym(colname) := st_as_sfc(!!sym(colname), EWKB = TRUE, crs = geom_crs)) |>
      #   st_as_sf() |>
      #   st_make_valid() |>
      #   mutate(!!sym(colname) := st_cast(st_collection_extract(!!sym(colname)), geom_type))

      df <- fix_sf_geometry(df, geom_col = colname, geom_crs = geom_crs, geom_type = geom_type)
      # Try to check PostGIS version
      postgis_installed <- TRUE
      tryCatch({
        dbGetQuery(con, "SELECT PostGIS_Version();")
      }, error = function(e) {
        message("⚠ PostGIS not found.")
        postgis_installed <<- FALSE
      })

      # If not installed, create a schema and install PostGIS
      if (!postgis_installed) {
        message("ⓘ  Attempting to create PostGIS extension...")
        # Create public schema if it doesn't exist
        dbExecute(con, "CREATE SCHEMA IF NOT EXISTS public;")
        message("✔ 'public' schema ensured.")

        # Create PostGIS extension in the public schema
        dbExecute(con, "CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;")
        message("✔ PostGIS extension created in 'public' schema.")
      }

      #  Verify PostGIS is now available
      version <- dbGetQuery(con, "SELECT PostGIS_Version();")
      message(paste("✔ PostGIS version:", version[[1]], "\n"))

    }

    # Create table with constraints
    dbExecute(con, sql)
    message("✔ Table created with custom constraints.")

    if(inherits(df, "sf")){
      rpostgis::pgWriteGeom(
        conn = con,                       # your DBI connection
        name = c(schema, table),          # schema + table as character vector
        data.obj = df,                    # your sf object
        geom = colname,              # name of the geometry column in your table
        overwrite = FALSE,                # append, do NOT overwrite table
        row.names = FALSE                 # do not include R row names
      )

      sql_populate <- sprintf(
        "SELECT Populate_Geometry_Columns('%s.%s'::regclass);",
        DBI::dbQuoteIdentifier(con, schema),
        DBI::dbQuoteIdentifier(con, table)
      )

      dbExecute(con, sql_populate)
      message(sprintf("✔ Geometry column '%s' (%s, SRID %d) added properly.",
                      colname, geom_type, geom_crs))
    } else {
      # Only append, since overwrite would drop constraints
      dbWriteTable(con, Id(schema = schema, table = table), df,
                   append = TRUE, row.names = FALSE)
    }
  } else {
    # Let dbWriteTable handle overwrite/append
    dbWriteTable(con, Id(schema = schema, table = table), df,
                 overwrite = overwrite, append = append, row.names = FALSE)
  }

  message(sprintf("✔ Data written to '%s.%s'.", schema, table))
}

# Function to clean up the database by dropping all user-defined schemas except those specified to skip
vacum_db <- function(con, skip_schemas = c("pg_catalog", "information_schema", "pg_toast")) {
  schemas <- dbGetQuery(con, sprintf("
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name NOT IN (%s)
    ORDER BY schema_name;",
                                     paste0("'", skip_schemas, "'", collapse = ",")
  ))$schema_name

  message("🧹 Cleaning up database...")

  if (length(schemas) == 0) {
    message("ⓘ No schemas to drop.")
    return(invisible(NULL))
  }

  for (s in schemas) {
    tryCatch({
      dbExecute(con, sprintf("DROP SCHEMA IF EXISTS %s CASCADE;", DBI::dbQuoteIdentifier(con, s)))
      message(sprintf("✔ Schema '%s' dropped.", s))
    }, error = function(e) {
      warning(sprintf("⚠ Failed to drop schema '%s': %s", s, e$message))
    })
  }

  message("✔ Database cleanup complete.")
  invisible(NULL)
}

fix_sf_geometry <- function(df, geom_col, geom_crs, geom_type) {
  # ensure column exists
  stopifnot(geom_col %in% names(df))

  # 2 Convert to sf if not already
  if (!inherits(df, "sf")) {

    # If geometry column is ALREADY an sfc object → make sf directly
    if (inherits(df[[geom_col]], "sfc")) {
      df <- sf::st_sf(df, sf_column_name = geom_col, crs = geom_crs)

    } else {
      # Otherwise convert using st_as_sfc (for WKT/WKB)
      df[[geom_col]] <- sf::st_as_sfc(df[[geom_col]], EWKB = TRUE, crs = geom_crs)
      df <- sf::st_sf(df, sf_column_name = geom_col, crs = geom_crs)
    }
  }

  # 3 Ensure geometry column is properly set
  if (is.null(sf::st_geometry(df))) {
    sf::st_geometry(df) <- geom_col
  }

  # 4 Make geometries valid (repair invalid ones)
  df <- sf::st_make_valid(df)

  # 5 Ensure correct geometry type (Polygon → MultiPolygon etc.)
  df[[geom_col]] <- sf::st_cast(df[[geom_col]], toupper(geom_type), warn = FALSE)

  # 6 Return fixed sf object
  return(df)
}

# Function to compare column names and types between two data frames
compare_columns <- function(df1, df2) {
  # Compare names
  name_match <- names(df1) == names(df2)

  # Compare types
  type_match <- sapply(df1, class) == sapply(df2, class)

  # Find mismatches
  mismatch_idx <- which(!name_match | !type_match)

  if(length(mismatch_idx) > 0){
    message("❌ Columns with mismatched names or types:")
    for(i in mismatch_idx){
      message(sprintf("Column %d:", i))
      message("  df1 name/type: ", names(df1)[i], "/", paste(class(df1[[i]]), collapse=", "))
      message("  df2 name/type: ", names(df2)[i], "/", paste(class(df2[[i]]), collapse=", "))
    }
  } else {
    message("✔ All column names and types match.")
  }
}

