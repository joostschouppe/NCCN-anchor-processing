## ---------------------------
##
## Script name: Import population grid
##
## Purpose of script: Upload population grid data to Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-04-09
##
##
## ---------------------------

# Set parameters ------
# TODO: this should not be necessary, but it is on my setup
readRenviron("C:/projects/pgn-data-airflow/.Renviron")

local_folder <- "C:/temp/popgrid/"
rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"

# Folder for logs
log_folder <- "C:/temp/logs/"

data_list_id<-'30fbee39-6e2a-4ab3-a32f-8728a35d58bf'

# Load external functions ------
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))


# Libraries -------------------------------
# """""""""""""""""" ----------------------

## Base libraries
library(sf)
library(RPostgres)
library(DBI)

## Data processing libraries
library(dplyr)




# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

## Prepare connection
get_con<-function(){
  con_pg <- dbConnect(Postgres(),
                      user=postgres_user, 
                      password=postgres_password,
                      host=db_host_name,
                      dbname=db_name,
                      port=5432, 
                      sslmode = 'prefer')
  return(con_pg)
}



# EXTRACT ----
# """""""""""""""""" ----


grid <- st_read(paste0(local_folder,"POP_GRID_2023_3035.shp"))

# drop pop_km2 column if exists
if("pop_km2" %in% names(grid)){
  grid <- grid %>% select(-pop_km2)
}

#reproject to 4326

grid <- st_transform(grid, 4326)



CreateImportTable<-function(dataset, schema, table_name){
  if(exists("dataset")){
    con_pg<-get_con()
    table_id <- DBI::Id(
      schema  = schema,
      table   = table_name
    )
    table_id_t <- paste0(schema,".",table_name)
    start<-Sys.time()
    print(paste0("Start :",format(Sys.time(), "%a %b %d %X %Y")))
    print(paste0("Import data into postgresql table ", table_id_t))
    dbWriteTable(con_pg, table_id, dataset, overwrite = TRUE, row.names = FALSE )
    
    print("ID primary key")
    query <- paste("ALTER TABLE ", table_id_t,
                   "ADD COLUMN ogc_fid SERIAL;")
    dbExecute(con_pg, query)
    query <- paste("ALTER TABLE ", table_id_t,
                   "ADD PRIMARY KEY (ogc_fid);")
    dbExecute(con_pg, query)
    
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
    
  }else{
    print(paste0("Error, the geojson you wanted to import into ", table_id_t, "does not exist, try again"))
  }
}





# LOAD ----
# """""""""""""""""" ----

### Create SQL for transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.population_grid CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.population_grid
  (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    original_id text,    
    name jsonb,
    legend_item jsonb,
	data_list_id uuid,
	risk_level integer,
    properties jsonb,
	properties_secondary jsonb,
	imported_at timestamptz,
	tags jsonb,
	deleted_at timestamptz,
	updated_at timestamptz,
	created_at timestamptz,
	created_by uuid,
	updated_by uuid,
    geometry geometry(geometry, 4326),
    CONSTRAINT population_grid_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT CONCAT(x_3035,'_',y_3035) as original_id, 
ms_len as cell_length, ms_pop as population, ms_hh as households,
ms_km2 as area_km2, ms_pop/ms_km2 AS pop_density, ms_hh/ms_km2 AS hh_density, geometry  FROM raw_data.population_grid)
INSERT INTO transformation.population_grid
( original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at) 
SELECT original_id, 
jsonb_strip_nulls(jsonb_build_object(
              'und', original_id,
              'fre', original_id,
              'ger', original_id,
              'dut', original_id)) as name,
            jsonb_build_object(
              'dut', 'cel bevolkingsraster',
              'fre', 'celule du grille de population',
              'ger', 'Zelle des Bevölkerungsgitters',
              'eng', 'cell of the population grid') as legend_item,
'",data_list_id,"' as data_list_id,
0 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'cell_length',cell_length,
	'population',population,
	'households',households,
	'pop_density',pop_density,
	'hh_density',hh_density)) as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"))


### Create fdw views ----
fdw_views_sql <- c("
DROP VIEW IF EXISTS fdw.fdw_population_grid CASCADE;
","
CREATE OR REPLACE VIEW fdw.fdw_population_grid
AS
SELECT row_number() OVER () AS gid,
id,
original_id,
name,
legend_item,
NULL::uuid as best_address_id,
NULL::uuid as capakey_id,
data_list_id,
risk_level,
properties,
properties_secondary,
imported_at,
tags,
deleted_at,
updated_at,
created_at,
created_by,
updated_by,
geometry,
st_pointonsurface(geometry) AS geometry_pt
FROM transformation.population_grid;
","
ALTER TABLE fdw.fdw_population_grid
OWNER TO paragon;
","
GRANT SELECT ON TABLE fdw.fdw_population_grid TO fdw4dev;
","
GRANT ALL ON TABLE fdw.fdw_population_grid TO paragon;
","
GRANT SELECT ON TABLE fdw.fdw_population_grid TO fdw4dev WITH GRANT OPTION;
","
GRANT ALL ON TABLE fdw.fdw_population_grid TO paragon;
","
GRANT ALL ON TABLE fdw.fdw_population_grid TO pgn_group_data_team_w WITH GRANT OPTION;                   
")









create_transformation_table <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in transformation_table_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("Ingestion table SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the transformation table failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}



create_fdw_views <- function() {
  con_pg <- get_con()
  tryCatch(
    {
      for (sql_command in fdw_views_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("Ingestion table SQL ran without error")
    },
    error = function(err) {
      print("The SQL functions for the FDW views failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}

CreateImportTable(dataset = grid, schema = "raw_data", table_name = "population_grid")  
create_transformation_table()
create_fdw_views()





