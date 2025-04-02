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
readRenviron("C:/projects/pgn-data-airflow/.Renviron")

local_folder <- "C:/temp/popgrid/"
rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"

# Folder for logs
log_folder <- "C:/temp/logs/"

data_list_id<-'30fbee39-6e2a-4ab3-a32f-8728a35d58bf'

# Load external functions ------
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))


# Libraries -------------------------------
# """""""""""""""""" ----------------------






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


download_and_extract_shp <- function(url) {
  ## Download a ZIP -----------------------------------------------------------------
  zip_file <- tempfile()
  GET(url, write_disk(zip_file))
  
  ## Extract the content -----------------------------------------------------------------
  unzipped <- unzip(zip_file, exdir = tempdir(), overwrite = TRUE)
  
  ## Extract the .shp file path
  shp_file <- unzipped[grep("\\.shp$", unzipped)]
  
  # Read the shapefile into an SF object
  sf_dataset <- st_read(shp_file)
  
  
  ## Suppression du fichier ZIP -----------------------------------------------------------------
  file.remove(zip_file)
  
  return(sf_dataset)
}


# check https://statbel.fgov.be/nl/open-data/datalab-grid-van-de-bevolking-met-cellen-van-variabele-grootte for updates
grid <- download_and_extract("https://statbel.fgov.be/sites/default/files/files/opendata/SH_VARYING_CELL_SIZE_GRID/POP_GRID_2024_3035.shp.zip")


#reproject to 4326
grid <- st_transform(grid, 4326)








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
ms_len as cell_length, ms_pop as population, 
--ms_hh as households,
ms_km2 as area_km2, ms_pop/ms_km2 AS pop_density, 
--ms_hh/ms_km2 AS hh_density, 
geometry  FROM raw_data.population_grid)
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
	--'households',households,
	'pop_density',pop_density
	--,'hh_density',hh_density
	)) as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS transformation.population_grid
    OWNER to pgn_group_data_team_w;","
GRANT ALL ON TABLE transformation.population_grid TO pgn_group_data_team_w;")



create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}



CreateImportTable(dataset = grid, schema = "raw_data", table_name = "population_grid")  

# Before running this, make sure you have a backup of the fdw and vt_fdw based on this table
# Also, make a copy of the current table with name population_grid_2024 (if you're processing 2025 data)
create_transformation_table()






