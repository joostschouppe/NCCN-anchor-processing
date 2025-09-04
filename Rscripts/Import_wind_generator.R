## ---------------------------
##
## Script name: Import wind generator from OSM
##
## Purpose of script: Load OSM wind generators data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-27
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# data references
data_list_id<-"2eb757ff-e4cd-46c8-aeba-c0b03f982ebf"
legend_item_id<-"d7e4c3da-a5e1-45f5-b79e-926f7a354f10"

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

# connection details
db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

# run status
run_status<-Sys.getenv("RUN_STATUS")
## this is set to false and prevents any accidental changes to the database by switching off the main_function(). On Airflow, this is set to true.
run_status<-ifelse(tolower(run_status) == "true", TRUE, FALSE)

# overrule the checks
overrule_checks<-Sys.getenv("OVERRULE_CHECKS")
## Set to FALSE by default. That means we do not update the anchors if some tests fail. Those tests include "the data has grown or shrunk by a lot of objects". If, after review of the log, you decide that nothing is wrong, set this manually to TRUE.
# If the input is not correctly understood as boolean, this will force it to it.
overrule_checks<-ifelse(tolower(overrule_checks) == "true", TRUE, FALSE)


# Do not run the main part of the processing, but just do an update based on the ingestion table already in the dbase
reuse_ingestion_data<-Sys.getenv("REUSE_INGESTION_DATA")
reuse_ingestion_data<-ifelse(tolower(reuse_ingestion_data) == "true", TRUE, FALSE)

# Only run the comparison script & update the ingestion table, but do not attempt to update the transformation table
do_dry_run<-Sys.getenv("DO_DRY_RUN")
do_dry_run<-ifelse(tolower(do_dry_run) == "true", TRUE, FALSE)



log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))

# Libraries -------------------------------
# """""""""""""""""" ----------------------

# everything loaded via utils




# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    
    # Download OSM data ----
    ### OSM DOWNLOAD PARAMETERS ----
    
    # Define the list of features
    features_list <- list("generator:source" = "wind")
    
    # Define extra tags to use as columns for properties
    extra_columns <- c("generator:output:electricity", "generator:type", "generator:model", "manufacturer", "manufacturer:ref","manufacturer:type",
                       "manufacturer:url","model", "rotor:diameter", "diameter", "est_height:hub", "hub:height", 
                       "height:hub", "height", "power","offshore","ref")
    # Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
    datatypes <- c("points", "mpolygon")
    
    
    
    ### Actual OSM download & transformation ----
    
    tryCatch({
      # Call the large function
      osm_all<-download_osm_process(features_list, datatypes, extra_columns, postgres=TRUE)
      print("OSM data downloaded & processes succesfully")
    }, error = function(e) {
      # Print error message
      print(paste("Something went wrong:", e$message))
    })
    
    
    
    # remove if power=* is missing (this is usually because that tag has been used to archive the object with a lifecycle tag) or generator:output:electricity=small_installation
    osm_all <- osm_all %>%
      filter(!is.na(power)) %>%
      filter(!(generator_output_electricity=='small_installation') | is.na(generator_output_electricity))
    
    # if generator_output_electricity contains kW or kw, create new numeric column output_kw
    osm_all <- osm_all %>%
      mutate(output_kw = ifelse(grepl("kW", generator_output_electricity, ignore.case = TRUE),
                                as.numeric(gsub(" kW", "", generator_output_electricity, fixed = TRUE)),NA))
    
    # keep if output_kw>300 or output_kw is NA and make available outside the function
    ## reasoning: we throw out 'small installations' and things with a very small production. We don't expect things written in MW to have a value of 0.3 MW or less. We also expect a lot of missing data, and assume it's a large installation when there is no data
    osm_all <- osm_all %>%
        filter(is.na(output_kw) | output_kw>300)

    CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_wind_generators") 
    
    
  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function


# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function



# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.wind_generators CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.wind_generators
  (
  id uuid NOT NULL DEFAULT gen_random_uuid(),
  original_id text,    
  name jsonb,
  legend_item jsonb,
  legend_item_id uuid,
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
    CONSTRAINT wind_generators_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
'https://osm.org/' || osm_id AS original_id,
jsonb_build_object(
    	'fre', 'éolienne',
    	'ger', 'Windkraftanlage',
    	'dut', 'windturbine',
    	'eng', 'wind turbine') as name,
jsonb_build_object(
    	'fre', 'éolienne',
    	'ger', 'Windkraftanlage',
    	'dut', 'windturbine',
    	'eng', 'wind turbine') as legend_item,
NULLIF(CONCAT_WS('; ',name, short_name, official_name, alt_name, old_name), '') AS names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END
	AS address,
NULLIF(CONCAT_WS('; ',contact_email, email), '') AS local_email,
operator_email,
NULLIF(CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2),'') AS local_phone,
NULLIF(CONCAT_WS('; ',website, contact_website),'') AS local_website,
operator_website,
operator_wikidata, operator, operator_type, image, 
generator_output_electricity as generator_output, 
CASE WHEN generator_type = 'horizontal_axis' THEN 'vertical rotor'
	 WHEN generator_type = 'vertical_axis' THEN 'horizontal rotor'
	 ELSE NULL END as rotor_orientation,
NULLIF(CONCAT_WS(',',manufacturer_ref,generator_model,manufacturer_type,model), '') AS model_info,
manufacturer,manufacturer_url, 
TRIM(TRAILING 'mM ' FROM COALESCE(rotor_diameter, diameter)) as rotor_diameter, 
NULLIF(CASE WHEN est_height_hub IS NOT NULL THEN CONCAT(est_height_hub,' (estimated)') 
	ELSE TRIM(TRAILING 'mM ' FROM CONCAT_WS(',',hub_height,hub_height,height_hub)) END, '')
	as hub_height,
height as total_height,
CASE WHEN offshore='yes' THEN 'yes' ELSE NULL END AS offshore,ref as reference,
geometry
FROM raw_data.osm_wind_generators)

INSERT INTO ingestion.wind_generators 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",legend_item_id,"'::uuid as legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
1 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'generator_output', generator_output,
	'rotor_orientation', rotor_orientation,
	'rotor_diameter', rotor_diameter,
	'model_info', model_info,
	'manufacturer', manufacturer,
	'manufacturer_url', manufacturer_url,
	'hub_height', hub_height,
	'total_height', total_height,
	'offshore', offshore,
	'reference', reference,
	'names', names,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image)) as properties,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.wind_generators OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.wind_generators TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.wind_generators TO pgn_user_airflow;")


### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}

# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-overrule_checks



run_smart_update = function() {
  smart_update_process("wind_generators", 50, 200, 100, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}

