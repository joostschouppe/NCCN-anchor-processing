## ---------------------------
##
## Script name: Import power stations from OSM
##
## Purpose of script: Load OSM power station data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2025-10-07
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

data_list_id <- "3ee81a13-c999-470e-8ea3-d2597e2949f8"
li_plant_battery <- "6983a9ea-9b92-45f5-b31b-78584fb7716d"
li_plant_biomass <- "46f23aff-9d2b-48dc-98a6-0c1671fe9bae"
li_plant_gas <- "90ff153d-da86-4d67-a6b6-4ecb9e995f2b"
li_plant_hydro <- "e133584c-a87f-4b03-bf3d-eeb930ad0477"		 
li_plant_nuclear <- "fa6e3936-555a-4e18-a272-ae839cb768e3"
li_plant_other_fossil <- "c787e077-f5d7-41f5-a25e-611703621e88"
li_plant_solar <- "6edc6247-a292-4488-b7ca-44a313e09f77"
li_plant_waste <- "970c8917-e0cf-48ef-80dd-dc479ec80985"
li_plant_wind <- "a0984268-231f-46d8-898c-11f613619e2c"

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



# EXTRACT ----
# """""""""""""""""" ----
# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {

# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list <- list("power"="plant")

# Define extra tags to use as columns for properties
extra_columns <- c("owner", "owner:wikidata", "plant:method", "plant:output:electricity", "plant:output:hot_water", "plant:output:heat", "plant:output:steam", 
                   "plant:source", "plant:storage", "plant:type", "ref:EU:ENTSOE_EIC" )
# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")



### Actual OSM download & transformation ----
osm_all <- run_process(
  download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE),
  paste0("OSM download & processing for ", describe_features_list(features_list))
)

# NOTE: we do not de-duplicate these records spatially, because the "inner" power stations are usually of a very different type. E.g. a big gas plant site, where there is also a small battery plant. In order to not lose that important spatial detail, we do not aggregate. *However* in some cases the same information is duplicated or spread out in a confusing way (see e.g. https://osm.org/note/5351438). It makes sense to periodically check for new cases.

# standardize the usual OSM columns
osm_all <- build_osm_derived_columns(osm_all)

# (plant:output:electricity or plant:storage) and plant:source must be filled in ----
osm_filtered <- osm_all %>%
  filter((!is.na(plant_output_electricity) | !is.na(plant_storage)) & !is.na(plant_source))

# NOTE: many wind farms ["plant:source"!="wind"] are not in this export, because multipoint site relations are ignored by the import process
# wind farms are sometimes abused to show the area around a single windfarm. These usually have no name, and normal locations do normally have a name
# so we throw out wind farms that do not have a name
osm_filtered <- osm_filtered %>%  
  filter(plant_source!='wind' | !is.na(name))


# standardize outputs to MW
osm_filtered <- osm_filtered %>%
  mutate(plant_output_electricity_mw = suppressWarnings(case_when(
    grepl("kw", plant_output_electricity, ignore.case = TRUE) ~ as.numeric(sub(" ?KW.*", "", plant_output_electricity,ignore.case = TRUE))/1000,
    grepl("mw", plant_output_electricity, ignore.case = TRUE) ~ as.numeric(sub(" ?MW.*", "", plant_output_electricity,ignore.case = TRUE)),
    grepl("gw", plant_output_electricity, ignore.case = TRUE) ~ as.numeric(sub(" ?GW.*", "", plant_output_electricity,ignore.case = TRUE))*1000,
    TRUE ~ NA)))

osm_filtered <- osm_filtered %>%
  mutate(plant_storage_mw = suppressWarnings(case_when(
    grepl("kw", plant_storage, ignore.case = TRUE) ~ as.numeric(sub(" ?KW.*", "", plant_storage,ignore.case = TRUE))/1000,
    grepl("mw", plant_storage, ignore.case = TRUE) ~ as.numeric(sub(" ?MW.*", "", plant_storage,ignore.case = TRUE)),
    grepl("gw", plant_storage, ignore.case = TRUE) ~ as.numeric(sub(" ?GW.*", "", plant_storage,ignore.case = TRUE))*1000,
    TRUE ~ NA)))

# remove stations with a small production
## This also removes ["plant:output:electricity"!="small_installation"]
## we keep solar plants if they have a missing production size
osm_filtered <- osm_filtered %>%
  filter(
    plant_output_electricity_mw >=0.3 |
      plant_storage_mw >= 0.3 |
    (plant_source=='solar' & is.na(plant_storage_mw) & is.na(plant_output_electricity_mw))
  )

# prepare legend items
osm_filtered <- osm_filtered %>%
  mutate(legend_text = case_when(
    grepl("solar", plant_source, ignore.case = TRUE) ~ 'solar',
    grepl("nuclear", plant_source, ignore.case = TRUE) ~ 'nuclear',
    grepl("biomass", plant_source, ignore.case = TRUE) ~ 'biomass',
    grepl("gas", plant_source, ignore.case = TRUE) ~ 'gas',
    grepl("hydro", plant_source, ignore.case = TRUE) ~ 'hydro',
    grepl("osmotic", plant_source, ignore.case = TRUE) ~ 'hydro',
    grepl("battery", plant_source, ignore.case = TRUE) ~ 'battery',
    grepl("waste", plant_source, ignore.case = TRUE) ~ 'waste',
    grepl("wind", plant_source, ignore.case = TRUE) ~ 'wind',
    grepl("coal|oil", plant_source, ignore.case = TRUE) ~ 'other_fossil',
    TRUE ~ "other"))

#prepare risk level
##nuclear=4
##other fuel-based = 3
##battery = 2
##solar/hydro = 1

osm_filtered <- osm_filtered %>%
  mutate(risk_level = case_when(
    legend_text=='nuclear' ~ 4,
    legend_text=='biomass' | legend_text=='gas' | legend_text=='waste' | legend_text=='other_fossil' ~ 3,
    legend_text=='battery' ~ 2,
    legend_text=='solar' | legend_text=='hydro' | legend_text=='wind' ~ 1))

summary <- as.data.frame(osm_filtered) %>%
  group_by(legend_text) %>%
  summarise(
    total_records = n(),
    missing_output = sum(is.na(plant_output_electricity_mw)),
    total_output = sum(plant_output_electricity_mw, na.rm = TRUE),
    risk_level = mean(risk_level)
  )
print("Summary of processed data:")
print(summary)

# Upload to raw data ----
CreateImportTable(dataset = osm_filtered, schema = "raw_data", table_name = "osm_power_station")

  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function



# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.power_station CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.power_station
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
    CONSTRAINT power_station_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
'https://osm.org/' || osm_id AS original_id,
jsonb_strip_nulls(jsonb_build_object(
      'und', CASE WHEN name IS NULL THEN 'power plant' ELSE name END,
    	'fre', name_fr,
      'ger', name_de::text,
      'dut', name_nl)) as name,
CASE WHEN legend_text='battery' THEN '",li_plant_battery,"'::uuid
	 WHEN legend_text='biomass' THEN '",li_plant_biomass,"'::uuid
	 WHEN legend_text='gas' THEN '",li_plant_gas,"'::uuid
	 WHEN legend_text='hydro' THEN '",li_plant_hydro,"'::uuid		 
	 WHEN legend_text='nuclear' THEN '",li_plant_nuclear,"'::uuid
	 WHEN legend_text='other_fossil' THEN '",li_plant_other_fossil,"'::uuid
	 WHEN legend_text='solar' THEN '",li_plant_solar,"'::uuid
	 WHEN legend_text='waste' THEN '",li_plant_waste,"'::uuid
	 WHEN legend_text='wind' THEN '",li_plant_wind,"'::uuid
  END as legend_item_id,
risk_level,
other_names,address,local_email,operator_email,local_phone,local_website,
operator_website,operator_wikidata, operator, operator_type, image,
wikidata, owner, owner_wikidata, plant_method, plant_output_electricity_mw, plant_output_hot_water, plant_output_heat, 
plant_output_steam,
plant_source, plant_storage_mw, plant_type, ref_eu_entsoe_eic,
geometry
FROM raw_data.osm_power_station)

INSERT INTO ingestion.power_station 
(original_id, name, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'address', address,
	'other_names', other_names,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'image', image,
	'wikidata', wikidata,
	'owner', owner,
	'owner_wikidata', owner_wikidata,
	'plant_method', plant_method,
	'plant_output_electricity_mw', plant_output_electricity_mw || ' MW',
	'plant_output_hot_water', NULLIF(CONCAT_WS(\'; ', plant_output_hot_water,	plant_output_heat, plant_output_steam),''),
	'plant_source', plant_source,
	'plant_storage_mw', plant_storage_mw, 
	'plant_type', plant_type,
	'ref_eu_entsoe_eic', ref_eu_entsoe_eic)) as properties,
ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.power_station OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.power_station TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.power_station TO pgn_user_airflow;"
)


### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
#overrule_checks<-TRUE


run_smart_update = function() {
  smart_update_process("power_station", 50, 200, 100, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
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



