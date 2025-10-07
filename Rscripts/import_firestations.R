## ---------------------------
##
## Script name: ETL flow for firestations
##
## Purpose of script: Load OSM firestation data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-20
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# data references
data_list_id <- "b3f833af-1c7a-4d03-8ceb-ffd55bfea5f8"
legend_item_id_firestation_be_official <- "64ab1090-dae0-4c3f-b1d2-a5133e9c7abb"
legend_item_id_firestation_other <- "6ec58334-645f-49e2-a700-3d4ec51c18d1"

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
## Wikidata library
library(WikidataQueryServiceR)





# EXTRACT ----
# """""""""""""""""" ----

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    
    # Download wikidata ----
    ### make a query (start at https://query.wikidata.org/querybuilder/?uselang=nl and use the "show query in the query service" interface)
    sparql_query <- "SELECT DISTINCT ?zone ?nl ?fr ?de ?website ?phone ?email ?kbo_bce WHERE {
  ?zone wdt:P31 wd:Q3575878.
  OPTIONAL { ?zone wdt:P856 ?website. }
  OPTIONAL { ?zone wdt:P1329 ?phone. }
  OPTIONAL { ?zone wdt:P968 ?email. }
  OPTIONAL { ?zone wdt:P3376 ?kbo_bce. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"nl\". ?zone rdfs:label ?nl. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"fr\". ?zone rdfs:label ?fr. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language \"de\". ?zone rdfs:label ?de. }
  FILTER NOT EXISTS { ?zone wdt:P576 ?dissolvedDate. }
}"
    
    ### load the actual data
    zones <- query_wikidata(sparql_query, format = c("simple", "smart"))
    ### give nice names
    zones_cleaned <- zones %>% rename(operator_wikidata=zone,zone_name_nl=nl,zone_name_fr=fr,zone_name_de=de)
    ### create a simple wikidata number variable
    zones_cleaned$operator_wikidata <- gsub("^http://www.wikidata.org/entity/", "", zones_cleaned$operator_wikidata)
    zones_cleaned$email <- gsub("^mailto:", "", zones_cleaned$email)
    ### remove fake names
    zones_cleaned <- zones_cleaned %>%
      mutate(
        zone_name_nl = ifelse(grepl("^Q[0-9]+$", zone_name_nl), NA, zone_name_nl),
        zone_name_fr = ifelse(grepl("^Q[0-9]+$", zone_name_fr), NA, zone_name_fr),
        zone_name_de = ifelse(grepl("^Q[0-9]+$", zone_name_de), NA, zone_name_de)
      )
    
    ### de-duplicate (caused by the website, which can have multiple values)
    zones_cleaned <- zones_cleaned %>%
      group_by(operator_wikidata) %>%
      summarize(
        zone_name_nl = first(zone_name_nl),
        zone_name_fr = first(zone_name_fr),
        zone_name_de = first(zone_name_de),
        phone=first(phone),
        email=first(email),
        kbo_bce=first(kbo_bce),
        website = paste(website, collapse = "; ")
      )
    
    zones_cleaned <<- zones_cleaned %>%
      rename(w_phone=phone,w_email=email,w_website=website,w_kbo_bce=kbo_bce)
    
    
    
    
    
    # Download OSM data ----
    ### OSM DOWNLOAD PARAMETERS ----
    
    # Define the list of features
    features_list <- list("amenity" = "fire_station")

        # Define extra tags to use as columns for properties
    extra_columns <- c("emergency_phone", "fire_station:type", 
                       "fire_station:type:FR","emergency","operator:phone")
    
    # Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
    datatypes <- c("points", "mpolygon")
    
    
    ### Actual OSM download & transformation ----
	osm_all <<- run_process(
		download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
		paste0("OSM download & processing for ", paste(paste(names(features_list), unlist(features_list), sep = "="), collapse = ", "), collapse = ", ")
	)
    
    
    # Test the quality: if it is in Belgium, it should have an operator_type, and if it is emergency_zone it should have an operator_wikidata tag
    # mapping guidelines at https://wiki.openstreetmap.org/wiki/WikiProject_Belgium/Firestations
    
    osm_all_problems <- osm_all %>% filter(
      (is.na(operator_type) & !is.na(language)) |
        (is.na(operator_wikidata) & operator_type=='emergency_zone' & !is.na(language))
    )
    
    # if osm_all_problems has records, save them to log as geojson
    if (nrow(osm_all_problems)>0){
      filename_visualization<-file.path(log_folder, paste0("fire_station_problems", format(Sys.time(), "%Y%m%d_%H%M%S"), ".geojson"))
      st_write(osm_all_problems, filename_visualization, driver = "GeoJSON")
      print(paste0("OSM data issues need to be fixed first, check them at ", filename_visualization))
      print(paste0("Affected OSM ids:",osm_all_problems$osm_id))
    } else {
      print("OSM data quality check passed")
    }
    
    # add a stop if there are problems
    if (nrow(osm_all_problems)>0){
      stop("OSM data quality check failed")
    }
    
    
    
    
    
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
DROP TABLE IF EXISTS ingestion.firestations CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.firestations
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
    CONSTRAINT firestations_pkey PRIMARY KEY (id)
  );
",paste0("WITH mergewiki AS(
SELECT f.*, s.zone_name_nl, s.zone_name_fr, s.zone_name_de, s.w_phone, s.w_email, s.w_kbo_bce, s.w_website FROM raw_data.osm_firestations f
LEFT JOIN raw_data.wikidata_be_safetyzones s ON f.operator_wikidata=s.operator_wikidata),

cleaned as (SELECT
'https://osm.org/' || osm_id as original_id,
jsonb_strip_nulls(jsonb_build_object(
    'und', CASE WHEN name IS NULL AND name_nl IS NULL AND name_fr IS NULL AND name_de IS NULL AND operator_type='industrial' then 'industrial firestation'
				WHEN name IS NULL AND name_nl IS NULL AND name_fr IS NULL AND name_de IS NULL AND operator_type='industrial' then 'airport firestation'
				WHEN name IS NULL AND name_nl IS NULL AND name_fr IS NULL AND name_de IS NULL AND operator_type='military' then 'military firestation'
				WHEN name IS NULL AND name_nl IS NULL AND name_fr IS NULL AND name_de IS NULL then 'firestation'
				ELSE name END,
    'dut', CASE WHEN name IS NULL AND name_nl IS NULL AND operator_type='industrial' then 'brandweerkazerne industriezone'
				WHEN name IS NULL AND name_nl IS NULL AND operator_type='industrial' then 'brandweerkazerne luchthaven'
				WHEN name IS NULL AND name_nl IS NULL AND operator_type='military' then 'militaire brandweerkazerne'
				WHEN name IS NULL AND name_nl IS NULL then 'brandweerkazerne' 
				ELSE name_nl END,
	    'fre', CASE WHEN name IS NULL AND name_fr IS NULL AND operator_type='industrial' then 'caserne de pompiers zone industrielle'
				WHEN name IS NULL AND name_fr IS NULL AND operator_type='industrial' then 'caserne de pompiers aéroport'
				WHEN name IS NULL AND name_fr IS NULL AND operator_type='military' then 'caserne de pompiers militaire'
				WHEN name IS NULL AND name_fr IS NULL then 'caserne de pompiers' 
				ELSE name_fr END,
	    'ger', CASE WHEN name IS NULL AND name_de IS NULL AND operator_type='industrial' then 'Feuerwache Industriegebiet'
				WHEN name IS NULL AND name_de IS NULL AND operator_type='industrial' then 'Flughafenfeuerwache'
				WHEN name IS NULL AND name_de IS NULL AND operator_type='military' then 'Militärische Feuerwache'
				WHEN name IS NULL AND name_de IS NULL then 'Feuerwache' 
				ELSE name_de END
)) as name,
jsonb_build_object(
	'dut', 	CASE WHEN operator_type='emergency_zone' THEN 'brandweerpost'
			ELSE 'brandweerkazerne (niet van hulpverleningszone)' END,
	'eng', 	CASE WHEN operator_type='emergency_zone' THEN 'fire station'
			ELSE 'fire station (non-emergency zone)' END,
	'fre', CASE WHEN operator_type='emergency_zone' THEN 'poste de pompiers'
	ELSE 'caserne de pompiers (pas d''une zone de secours)' END,
	'ger', CASE WHEN operator_type='emergency_zone' THEN 'Feuerwachen'
	ELSE 'Feuerwachen (nicht von Hilfeleistungszone)' END) as legend_item,
CASE WHEN operator_type='emergency_zone' THEN '",legend_item_id_firestation_be_official,"'::uuid
  ELSE '",legend_item_id_firestation_other,"'::uuid
  END as legend_item_id,
NULLIF(CONCAT_WS('; ',short_name, official_name, alt_name, old_name), '') AS other_names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END
	AS address,
NULLIF(CONCAT_WS('; ',fire_station_type, mergewiki.fire_station_type_fr), '') AS firestation_type,
NULLIF(CONCAT_WS('; ',contact_email, email), '') AS email,
NULLIF(CONCAT_WS('; ',operator_email, w_email), '') AS operator_email,
NULLIF(CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2), '') AS phone,
NULLIF(CONCAT_WS('; ',operator_phone, w_phone), '') AS operator_phone,
NULLIF(CONCAT_WS('; ',website, contact_website), '') AS website,
NULLIF(CONCAT_WS('; ',operator_website, w_website), '') AS operator_website,
operator_wikidata, operator, operator_type, emergency, image, geometry
FROM mergewiki)


INSERT INTO ingestion.firestations 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
0 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'other_names', other_names,
	'address', address,
	'firestation_type', firestation_type,
	'email',email,
	'phone',phone,
	'operator_phone',operator_phone,
	'website',website,
	'operator_website',operator_website,
	'operator_wikidata',operator_wikidata,
	'operator',operator,
	'operator_type',operator_type,
	'ambulance_station', CASE WHEN emergency='ambulance_station' THEN 'yes' ELSE NULL END,
	'image', image)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.firestations OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.firestations TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.firestations TO pgn_user_airflow;")


### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}

# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-overrule_checks


run_smart_update = function() {
  smart_update_process("firestations", 75, 250, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    CreateImportTable(dataset = osm_all, schema = "raw_data", table_name = "osm_firestations")  
    CreateImportTable(dataset = zones_cleaned, schema = "raw_data", table_name = "wikidata_be_safetyzones")
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}

