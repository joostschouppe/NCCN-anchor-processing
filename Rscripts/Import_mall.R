## ---------------------------
##
## Script name: Import shopping malls
##
## Purpose of script: Load OSM shopping malls & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2025-03-20
##
##
## ---------------------------

# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

data_list_id <- "236626f3-af57-42f9-8113-2d7c383bdfa2"
legend_item_id <- "12f749c3-77c6-44ea-8792-052236e57aa3"

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

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


data_list_id<-"236626f3-af57-42f9-8113-2d7c383bdfa2"

temporary_folder <-Sys.getenv("TEMPORARY_STORAGE")
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")


# Libraries -------------------------------
# """""""""""""""""" ----------------------

# no extra libraries needed

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))




# EXTRACT ----
# """""""""""""""""" ----


DownloadOSM <- function(){
  tryCatch({

# Download OSM data ----
### OSM DOWNLOAD PARAMETERS ----

# Define the list of features
features_list_1 <- list("shop" = "mall")
features_list_2 <- list("shop" = "department_store")

# Define extra tags to use as columns for properties
extra_columns <- c("shop", "max_level", "min_level", "brand:wikidata")

# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("points", "mpolygon")


### Actual OSM download & transformation ----


tryCatch({
  # Call the large function
  osm_mall<-download_osm_process(features_list_1, datatypes, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

tryCatch({
  # Call the large function
  osm_dept<-download_osm_process(features_list_2, datatypes, extra_columns, postgres=TRUE)
  print("OSM data downloaded & processes succesfully")
}, error = function(e) {
  # Print error message
  print(paste("Something went wrong:", e$message))
})

# remove if no name and remove artefacts from download
osm_dept<-osm_dept %>%
  filter(shop=="department_store" & !is.na(name))

# only keep relevant department stores:
## INNO Q300632, La Fayette Q3094686 , Galeria (Karstadt of Kaufhof) Q80220059 (brand:wikidata), De Bijenkorf Q2063937
osm_dept<-osm_dept %>%
  filter(grepl("Q300632|Q3094686|Q80220059|Q2063937", `brand_wikidata`))


# remove if no name and remove artefacts from download
osm_mall<-osm_mall %>%
  filter(shop=="mall" & !is.na(name))

# merge both sets
osm_all<-rbind(osm_mall, osm_dept)


# detect objects on top of other objects in the dataset
## first define a set of potential outers
osm_outers<-osm_all %>%
  select(outer_id=osm_id) %>%
  filter(!st_is(geometry, "POINT"))
## then join them to the objects
join <- st_join(osm_all, osm_outers, join = st_within)

# count the number of times an inner is used (to later find objects that are linked to itself and something else too, or to more than one outer)
join<-join %>%
  mutate(outer_id=case_when(!is.na(outer_id)~outer_id, TRUE~osm_id)) %>%
  group_by(osm_id) %>%
  mutate(count_inner=case_when(!is.na(outer_id) ~n(), TRUE~NA))
# remove if the inner is used by an outer but still refers to itself as well
join<-join %>%
  filter(count_inner==1 | (count_inner>1 & osm_id!=outer_id))


# add a count with the number of times an outer is used
join<-join %>%
  mutate(outer_id=case_when(!is.na(outer_id)~outer_id, TRUE~osm_id)) %>%
  group_by(outer_id) %>%
  mutate(count=case_when(!is.na(outer_id) ~n(), TRUE~NA))

# count inners again and give them an order_inner number, where 1 is for the case linked to the biggest count (outer)
join<-join %>%
  group_by(osm_id) %>%
  mutate(count_inner=case_when(!is.na(outer_id) ~n(), TRUE~NA)) %>%
  arrange(desc(count)) %>%
  mutate(order_inner = row_number())

# if count_inner>1, take the first one (i.e. the one linked to the biggest polygon)
join<-join %>%
  filter(count_inner==1 | (count_inner>1 & order_inner==1))



# create aggregated variables
join <- join %>%
  group_by(outer_id) %>%
  mutate(operator= ifelse(count > 1, paste(unique(na.omit(operator)), collapse = "; "), operator)) %>%
  mutate(addr_city= ifelse(count > 1, paste(unique(na.omit(addr_city)), collapse = "; "), addr_city)) %>%
  mutate(addr_housenumber= ifelse(count > 1, paste(unique(na.omit(addr_housenumber)), collapse = "; "), addr_housenumber)) %>%
  mutate(addr_street= ifelse(count > 1, paste(unique(na.omit(addr_street)), collapse = "; "), addr_street)) %>%
  mutate(addr_postcode= ifelse(count > 1, paste(unique(na.omit(addr_postcode)), collapse = "; "), addr_postcode)) %>%
  mutate(contact_email= ifelse(count > 1, paste(unique(na.omit(contact_email)), collapse = "; "), contact_email)) %>%
  mutate(email= ifelse(count > 1, paste(unique(na.omit(email)), collapse = "; "), email)) %>%
  mutate(website= ifelse(count > 1, paste(unique(na.omit(website)), collapse = "; "), website)) %>%
  mutate(contact_website= ifelse(count > 1, paste(unique(na.omit(contact_website)), collapse = "; "), contact_website)) %>%
  mutate(phone= ifelse(count > 1, paste(unique(na.omit(phone)), collapse = "; "), phone)) %>%
  mutate(contact_phone= ifelse(count > 1, paste(unique(na.omit(contact_phone)), collapse = "; "), contact_phone)) %>%
  mutate(opening_hours= ifelse(count > 1, paste(unique(na.omit(opening_hours)), collapse = "; "), opening_hours)) %>%
  mutate(mobile= ifelse(count > 1, paste(unique(na.omit(mobile)), collapse = "; "), mobile)) %>%
  mutate(contact_mobile= ifelse(count > 1, paste(unique(na.omit(contact_mobile)), collapse = "; "), contact_mobile)) %>%
  mutate(check_date= ifelse(count > 1, paste(unique(na.omit(check_date)), collapse = "; "), check_date)) %>%
  mutate(wikidata= ifelse(count > 1, paste(unique(na.omit(wikidata)), collapse = "; "), wikidata)) %>%
  mutate(operator_wikidata= ifelse(count > 1, paste(unique(na.omit(operator_wikidata)), collapse = "; "), operator_wikidata)) %>%
  mutate(phone_2= ifelse(count > 1, paste(unique(na.omit(phone_2)), collapse = "; "), phone_2)) %>%
  mutate(alt_website= ifelse(count > 1, paste(unique(na.omit(alt_website)), collapse = "; "), alt_website)) %>%
  mutate(operator_website= ifelse(count > 1, paste(unique(na.omit(operator_website)), collapse = "; "), operator_website)) %>%
  mutate(shop= ifelse(count > 1, paste(unique(na.omit(shop)), collapse = "; "), shop)) %>%
  mutate(min_level= ifelse(count > 1, paste(unique(na.omit(min_level)), collapse = "; "), min_level)) %>%
  mutate(max_level= ifelse(count > 1, paste(unique(na.omit(max_level)), collapse = "; "), max_level))

  
  
# aggregate names
aggregate_names <- function(df, names_to_aggregate) {
  for (col in names_to_aggregate) {
    df <- df %>%
      group_by(outer_id) %>%
      mutate(
        primary_name = ifelse(!is.na(get(col))[osm_id == outer_id][1], get(col)[osm_id == outer_id][1], NA),
        other_names = paste(unique(na.omit(get(col)[osm_id != outer_id & get(col) != primary_name])), collapse = ", "),
        across(col, ~ ifelse(!is.na(.), case_when(
          count > 1 & !is.na(other_names) & other_names != "" ~ ifelse(is.na(primary_name) | primary_name == "", other_names, paste0(primary_name, " (", other_names, ")")),
          TRUE ~ .
        ), .))
      ) %>%
      ungroup() %>%
      select(-primary_name, -other_names)  # Remove temporary columns if not needed
  }
  
  return(df)
}


# Define your list of columns to aggregate
names_to_aggregate <- c("name", "name_nl", "name_fr", "name_de", "alt_name", "short_name", "official_name", "old_name")

# Call the function to aggregate names
join <- aggregate_names(join, names_to_aggregate)



# Filter records based on the count and outer_id conditions
join_filtered <- join %>%
  filter(count == 1 | (count > 1 & outer_id == osm_id)) %>%
  select(-count,-outer_id)

# confirm as SF dataset
join_filtered <- st_as_sf(join_filtered, "geometry")

# set all empty values in strings to missing
replace_empty_with_null <- function(x) {
  ifelse(x == "", NA_character_, x)
}
string_columns <- names(join_filtered)[sapply(join_filtered, is.character) & names(join_filtered) != "geometry"]
join_filtered <- join_filtered %>%
  mutate_at(vars(all_of(string_columns)), ~ replace_empty_with_null(.))


return(join_filtered)  
  }, 
error = function(e) {
  print(paste("Error downloading OSM:", e))
  return(NULL)  # Return NULL on failure
})
}


# Upload to raw data ----

# CreateImportTable is loaded via utils and called in the main function




# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.mall CASCADE;
",paste0("
CREATE TABLE IF NOT EXISTS ingestion.mall
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
  CONSTRAINT mall_pkey PRIMARY KEY (id)
);"),
paste0("WITH 
cleaned as (SELECT
'https://osm.org/' || osm_id as original_id,
            jsonb_strip_nulls(jsonb_build_object(
              'und', name,
              'fre', name_fr,
              'ger', name_de::text,
              'dut', name_nl)) as name,
            jsonb_build_object(
              'dut', 'winkelcentrum',
              'fre', 'centre commercial',
              'ger', 'Einkaufszentrum',
              'eng', 'shopping mall') as legend_item,
            CASE WHEN short_name IS NULL AND official_name IS NULL AND alt_name IS NULL AND old_name IS NULL THEN NULL 
	ELSE CONCAT_WS('; ',short_name, official_name, alt_name, old_name) END AS other_names,
CASE WHEN addr_street IS NULL THEN NULL 
	ELSE LTRIM(CONCAT(addr_street, ' ' || CASE WHEN nohousenumber='yes' THEN 'w/n' ELSE addr_housenumber END, ', ' || CONCAT((addr_postcode || ' '), addr_city))) END
	AS address,
CASE WHEN contact_email IS NULL AND email IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_email, email) END AS local_email,
operator_email,
CASE WHEN contact_mobile IS NULL AND mobile IS NULL AND contact_phone IS NULL AND phone IS NULL AND phone_2 IS NULL THEN NULL
	ELSE CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2) END AS local_phone,
CASE WHEN website IS NULL AND contact_website IS NULL THEN NULL
	ELSE CONCAT_WS('; ',website, contact_website) END AS local_website,
operator_website,operator_wikidata,operator,
min_level,max_level,
	geometry FROM raw_data.osm_mall)
INSERT INTO ingestion.mall 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item,
'",legend_item_id,"'::uuid as legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
1 as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'other_names', other_names,
  'address', address,
	'local_email',local_email,
	'local_phone',local_phone,
	'local_website',local_website,
	'operator_email',operator_email,
	'operator_website',operator_website,
	'operator',operator,
	'building_lowest_level',min_level,
	'building_highest_level',max_level
)),
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.mall OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.mall TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.mall TO pgn_user_airflow;")



### Execute the SQL commands ----


create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}

run_smart_update = function() {
  smart_update_process("mall", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    join_filtered<-DownloadOSM()
    CreateImportTable(dataset = join_filtered, schema = "raw_data", table_name = "osm_mall")
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}

