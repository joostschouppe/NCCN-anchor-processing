## ---------------------------
##
## Script name: Import power lines from OSM and pylons from NGI
##
## Purpose of script: Load OSM power lines data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2025-10-07
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

data_list_id_osm <- "ffb17bb2-1de1-4381-b6ea-209285f35e38"
data_list_id_ngi <- "687744db-449b-4d54-968c-8d1384aeb9f9"
li_power_line <- "48ad9691-19bd-4cc3-b370-5b76989a1a52"
li_power_cable <- "da9e81ed-73b9-42e3-9494-ac8fcbb4fea4"
li_pylon <- "dcab47fb-28aa-4d6a-a890-b44f0182d238"
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

# Set location for temporary files
temporary_folder <-Sys.getenv("TEMPORARY_STORAGE")


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
features_list <- list(or_group(
  tag_filter("power", "line"),
  tag_filter("power", "cable")
  ))

# Define extra tags to use as columns for properties
extra_columns <- c("power", "cables", "disused", "status", "line", "location", "operational_status", "ref", "ref:FR:RTE", "ref:circuits", "overhead:clearance", "voltage", "disused:voltage")

# Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
datatypes <- c("lines")

### Download OSM high voltage lines & cables ----
osm_highvoltage <- run_process(
  download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE),
  paste0("OSM download & processing for ", describe_features_list(features_list))
  
)

# set max voltage
osm_highvoltage <- osm_highvoltage %>%
  mutate(voltage_max = sapply(strsplit(as.character(voltage), ";"), function(x) {
    max(as.numeric(trimws(x)), na.rm = TRUE)
  }))


# if voltage_max=0 and disused_voltage>0, then set voltage_max to disused_voltage
osm_highvoltage <- osm_highvoltage %>%
  mutate(voltage_max=ifelse(voltage_max==0 & !is.na(disused_voltage),
              as.numeric(trimws(disused_voltage)),voltage_max))

# only keep ones with a high voltage
## underground cables at 36kV tend to be poorly mapped. Review this filter if the situation improves. An issue in particular in West-Vlaanderen
osm_highvoltage <- osm_highvoltage %>%
  filter(
    (voltage_max>=50000 & power=="cable") |
    (voltage_max>=36000 & power=="line"))


# if operational_status=out_of_order set disused=yes
osm_highvoltage <- osm_highvoltage %>%
  mutate(disused = case_when(
    operational_status == "out_of_order" ~ "yes",
    status == "inactive" ~ "yes",
    TRUE ~ disused
  ))


# only keep underground cables & overground lines
osm_highvoltage <- osm_highvoltage %>%
  filter(((location=="underground" | location=="underwater") & power=="cable") | ((power=="line") & (is.na(location) | location!="underground")))

# remove lines within transformation sites
print("Below the values of the 'line' column. The vast majority should be NA. busbar, bay, substation  and transformer are for lines within transfo stations. If a new value should appear to indicate 'normal' lines, then the current filter should be replaced.")
print(table(osm_highvoltage$line, useNA='always'))
osm_highvoltage <- osm_highvoltage %>%
  filter(is.na(line))

# Download NGI data ----
ngi_url <- "https://ac.ngi.be/remoteclient-open/ngi-standard-open/Vectordata/Top10Vector/Top10Vector-All/e0fdc885-8851-482e-80c9-6a0ba3709761_geopackage+sqlite3_3812.zip"

# clear and create a local folder to work in
zip_folder <- file.path(temporary_folder, "highvoltage")

# If the folder exists, delete it and everything in it
if (dir.exists(zip_folder)) {
  unlink(zip_folder, recursive = TRUE, force = TRUE)
}
# Now (re)create the folder
dir.create(zip_folder, recursive = TRUE)

zip_path <- file.path(zip_folder, "top10vector.zip")

# Download the zip file
GET(ngi_url, write_disk(zip_path, overwrite = TRUE))

# Get the file name of the gpkg that was downloaded
zip_contents <- unzip(zip_path, list = TRUE)

# Expect exactly one file in the archive
n_files <- nrow(zip_contents)
if (n_files != 1) {
  stop("NGI Zip contains ", n_files, " files, expected exactly 1.")
}
# save the filename of the expected file
file_name <- zip_contents$Name[[1]]

# Unzip it
unzip(zip_path, exdir = zip_folder)


# Import NGI data ----
ngi_pylon <- st_read(file.path(zip_folder,file_name), layer = "HT_powerpylon")

ngi_pylon_backup <- ngi_pylon

# remove local data
unlink(zip_folder, recursive = TRUE, force = TRUE)


# force geom to 2D
ngi_pylon <- st_zm(ngi_pylon, drop = TRUE)

# reproject to wgs84
ngi_pylon <- st_transform(ngi_pylon, crs = st_crs(4326))



# Upload to raw data ----
CreateImportTable(dataset = osm_highvoltage, schema = "raw_data", table_name = "osm_high_voltage")
CreateImportTable(dataset = ngi_pylon, schema = "raw_data", table_name = "ngi_pylon")


  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function



# TRANSFORM ----
# """""""""""""""""" ----

# LOAD ----
# """""""""""""""""" ----

### Create SQL for proper ingestion table ----

ingestion_table_sql_osm <- c("
DROP TABLE IF EXISTS ingestion.power_line CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.power_line
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
    CONSTRAINT power_line_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
'https://osm.org/' || osm_id AS original_id,
jsonb_strip_nulls(jsonb_build_object(
      'und', CASE WHEN name IS NULL THEN 'power line ' || voltage_max/1000 || 'kV' ELSE name END,
    	'fre', name_fr,
      'ger', name_de::text,
      'dut', name_nl)) as name,
CASE WHEN power='line' THEN '",li_power_line,"'::uuid
  ELSE '",li_power_cable,"'::uuid END as legend_item_id,
1 as risk_level,
NULLIF(CONCAT_WS('; ',name, short_name, official_name, alt_name, old_name), '') AS other_names,
NULLIF(CONCAT_WS('; ',contact_email, email), '') AS local_email,
operator_email,
NULLIF(CONCAT_WS('; ',contact_mobile, mobile, contact_phone, phone, phone_2),'') AS local_phone,
NULLIF(CONCAT_WS('; ',website, contact_website),'') AS local_website,
operator_website,
operator_wikidata, operator, operator_type, image,
wikidata,
cables, disused, line, location, ref, ref_fr_rte, ref_circuits, overhead_clearance,
voltage_max,
geometry
FROM raw_data.osm_high_voltage)

INSERT INTO ingestion.power_line
(original_id, name, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
legend_item_id,
'",data_list_id_osm,"'::uuid as data_list_id,
risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
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
  'nr_of_cables', cables,
  'disused', disused, 
  'high_voltage_line_type', line, 
  'location', location, 
  'ref', ref,
  'ref_fr_rte', ref_fr_rte,
  'ref_circuits', ref_circuits, 
  'overhead_clearance', overhead_clearance,
  'voltage_max', voltage_max/1000 || 'kV'
)) as properties,
ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.power_line OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.power_line TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.power_line TO pgn_user_airflow;"
)




ingestion_table_sql_ngi <- c("
DROP TABLE IF EXISTS ingestion.power_line_pole CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.power_line_pole
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
    CONSTRAINT power_line_pole_pkey PRIMARY KEY (id)
  );
",paste0("
WITH cleaned as (SELECT
tgid AS original_id,
jsonb_strip_nulls(jsonb_build_object(
      'und', CASE WHEN pylonnr='888' THEN 'high-voltage pylon' ELSE pylonnr END
    	)) as name,
1 as risk_level,
case when height>0 then height else NULL END as height,
modifdate,
shape as geometry
FROM raw_data.ngi_pylon)

INSERT INTO ingestion.power_line_pole 
(original_id, name, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
name,
'",li_pylon,"'::uuid as legend_item_id,
'",data_list_id_ngi,"'::uuid as data_list_id,
risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
	'height', height,
	'modification_date', modifdate
)) as properties,
ST_ReducePrecision(geometry, 1e-8) as geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.power_line_pole OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.power_line_pole TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.power_line_pole TO pgn_user_airflow;"
)



### Execute the SQL commands ----

create_ingestion_table_osm <- function() {execute_sql_commands(ingestion_table_sql_osm, "Ingestion table OSM")}
create_ingestion_table_ngi <- function() {execute_sql_commands(ingestion_table_sql_ngi, "Ingestion table NGI")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
#overrule_checks<-TRUE


run_smart_update = function() {
  smart_update_process("power_line_pole", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
  smart_update_process("power_line", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data, same_id_size_factor = 1.25,different_id_size_factor = 1.25)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_fresh_data()
    create_ingestion_table_osm()
    create_ingestion_table_ngi()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}