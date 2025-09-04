## ---------------------------
##
## Script name: Import town halls from NGI
##
## Purpose of script: Load NGI town halls data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2023-11-28
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

li_town_hall <- "968bb9ac-51f0-42e2-ac13-cb6938eb5260"
li_municipal_building <- "611c6125-3a7e-4fe8-9223-a677c522e5e5"

data_list_id<-"c4dcb93c-8a52-4239-8763-97194ed3a793"

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

# Set location for temporary files
temporary_folder <-Sys.getenv("TEMPORARY_STORAGE")

# Set log folder
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")




NGI_url <- "https://ac.ngi.be/remoteclient-open/ngi-standard-open/Vectordata/POI/POI-ElementsOfGeneralInterest/6c697427-8b2e-4bc6-8737-50e7cf12f5b4_geopackage+sqlite3_4326.zip"

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(file.path(rscript_folder,"utils_updated_check_protoanchors.R"))
source(file.path(rscript_folder,"utils.R"))




# EXTRACT ----
# """""""""""""""""" ----

PrepareNewData <- function(){

# Download the data ----


zip_path <- file.path(temporary_folder, "ngi_poi.zip")

# Download the zip file
GET(NGI_url, write_disk(zip_path, overwrite = TRUE))

# Unzip it
unzip(zip_path, exdir = temporary_folder)

# Import NGI data ----

#yes, there is a typo in the name of the layer
ngi_muni <- st_read(file.path(temporary_folder, "poi_elementsofgeneralinterest_4326.gpkg"), layer = "municpalitybuilding")

# force geom to 2D
ngi_muni <- st_zm(ngi_muni, drop = TRUE)

# remove objects that are in ruins, destroyed
ngi_muni<-ngi_muni %>%
  filter(!(status %in% c(3, 4)))


# if it is subtype=5214 & status=2, it is a real town hall
ngi_muni<-ngi_muni %>%
  mutate(real_town_hall=ifelse(subtype == 5214 & status == 2,1,0))



# remove spatial duplicates ----

# prepare original
ngi_muni$geom <- st_set_crs(ngi_muni$geom, 4326)
ngi_muni$geometry_l <- st_transform(ngi_muni$geom, crs = st_crs(31370))
ngi_muni <- st_set_geometry(ngi_muni, "geometry_l")

# prepare copy
ngi_copy<-ngi_muni %>%
  select(tgid_copy=tgid,real_town_hall_copy=real_town_hall)


# do a join of nearby features
ngi_muni <- st_join(ngi_muni, ngi_copy, join = st_is_within_distance, dist = 20)

# left join ngi_copy to ngi_muni
ngi_copy <- as.data.frame(ngi_copy)
names(ngi_copy)[which(colnames(ngi_copy) == "geometry_l")] <- "geometry_copy"
ngi_copy<-ngi_copy %>%
  select(-real_town_hall_copy)
ngi_muni <- left_join(ngi_muni, ngi_copy, by = "tgid_copy")


# distance calculator (Hausdorff distance, or the longest shortest path between any two points of the two geometries)
calculate_distance <- function(geometry1, geometry2) {
  # Calculate Hausdorff distance for any geometry types
  distance <- st_distance(geometry1, geometry2, which = "Hausdorff")
  return(distance)
}

# Calculate distance between potential matches
ngi_muni$distance <- mapply(calculate_distance, ngi_muni$geometry_l, ngi_muni$geometry_copy)



# filter duplicates where a real town hall is involved; keep only the real one
ngi_muni <- ngi_muni %>%
  group_by(tgid) %>%
  mutate(max_real = max(real_town_hall_copy)) %>%  
  ungroup()
ngi_muni <- ngi_muni %>%
  filter(!(real_town_hall==0 & max_real==1) & !(real_town_hall==1 & real_town_hall_copy==0))


# identify duplicates
ngi_muni <- ngi_muni %>%
  group_by(tgid) %>%
  mutate(count_copy = n()) %>%
  mutate(max_real = max(real_town_hall_copy)) %>%  
  ungroup()




# group by the "lowest ID in a group". This will merge three of four points rows that relate to a duplicates pair.
ngi_muni$tgid_low <- apply(ngi_muni[, c("tgid", "tgid_copy")], 1, function(x) min(unlist(x), na.rm = TRUE))
ngi_muni <- ngi_muni %>%
  group_by(tgid_low) %>%
  mutate(count_group = n()) %>%
  ungroup()

# keep only the "unique points", which are the ones that are not part of a duplicate pair or are one of the four rows where three were linked to  each other
ngi_townhalls <- ngi_muni %>%
  filter(count_group == 1)




con_pg <- get_con()
ngi_muni <- dbGetQuery(con_pg, "SELECT niscode, CASE WHEN languagestatute=1 THEN 'dut'
WHEN languagestatute=5 THEN 'dut'
WHEN languagestatute=4 THEN 'brussels'	
WHEN languagestatute=8 THEN 'ger'	
WHEN languagestatute=7 THEN 'fre'		
WHEN languagestatute=6 THEN 'fre'
WHEN languagestatute=2 THEN 'fre'
ELSE 'und' END as language, nameger, namefre, namedut, ST_AsText(shape) as geometry FROM raw_data.ngi_ign_municipality")
dbDisconnect(con_pg)
ngi_muni<-st_as_sf(ngi_muni, wkt="geometry")
ngi_muni$geometry <- st_set_crs(ngi_muni$geometry, 4326)


# spatial join to main data
ngi_townhalls <- st_join(ngi_townhalls, ngi_muni, join = st_within)

# count occurrence
ngi_townhalls <- ngi_townhalls %>%
  group_by(niscode,real_town_hall) %>%
  mutate(count_ordering = n()) %>%
  mutate(ordering_number = row_number(desc(real_town_hall))) %>%
  ungroup()



# simplify dataset
ngi_townhalls <- st_set_geometry(ngi_townhalls, "geom")
ngi_townhalls <<-ngi_townhalls %>%
  select(tgid, real_town_hall, language, nameger, namefre, namedut, count_ordering, ordering_number)
}



# TRANSFORM ----
# """""""""""""""""" ----


# Create ingestion table ----
ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.town_halls CASCADE;",
"CREATE TABLE IF NOT EXISTS ingestion.town_halls
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
  CONSTRAINT town_halls_pkey PRIMARY KEY (id)
);
",paste0("WITH cleaned AS (
  SELECT tgid,
  real_town_hall,
  CASE WHEN real_town_hall=1 THEN	
  jsonb_build_object(
    'dut', 'gemeentehuis',
    'fre', 'maison communal',
    'ger', 'Rathaus',
    'eng', 'town hall')
  ELSE 
  jsonb_build_object(
    'dut', 'gemeentelijk gebouw',
    'fre', 'bâtiment municipal',
    'ger', 'städtisches Gebäude',
    'eng', 'municipal building')
  END as legend_item,
  CASE 
    WHEN real_town_hall=1 THEN '",li_town_hall,"'::uuid
    ELSE '",li_municipal_building,"'::uuid END 
    AS legend_item_id,
  CASE WHEN count_ordering>1 THEN nameger || ' (' || ordering_number || '/' || count_ordering || ')' ELSE nameger END as nameger,
  CASE WHEN count_ordering>1 THEN namedut || ' (' || ordering_number || '/' || count_ordering || ')' ELSE namedut END as namedut,
  CASE WHEN count_ordering>1 THEN namefre || ' (' || ordering_number || '/' || count_ordering || ')' ELSE namefre END as namefre,
  geom as geometry
  FROM raw_data.ngi_ign_municipality_building)

INSERT INTO ingestion.town_halls 
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, geometry, created_at)
SELECT
tgid as original_id,
CASE WHEN real_town_hall=1 THEN 
JSONB_BUILD_OBJECT(
  'fre','Maison communal de ' || namefre,
  'ger','Rathaus ' || nameger,
  'dut','Gemeentehuis ' || namedut) ELSE
jsonb_build_object(
  'dut', 'Gemeentelijk gebouw '|| namedut,
  'fre', 'Bâtiment communal ' || namefre,
  'ger', 'Städtisches Gebäude ' || nameger) END
AS name,
legend_item,
legend_item_id,
'",data_list_id,"'::uuid as data_list_id,
1 as risk_level,
geometry,
CURRENT_DATE as created_at
FROM cleaned;"),
"ALTER TABLE IF EXISTS ingestion.town_halls OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.town_halls TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.town_halls TO pgn_user_airflow;")




#LOAD ----
# """""""""""""""""" ----

### Execute the SQL commands ----

create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}


# Prepare update -----

run_smart_update = function() {
  smart_update_process("town_halls", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}

# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    PrepareNewData()
    CreateImportTable(dataset = ngi_townhalls, schema = "raw_data", table_name = "ngi_ign_municipality_building")
    create_ingestion_table()
  }
  #create_transformation_table()
  run_smart_update()
}


if(run_status){
  main_function()
}


