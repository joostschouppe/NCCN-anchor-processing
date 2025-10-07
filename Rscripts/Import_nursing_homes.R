## ---------------------------
##
## Script name: Import nursery
##
## Purpose of script: Load nursing home data & transform into proto-anchors for Paragon
##
## Author: Joost Schouppe
##
## Date Created: 2024-08-01
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# Set external IDs
data_list_id_cob <- "7db4a005-0186-4e72-8a7a-e82c2030508c"
data_list_id_osm <- "99e8beb0-fd2e-4799-b78f-e59a7340b1c9"
li_elderly_day_care <- "31f9c3fc-8a2f-4548-8f94-9ab9be35d0b3"
li_wzc_combo <- "20eadaa3-ed0e-4ccc-8776-0b91bc8532ab"
li_wzc <- "e24e5cbe-077f-44fe-81f7-5cbbf8dfe139"
li_assisted_lving <- "e4f68d50-f9e5-4a2f-8ec4-24ba8e2a4ca0"

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



# Set log folder
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------
rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))


# Libraries -------------------------------
# """""""""""""""""" ----------------------

# all are loaded via the utils scripts


# EXTRACT ----
# """""""""""""""""" ----

# OSM data for German speaking region & outside Belgium


process_osm_data <- function(){
  
  # Define the list of features
  features_list <- list("social_facility:for"="senior", 
                        "social_facility"="nursing_home",
                        "social_facility"="assisted_living",
                        "social_facility"="group_home")
  
  # Define extra tags to use as columns for properties
  extra_columns <- c("amenity", "capacity", "social_facility", "social_facility:for", "description")
  # Choose which datatypes are needed, as a list of datatypes, using any of "points", "lines", "mpolygons" (this is polygons+multipolygons together)
  datatypes <- c("points", "mpolygon")
  
  
  ### Actual OSM download & transformation ----
  osm_all <- run_process(
    download_osm_process(features_list, datatypes, extra_columns, keep_region=TRUE, postgres=TRUE),
    paste0("OSM download & processing for ", paste(paste(names(features_list), unlist(features_list), sep = "="), collapse = ", "), collapse = ", ")
  )
  
  # social_facility=nursing_home and social_facility_for should be "senior" or missing
  # or another social_facility value, but then social_facility_for must have the value "senior"
  # if in Belgium, it should be in the German speaking area
  
  # merge to outer and keep relevant info
  # merge nearby features if same name?
  # the resulting object should have a name
  
  osm_filtered <- osm_all %>%
    filter(
        (
          # Case 1: nursing home with senior or missing "for"
          (social_facility == "nursing_home" &
             (is.na(social_facility_for) | grepl("senior", social_facility_for, ignore.case = TRUE))) |
            
            # Case 2: other facility types must have "senior"
            (social_facility != "nursing_home" &
               grepl("senior", social_facility_for, ignore.case = TRUE))
        ) 
        #& (language == "ger" | is.na(language)) #commented out for now to be able to asses OSM BE quality
    )
  
  
  
  # detect objects on top of other objects in the dataset
  ## first define a set of potential outers
  osm_outers<-osm_filtered %>%
    select(outer_id=osm_id) %>%
    filter(!st_is(geometry, "POINT"))
  ## then join them to the objects
  join <- st_join(osm_filtered, osm_outers, join = st_within)
  
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
    mutate(operator= ifelse(count > 1, na_if(paste(unique(na.omit(operator)), collapse = "; "),""), operator)) %>%
    mutate(addr_city= ifelse(count > 1, na_if(paste(unique(na.omit(addr_city)), collapse = "; "),""), addr_city)) %>%
    mutate(addr_housenumber= ifelse(count > 1, na_if(paste(unique(na.omit(addr_housenumber)), collapse = "; "),""), addr_housenumber)) %>%
    mutate(addr_street= ifelse(count > 1, na_if(paste(unique(na.omit(addr_street)), collapse = "; "),""), addr_street)) %>%
    mutate(addr_postcode= ifelse(count > 1, na_if(paste(unique(na.omit(addr_postcode)), collapse = "; "),""), addr_postcode)) %>%
    mutate(contact_email= ifelse(count > 1, na_if(paste(unique(na.omit(contact_email)), collapse = "; "),""), contact_email)) %>%
    mutate(email= ifelse(count > 1, na_if(paste(unique(na.omit(email)), collapse = "; "),""), email)) %>%
    mutate(website= ifelse(count > 1, na_if(paste(unique(na.omit(website)), collapse = "; "),""), website)) %>%
    mutate(contact_website= ifelse(count > 1, na_if(paste(unique(na.omit(contact_website)), collapse = "; "),""), contact_website)) %>%
    mutate(phone= ifelse(count > 1, na_if(paste(unique(na.omit(phone)), collapse = "; "),""), phone)) %>%
    mutate(contact_phone= ifelse(count > 1, na_if(paste(unique(na.omit(contact_phone)), collapse = "; "),""), contact_phone)) %>%
    mutate(opening_hours= ifelse(count > 1, na_if(paste(unique(na.omit(opening_hours)), collapse = "; "),""), opening_hours)) %>%
    mutate(mobile= ifelse(count > 1, na_if(paste(unique(na.omit(mobile)), collapse = "; "),""), mobile)) %>%
    mutate(contact_mobile= ifelse(count > 1, na_if(paste(unique(na.omit(contact_mobile)), collapse = "; "),""), contact_mobile)) %>%
    mutate(check_date= ifelse(count > 1, na_if(paste(unique(na.omit(check_date)), collapse = "; "),""), check_date)) %>%
    mutate(wikidata= ifelse(count > 1, na_if(paste(unique(na.omit(wikidata)), collapse = "; "),""), wikidata)) %>%
    mutate(operator_website= ifelse(count > 1, na_if(paste(unique(na.omit(operator_website)), collapse = "; "),""), operator_website)) %>%
    mutate(capacity= ifelse(count > 1, na_if(paste(unique(na.omit(capacity)), collapse = "; "),""), capacity)) %>%
    mutate(description= ifelse(count > 1, na_if(paste(unique(na.omit(description)), collapse = "; "),""), description)) %>%
    mutate(amenity=ifelse(count > 1, na_if(paste(unique(na.omit(amenity)), collapse = "; "),""), amenity)) %>%
    mutate(social_facility=ifelse(count > 1, na_if(paste(unique(na.omit(social_facility)), collapse = "; "),""), social_facility)) %>%
    mutate(social_facility_for=ifelse(count > 1, na_if(paste(unique(na.omit(social_facility_for)), collapse = "; "),""), social_facility_for))
  

  # aggregate names
  #if osm_id=outer_id and name is missing, fill it in with "sports"
  #join<-join %>%  mutate(name=ifelse(osm_id==outer_id & is.na(name) & count>1, "sports", name))
  
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
  
  
  # Define list of columns for name aggregation
  names_to_aggregate <- c("name", "name_nl", "name_fr", "name_de", "alt_name", "short_name", "official_name", "old_name")
  
  # Call the function to aggregate names
  join <- aggregate_names(join, names_to_aggregate)
  
  
  
  # Filter records based on the count and outer_id conditions
  join_filtered <- join %>%
    filter(count == 1 | (count > 1 & outer_id == osm_id))
  #table(join_filtered$social_facility)
  
  #dummies for assisted living, nursing home, day care > remove if none of those
  join_filtered <- join_filtered %>%
    mutate(day_care_dummy = case_when(
      grepl("day_care", social_facility, ignore.case = TRUE) ~ 1,
      TRUE ~ 0
    )) %>%
    mutate(nursing_home_dummy = case_when(
      grepl("group_home", social_facility, ignore.case = TRUE) ~ 1,
      grepl("nursing_home", social_facility, ignore.case = TRUE) ~ 1,
      TRUE ~ 0 )) %>%
    mutate(assisted_living_dummy =case_when(
      grepl("assisted_living", social_facility, ignore.case = TRUE) ~ 1,
      TRUE ~ 0
    ))
  
  join_filtered <- join_filtered %>%
    filter(day_care_dummy==1 | nursing_home_dummy==1 | assisted_living_dummy==1)
  
  # remove line geometries (if any)
  join_filtered <- join_filtered %>%
    filter(!st_geometry_type(geometry) %in% c("LINESTRING", "MULTILINESTRING"))
  
  # find the records without a name for manual review/maproulette task
  #join_noname <- join_filtered %>%
  # filter(is.na(name) & !is.na(language)) %>%
  #  select(osm_id)
  #st_write(join_noname, "C:/temp/elderly_care_noname.geojson", delete_dsn = TRUE)
  
  
  # remove results without a name
  join_filtered <- join_filtered %>%
    filter(!is.na(name))
  
  # add legend item
  join_filtered <- join_filtered %>%
    mutate(legend_item_id = case_when(
      day_care_dummy + nursing_home_dummy + assisted_living_dummy > 1 ~ li_wzc_combo,
      day_care_dummy == 1 ~ li_elderly_day_care,
      nursing_home_dummy == 1 ~ li_wzc,
      assisted_living_dummy == 1 ~ li_assisted_lving))
  
  # add risk level
  join_filtered <- join_filtered %>%
    mutate(risk_level = case_when(
      day_care_dummy + nursing_home_dummy + assisted_living_dummy > 1 ~ 2,
      day_care_dummy == 1 ~ 1,
      nursing_home_dummy == 1 ~ 2,
      assisted_living_dummy == 1 ~ 1))
  
  
  

  join_filtered <<- join_filtered %>%
    mutate(data_list_id=data_list_id_osm)
  
  #st_write(join_filtered, "C:/temp/elderly_care.geojson", delete_dsn = TRUE)
  
  CreateImportTable(dataset = join_filtered, schema = "raw_data", table_name = "osm_elderly_care")
  
  
} # end process_osm_data function


# download official data from cobrha ----

process_and_merge_cobrha_data <- function(){
con_pg <- get_con()

nursing_cobrha <- dbGetQuery(con_pg, paste0(
  "with nulls_cleaned AS (
  SELECT 
  NULLIF(hco_id, '') AS hco_id,
  NULLIF(cbe_id::text, '') AS cbe_id,
  NULLIF(hco_type_des, '') AS hco_type_des,
  NULLIF(hco_type_code, '') AS hco_type_code,
  NULLIF(as_code, '') AS as_code,
  NULLIF(hco_approval_status, '') AS hco_approval_status,
  NULLIF(hco_approval_id, '') AS hco_approval_id,
  NULLIF(hco_name_nl, '') AS hco_name_nl,
  NULLIF(hco_name_fr, '') AS hco_name_fr,
  NULLIF(hco_name_de::text, '') AS hco_name_de,
  NULLIF(nihii_id::text, '') AS nihii_id,
  NULLIF(nihii_qual_code::text, '') AS nihii_qual_code,
  NULLIF(nihii_sit_code, '') AS nihii_sit_code,
  NULLIF(hco_street, '') AS hco_street,
  NULLIF(hco_house_number, '') AS hco_house_number,
  NULLIF(hco_zip_code::text, '') AS hco_zip_code,
  NULLIF(hco_municipality, '') AS hco_municipality,
  NULLIF(site_id::text, '') AS site_id,
  NULLIF(site_name_nl, '') AS site_name_nl,
  NULLIF(site_name_fr, '') AS site_name_fr,
  NULLIF(site_name_de::text, '') AS site_name_de,	
  NULLIF(site_approval_id, '') AS site_approval_id,
  NULLIF(site_approval_status, '') AS site_approval_status,
  ad_hoc_id,
  NULLIF(substring(hco_contact from 'Fax:([^|]+)'), '') AS operator_fax,
  NULLIF(substring(site_contact from 'Fax:([^|]+)'), '') AS local_fax,
  NULLIF(substring(hco_contact from 'Mail:([^|]+)'), '') AS operator_email,
  NULLIF(substring(site_contact from 'Mail:([^|]+)'), '') AS local_email,
  NULLIF(substring(hco_contact from 'Phone:([^|]+)'), '') AS operator_phone,
  NULLIF(substring(site_contact from 'Phone:([^|]+)'), '') AS local_phone,
  NULLIF(substring(hco_contact from '(?i)Url:([^|]+)'), '') AS operator_website,
  NULLIF(substring(site_contact from '(?i)Url:([^|]+)'), '') AS local_website,
  NULLIF(municipality, '') AS municipality,
  NULLIF(zip_code::text, '') AS zip_code,
  NULLIF(street, '') AS street,
  NULLIF(house_number, '') AS house_number,
  geometry,
  ogc_fid,
  CASE 
  WHEN hco_name_nl = site_name_nl THEN hco_name_nl
  WHEN hco_name_nl != site_name_nl AND 
  (SELECT COUNT(*) FROM raw_data.ehealth_cobrha_geocoded AS sub WHERE sub.hco_name_nl = raw_data.ehealth_cobrha_geocoded.hco_name_nl) > 1
  THEN hco_name_nl || ' (' || site_name_nl || ')'
  ELSE hco_name_nl
  END AS name_nl,
  CASE 
  WHEN hco_name_fr = site_name_fr THEN hco_name_fr
  WHEN hco_name_fr != site_name_fr AND 
  (SELECT COUNT(*) FROM raw_data.ehealth_cobrha_geocoded AS sub WHERE sub.hco_name_fr = raw_data.ehealth_cobrha_geocoded.hco_name_fr) > 1
  THEN hco_name_fr || ' (' || site_name_fr || ')'
  ELSE hco_name_fr
  END AS name_fr,
  CASE 
  WHEN hco_name_de::text = site_name_de THEN hco_name_de::text
  WHEN hco_name_de::text != site_name_de AND 
  (SELECT COUNT(*) FROM raw_data.ehealth_cobrha_geocoded AS sub WHERE sub.hco_name_de = raw_data.ehealth_cobrha_geocoded.hco_name_de) > 1
  THEN hco_name_de::text || ' (' || site_name_de::text || ')'
  ELSE hco_name_de::text
  END AS name_de
  FROM raw_data.ehealth_cobrha_geocoded
  WHERE (hco_approval_status != 'Ended' OR hco_approval_status IS NULL) AND (site_approval_status != 'Ended' OR site_approval_status IS NULL)),

filtered AS (select cbe_id,hco_id,street,as_code,name_de,name_fr,name_nl,ogc_fid,site_id,geometry,nihii_id,zip_code,ad_hoc_id,hco_street,hco_name_de,hco_name_fr,hco_name_nl,hco_type_des,hco_zip_code,house_number,municipality,site_name_de,site_name_fr,site_name_nl,hco_type_code,nihii_sit_code,hco_approval_id,nihii_qual_code,hco_house_number,hco_municipality,site_approval_id,hco_approval_status,site_approval_status,
             	CASE WHEN operator_fax=local_fax OR operator_fax='- ' THEN NULL ELSE operator_fax END AS operator_fax,
	            CASE WHEN local_fax='- ' THEN NULL ELSE local_fax END AS local_fax,
	            CASE WHEN operator_email=local_email OR operator_email='- ' THEN NULL ELSE operator_email END AS operator_email,
	            CASE WHEN local_email='- ' THEN NULL ELSE local_email END AS local_email,
	            CASE WHEN operator_phone=local_phone OR operator_phone='- ' THEN NULL ELSE operator_phone END AS operator_phone,
	            CASE WHEN local_phone='- ' THEN NULL ELSE local_phone END AS local_phone,
	            CASE WHEN operator_website=local_website OR operator_website='- ' THEN NULL ELSE operator_website END AS operator_website,
	            CASE WHEN local_website='- ' THEN NULL ELSE local_website END AS local_website,
             CONCAT(hco_id,'_',cbe_id,'_',as_code,'_',hco_approval_id,'_',nihii_id,'_',site_id) as original_id,
             CASE 
             WHEN hco_type_code in ('034', '751', 'AWH_MRPA', '740', '730') THEN 1
             WHEN hco_type_code in ('038', 'CSJ_TP', '757', '756') THEN 2
             WHEN hco_type_code in ('035') THEN 3
             WHEN hco_type_code in ('968') THEN 4
             ELSE 999 END AS activity_type,
			 case when hco_type_code in ('034', '751', 'AWH_MRPA', '740', '730') then 2
				when hco_type_code in ('038', 'CSJ_TP', '757', '756') then 1
				when hco_type_code in ('035') then 3
				when hco_type_code in ('968') then 1 END
			 as risk_level
             from nulls_cleaned
             where hco_type_code in ('034', '038', '035', '751', 'AWH_MRPA', '740', '730', 'CSJ_TP', '757', '756', '968'))


select original_id, activity_type::text, risk_level,
street, house_number,
LTRIM(CONCAT(replace(street,',',''),' ' || house_number, ', ' || zip_code, ' ' || municipality)) as address,
LTRIM(CONCAT(replace(hco_street,',',''),' ' || hco_house_number, ', ' || hco_zip_code, ' ' || hco_municipality)) as hco_address, 
cbe_id as kbo_bce,
hco_id,
hco_type_des as original_legend_item,
hco_type_code,
as_code,
hco_approval_status,
hco_approval_id,
hco_name_fr,
hco_name_nl,
hco_name_de,
site_name_fr,
site_name_de,
site_name_nl,
nihii_id,
nihii_qual_code,
nihii_sit_code,
site_id,
site_approval_id,
operator_fax::text,
operator_email::text,
operator_phone::text,
operator_website::text,
local_fax::text,
local_email::text,
local_phone::text,
local_website::text,
name_nl,name_fr,name_de,
CASE WHEN activity_type='1' THEN '",li_wzc,"'::uuid
WHEN activity_type='2' THEN '",li_elderly_day_care,"'::uuid
ELSE '",li_wzc_combo,"'::uuid END as legend_item_id,
ST_AsText(geometry) as geometry
from filtered"))

dbDisconnect(con_pg)


# make sf
nursing_cobrha<-nursing_cobrha %>% filter( !is.na(geometry) & geometry != "")
nursing_cobrha<-st_as_sf(nursing_cobrha, wkt="geometry")
nursing_cobrha$geometry <- st_set_crs(nursing_cobrha$geometry, 31370)
#nursing_cobrha <- st_transform(nursing_cobrha, 31370)


# make OSM data 31370
join_filtered<-st_transform(join_filtered, 31370)

# split OSM data into groups ----
## split off everything outside BE except Ostbelgien (ger language) data
osm_be <- join_filtered %>%
  filter(!is.na(language) & language != 'ger')

osm_rest <- join_filtered %>%
  filter(is.na(language) | language == 'ger')

## remove point features in rest of BE
osm_be <- osm_be %>%
  filter(st_geometry_type(geometry)!="POINT")

# merge cobrha data to OSM data in rest of BE ----

# create a name field specific for the comparison
nursing_cobrha <- nursing_cobrha %>%
  mutate(name_comp = case_when(
    is.na(name_nl) ~ name_fr,
    is.na(name_fr) ~ name_nl,
    !is.na(name_fr) & !is.na(name_nl) & name_nl != name_fr ~ 
      paste0(name_nl, " / ", name_fr),
    .default = name_fr
  ))

# Set parameters
distance_matched_threshold <- 50
distance_raw_threshold <- 500
## insignificant words in names that shouldn't be included in name comparison (don't use accents etc.)
patterns_to_remove <- c("wzc", "woonzorgcentrum", "assistentiewoningen", "woon- en zorgcentrum", "residentie", "residence", "maison de repos et de soins", "home", "centre de soins", "dagverzorgingscentrum")


nursing_cobrha_enriched <- merge_to_external_polygons(
  attribute_features = nursing_cobrha,
  geom_features = osm_be,
  attr_id_col = "original_id",
  geom_id_col = "osm_id",
  attr_name_col = "name_comp",
  geom_name_col = "name",
  attr_street_col = "street",
  attr_hnr_col = "house_number",
  geom_street_col = "addr_street",
  geom_hnr_col = "addr_housenumber",
  distance_matched_threshold = 50,
  distance_raw_threshold = 500,
  patterns_to_remove = "patterns_to_remove"
)

# aggregation ID
nursing_cobrha_enriched <- nursing_cobrha_enriched %>%
  mutate(agg_id = ifelse(!is.na(osm_id),osm_id,st_as_text(geometry)))

# back to 4326, explicit geometry name
nursing_cobrha_enriched<-nursing_cobrha_enriched %>% rename(off_geometry=geometry)
nursing_cobrha_enriched <- st_as_sf(nursing_cobrha_enriched, wkt="off_geometry")
nursing_cobrha_enriched$off_geometry <- st_set_crs(nursing_cobrha_enriched$off_geometry, 31370)
nursing_cobrha_enriched <- st_transform(nursing_cobrha_enriched, 4326)




# create OSM attributes in original OSM dataset
## from osm_be, we use all the records that are there (just the polygons), but some will be removed and read with the cobrha data
## from osm_rest we use all the records
osm_properties <- rbind(osm_be,osm_rest)

#convert back to 4326
osm_properties <- st_transform(osm_properties, 4326)

osm_properties <- osm_properties %>%
  rowwise() %>%
  mutate(
    # Build other_names
    other_names = ifelse(
      all(is.na(c(short_name, official_name, alt_name, old_name))),
      NA_character_,
      paste(na.omit(c(short_name, official_name, alt_name, old_name)), collapse = "; ")
    ),
    
    # Build local_email
    local_email = ifelse(
      all(is.na(c(contact_email, email))),
      NA_character_,
      paste(na.omit(c(contact_email, email)), collapse = "; ")
    ),
    
    # Build local_phone
    local_phone = ifelse(
      all(is.na(c(contact_mobile, mobile, contact_phone, phone, phone_2))),
      NA_character_,
      paste(na.omit(c(contact_mobile, mobile, contact_phone, phone, phone_2)), collapse = "; ")
    ),
    
    # Build local_website
    local_website = ifelse(
      all(is.na(c(website, contact_website, alt_website))),
      NA_character_,
      paste(na.omit(c(website, contact_website, alt_website)), collapse = "; ")
    ),
    
    # Build address
    address = if_else(
      is.na(addr_street),
      NA_character_,
      trimws(paste0(
        addr_street, " ",
        if_else(
          !is.na(nohousenumber) & nohousenumber == "yes", 
          "w/n", 
          coalesce(addr_housenumber, "")
        ),
        ", ",
        paste0(coalesce(addr_postcode, ""), " "),
        coalesce(addr_city, "")
      ))
    )
  ) %>%
  mutate(properties = jsonlite::toJSON(
    purrr::discard(list(
      operator_wikidata = operator_wikidata,
      operator = operator,
      operator_type = operator_type,
      operator_email = operator_email,
      operator_website = operator_website,
      opening_hours = opening_hours,
      image = image,
      wikidata = wikidata,
      capacity = capacity,
      social_facility = social_facility,
      social_facility_for = social_facility_for,
      description = description,
      address = address,
      other_names = other_names,
      local_email = local_email,
      local_phone = local_phone,
      local_website = local_website
    ), is.na), auto_unbox = TRUE)) %>%
  mutate(name = jsonlite::toJSON(
    purrr::discard(list(
      und = name,
      fre = name_fr,
      dut = name_nl,
      ger = name_de
    ), is.na), auto_unbox = TRUE))



# give cobrha records OSM geometries where needed
nursing_cobrha_enriched_joined <- left_join(
  nursing_cobrha_enriched, 
  as.data.frame(osm_properties) %>% select(osm_id, osm_name=name, osm_legend_item_id=legend_item_id, osm_risk_level=risk_level, properties, geometry),
  by = "osm_id"
)

# simplify the osm_properties set
osm_properties <- osm_properties %>%
  mutate(original_id=paste0("https://osm.org/", osm_id)) %>%
  select(original_id, name, properties, data_list_id, legend_item_id, language)



# group cobrha records and create off_attributes there
## aggregate
nursing_cobrha_enriched_joined <- nursing_cobrha_enriched_joined %>%
  group_by(agg_id) %>%
  summarize(
    original_id = na_if(paste(unique(original_id), collapse = "; "),"NA"),
    name_nl = na_if(paste(unique(name_nl), collapse = "; "),"NA"),
    name_fr = na_if(paste(unique(name_fr), collapse = "; "),"NA"),
    name_de = na_if(paste(unique(name_de), collapse = "; "),"NA"),
    address = na_if(paste(unique(address), collapse = "; "),"NA"),
    hco_address = na_if(paste(unique(hco_address), collapse = "; "),"NA"),
    kbo_bce = na_if(paste(unique(kbo_bce), collapse = "; "),"NA"),
    hco_id = na_if(paste(unique(hco_id), collapse = "; "),"NA"),
    original_legend_item = na_if(paste(unique(original_legend_item), collapse = "; "),"NA"),
    hco_type_code = na_if(paste(unique(hco_type_code), collapse = "; "),"NA"),
    as_code = na_if(paste(unique(as_code), collapse = "; "),"NA"),
    hco_approval_status = na_if(paste(unique(hco_approval_status), collapse = "; "),"NA"),
    hco_approval_id = na_if(paste(unique(hco_approval_id), collapse = "; "),"NA"),
    hco_name_fr = na_if(paste(unique(hco_name_fr), collapse = "; "),"NA"),
    hco_name_de = na_if(paste(unique(hco_name_de), collapse = "; "),"NA"),
    hco_name_nl = na_if(paste(unique(hco_name_nl), collapse = "; "),"NA"),
    site_name_fr = na_if(paste(unique(site_name_fr), collapse = "; "),"NA"),
    site_name_de = na_if(paste(unique(site_name_de), collapse = "; "),"NA"),
    site_name_nl = na_if(paste(unique(site_name_nl), collapse = "; "),"NA"),
    nihii_id = na_if(paste(unique(nihii_id), collapse = "; "),"NA"),
    nihii_qual_code = na_if(paste(unique(nihii_qual_code), collapse = "; "),"NA"),
    nihii_sit_code = na_if(paste(unique(nihii_sit_code), collapse = "; "),"NA"),
    site_id = na_if(paste(unique(site_id), collapse = "; "),"NA"),
    site_approval_id = na_if(paste(unique(site_approval_id), collapse = "; "),"NA"),
    operator_fax = na_if(paste(unique(operator_fax), collapse = "; "),"NA"),
    operator_email = na_if(paste(unique(operator_email), collapse = "; "),"NA"),
    operator_phone = na_if(paste(unique(operator_phone), collapse = "; "),"NA"),
    operator_website = na_if(paste(unique(operator_website), collapse = "; "),"NA"),
    local_fax = na_if(paste(unique(local_fax), collapse = "; "),"NA"),
    local_email = na_if(paste(unique(local_email), collapse = "; "),"NA"),
    local_phone = na_if(paste(unique(local_phone), collapse = "; "),"NA"),
    local_website = na_if(paste(unique(local_website), collapse = "; "),"NA"),
    legend_item_id = na_if(paste(unique(legend_item_id), collapse = "; "),"NA"),
    risk_level = max(risk_level),
    osm_legend_item_id = na_if(paste(unique(osm_legend_item_id), collapse = "; "),"NA"),
    osm_id=first(osm_id),
    osm_name=first(osm_name),
    osm_properties=first(properties),
    osm_risk_level=max(osm_risk_level),
    off_geometry=first(off_geometry),
    geometry_osm=first(geometry))

# create official properties
nursing_cobrha_enriched_joined <- nursing_cobrha_enriched_joined %>%
  rowwise() %>%
  mutate(properties = jsonlite::toJSON(
    purrr::discard(list(
      address = address,
      hco_address = hco_address,
      kbo_bce = kbo_bce,
      hco_id = hco_id,
      hco_type_code = hco_type_code,
      hco_approval_id = hco_approval_id,
      hco_approval_status = hco_approval_status,
      hco_name_fr = hco_name_fr,
      hco_name_de = hco_name_de,
      hco_name_nl = hco_name_nl,
      site_name_fr = site_name_fr,
      site_name_de = site_name_de,
      site_name_nl = site_name_nl,
      nihii_id = nihii_id,
      nihii_qual_code = nihii_qual_code,
      nihii_sit_code = nihii_sit_code,
      site_id = site_id,
      operator_email = operator_email,
      operator_website = operator_website,
      operator_fax = operator_fax,
      operator_phone = operator_phone,
      local_email = local_email,
      local_website = local_website,
      local_fax = local_fax,
      local_phone = local_phone
    ), is.na), auto_unbox = TRUE))


# create official names as json
nursing_cobrha_enriched_joined <- nursing_cobrha_enriched_joined %>%
  rowwise() %>%
  mutate(name = jsonlite::toJSON(
    purrr::discard(list(
      dut = name_nl,
      fre = name_fr,
      ger = name_de
    ), is.na), auto_unbox = TRUE))


# set legend item
nursing_cobrha_enriched_joined <- nursing_cobrha_enriched_joined %>%
  mutate(legend_item_id = case_when(
    grepl(";",legend_item_id) ~ li_wzc_combo, ## if ; in legend_item_id, then it is always a combined place
    !is.na(osm_id) & osm_legend_item_id==legend_item_id ~ legend_item_id, ## if OSM & official identical: copy 
    !is.na(osm_id) & osm_legend_item_id!=legend_item_id ~ li_wzc_combo, ## if OSM & official different: li_wzc_combo
    is.na(osm_id) ~ legend_item_id ## in other cases, use the original value
    ))

# set data list id
nursing_cobrha_enriched_joined <- nursing_cobrha_enriched_joined %>%
  mutate(
    data_list_id=ifelse(is.na(osm_id),data_list_id_cob,data_list_id_osm))


# set correct geometry, original_id, properties and name
nursing_cobrha_enriched_joined <- as.data.frame(nursing_cobrha_enriched_joined) %>%
  rowwise() %>%
  mutate(
    original_id = ifelse(is.na(osm_id), original_id, paste0("https://osm.org/", osm_id)),
    name = ifelse(is.na(osm_id), name, osm_name),
    properties_secondary = ifelse(!is.na(osm_id), properties, NA),
    properties = ifelse(is.na(osm_id), properties, osm_properties),
    geometry = case_when(
      is.na(osm_id) ~ off_geometry,
      TRUE ~ geometry_osm
    )
  ) %>%
  ungroup()

# set risk level
nursing_cobrha_enriched_joined <- nursing_cobrha_enriched_joined %>%
  rowwise() %>%
  mutate(risk_level = max(risk_level, osm_risk_level, na.rm = TRUE))


# Keep only relevant columns
nursing_cobrha_enriched_joined <- nursing_cobrha_enriched_joined %>%
  select(original_id, name, risk_level, data_list_id, legend_item_id, properties, properties_secondary, geometry)

# Ensure the resulting dataframe is an sf object
nursing_cobrha_enriched_joined <- st_as_sf(nursing_cobrha_enriched_joined)
st_crs(nursing_cobrha_enriched_joined) <- 4326



# merge OSM & enriched two datasets
## do an anti-join to remove OSM objects already used in cobrha data
osm_properties <- osm_properties %>%
  anti_join(as.data.frame(nursing_cobrha_enriched_joined), by = "original_id")

nursing_home_be_not_in_cobrha <- osm_properties %>% filter(!is.na(language))
print(paste0(nrow(nursing_home_be_not_in_cobrha), " nursing homes are mapped in OSM but could not be found in Cobrha. Sometimes these are real issues, sometimes it's just that the point in cobrha is already assigned to a nearby facility or may be pinned incorrectly"))

## format jsons as text
osm_properties$name <- as.character(osm_properties$name)
osm_properties$properties <- as.character(osm_properties$properties)

osm_cobrha_merge <- bind_rows(nursing_cobrha_enriched_joined,osm_properties)


# Verify if geometries are valid and try to fix
invalid_geometries <- osm_cobrha_merge %>% filter(st_is_valid(geometry) == FALSE)
print(paste0("Invalid geometries: ", nrow(invalid_geometries)))
if (nrow(invalid_geometries)>0) {
  osm_cobrha_merge$geometry <- st_make_valid(osm_cobrha_merge$geometry)
}
invalid_geometries <- osm_cobrha_merge %>% filter(st_is_valid(geometry) == FALSE)
if (nrow(invalid_geometries)>0) {
  stop(paste0("ERROR: There are still ",nrow(invalid_geometries)," invalid geometries after fix"))
}

# verify all objects have a name
invalid_count <- osm_cobrha_merge %>%
  filter(!sapply(name, function(x) {
    if (is.null(x) || is.na(x) || x == "") return(FALSE)
    parsed <- tryCatch(
      jsonlite::fromJSON(x),
      error = function(e) NULL
    )
    if (is.null(parsed) || length(parsed) == 0) return(FALSE)
    any(c("dut", "fre", "ger", "und") %in% names(parsed))
  })) %>%
  nrow()
if (invalid_count>0) {
  stop(paste0("ERROR: There are ",invalid_count," objects without a name"))
}


# upload to curation
CreateImportTable(dataset = osm_cobrha_merge, schema = "raw_data", table_name = "nursery_home_osm_cobrha")

}

# LOAD ----
# """""""" ----



### Create SQL for proper ingestion table ----


ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.nursing_homes CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.nursing_homes
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
  CONSTRAINT nursing_homes_pkey PRIMARY KEY (id)
);",paste0("
INSERT INTO ingestion.nursing_homes 
(original_id, name, legend_item_id, data_list_id, risk_level, properties, properties_secondary, geometry, created_at)
select 
  original_id, 
  name::jsonb, 
  legend_item_id::uuid,
  data_list_id::uuid,
  risk_level,
	properties::jsonb,
	jsonb_strip_nulls(jsonb_build_object('",data_list_id_cob,"', properties_secondary::jsonb)) AS properties_secondary,
	geometry,
	CURRENT_DATE as created_at
from raw_data.nursery_home_osm_cobrha;"),
"ALTER TABLE IF EXISTS ingestion.nursing_homes OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.nursing_homes TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.nursing_homes TO pgn_user_airflow;")
                         
            
### Execute the SQL commands ----
   
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}

run_smart_update = function() {
  smart_update_process("nursing_homes", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run)
}



# Main function -----------------------------------------------------------
# """"""""""""""""""""----

main_function = function() {
  if (!reuse_ingestion_data) {
    process_osm_data()
    process_and_merge_cobrha_data()
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}

