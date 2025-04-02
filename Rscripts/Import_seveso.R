## ---------------------------
##
## Script name: ETL flow for seveso
##
## Purpose of script: Merge data about seveso sites from regions and federal sources and transform into proto-anchors for Paragon + update seveso.be
##
## Author: Joost Schouppe
##
## Date Created: 2023-12-14
##
##
## ---------------------------


# what to do before/while/after the script runs?
## update the file locations and the new table and file names!
## inform Jonathan that a new table has been created in GISGOV (see upload new data to GISGOV)
## email Comm <comm@nccn.fgov.be> with Bettina in CC with the excel file created under "save an Excel for Communications"
## email DRI.Business.PoliceAccounting@police.belgium.eu with a new geo CSV

# note: script assumes sites do not move, but Flanders now does manage the coordinates. To be monitored, especially during smart update!

# NOTE
## if there is NO new file, make sure we don't geocode the new records from last time again. This is likely NOT possible, unless there is an inconsistency between the ACR file and the regional files.


# todo
## migrate from csv to a table in curated_dev_playground
## build a check to see if the list of used provinces is still what we expect
## if allowed, update the excel for the website with this: select(-contact_entreprise,-site_firme) to keep all the relevant info --- Bettina said no, to be continued
## if Brussels has a new site, the  assignment of region and province name etc will not work!

# known issues
## what if the 99X IDs reach the real IDs?

# FAQ
## what is site_firme and why is it empty, same for contact_entreprise. 
#We can ignore it but don't delete it
## how to get description in the other language?
# delete the text when it's from the other language



# script main logic
## We have a GISGOV DB with enriched Seveso data.
## The data contains some federal and some regional data.
## Source of truth are the regions.
## The federal government uses sequential numbers as a unique identifier. The regions use unique identifiers like VL1234.
## For some reason, the GISGOV dataset only contains the federal ID, not the regional one.
## The federal dataset (ACR) contains the federal ID, the regional ID and the last inspection date. This implies a new Seveso site does not have a federal ID until it is inspected. Therefor we generate fake federal IDs for these cases. These are archived in a CSV file for now.

## We update Seveso when we have an update from the three regions and the federal government. They send the data to paragon-data@nccn.fgov.be unprompted. If they do not, ask Bettina Leoni <Bettina.Leoni@nccn.fgov.be> or Jeroen Raedschelders <Jeroen.Raedschelders@nccn.fgov.be> to give them a little push.
## Brussels usually just says "nothing changed", so we can keep using the last file we received from them.
## previously received data is kept on teams: https://mibz.sharepoint.com/:f:/r/sites/Paragon-ParagonDataChannel/Gedeelde%20documenten/Paragon%20Data%20Channel/Anchor/seveso?csf=1&web=1&e=WjVyuK



# Set parameters ------
readRenviron("C:/projects/pgn-data-airflow/.Renviron")
local_folder <- "C:/projects/proto-anchors/raw-data/seveso/"

log_folder <- "C:/temp/logs/"
rscript_folder <- "C:/projects/pgn-data-airflow/rscripts/"

# Update every time!
xlsx_output_filename <- "seveso_sites_03_25"
new_table <- "seveso_03_25"

# Update if new files received
fedlink_filename <- "seveso ACR 20250305.xlsx"
flanders_filename <- "20250305_VlaamsGewest.xlsx"
flanders_linkfile <- "20240704_Seveso_exploitant-vergunning.xlsx"
# note: doublecheck for "Seveso status"=0
# next update, check if changed coordinates in wallonia_20250113_notes.xlsx have been integrated
wallonia_filename <- "wallonia_20250221.xlsx"
brussels_filename <- "LIST_20231214_GegevensBedrijven.xlsx"



# Load external functions ------
source(paste0(rscript_folder,"utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"utils.R"))


# Library -----------------------------------------------------------------
# """""""""""""""""" ----------------------
library(openxlsx)
# other required libraries loaded via the utils scripts



# Load variables ------
#  """""""""""""""""" ------

# Paragon connection

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- get_azure_access_token()
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

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




#GISGOV connection
db_host_name_gg <- Sys.getenv("GISGOV_HOST_NAME")
postgres_user_gg <- Sys.getenv("GISGOV_USERNAME")
postgres_password_gg <- Sys.getenv("GISGOV_PASSWORD")
db_name_gg<- Sys.getenv("GISGOV_DBNAME")

get_con_gg<-function(){
  con_pg_gg <- dbConnect(Postgres(),
                         user=postgres_user_gg, 
                         password=postgres_password_gg,
                         host=db_host_name_gg,
                         dbname=db_name_gg,
                         port=5432, 
                         sslmode = 'prefer')
  return(con_pg_gg)
}






# EXTRACT ----
# """""""""""""""""" ----------------------


# Get current version ----

con_pg_gg<-get_con_gg()
seveso0<- dbGetQuery(con_pg_gg, "SELECT id, name, type, street, nr, zip, city, commune, province, region, contact_entreprise, site_firme, emanations, incendie, explosion, ecotoxique, act_all_nl, act_all_fr, act_all_de, act_all_en, x, y, date_inspection FROM seveso.seveso")
dbDisconnect(con_pg_gg)

# write a geojson for testing purposes 
#sf::write_sf(st_as_sf(seveso1, coords = c("x", "y"), remove = FALSE, crs = st_crs(31370)), paste0(log_folder,"seveso_old_",format(Sys.time(), "%Y%m%d_%H%M%S"),".geojson"), driver = "GeoJSON", delete_layer = TRUE)


#seveso_backup<-seveso0

seveso0 <- seveso0 %>% 
  mutate(in_previous=1)

# Get link/inspection ----

LoadFEDlink <- function(){  
  tryCatch({
    fedlink <- readxl::read_xlsx(paste0(local_folder,fedlink_filename))
    fedlink <- fedlink %>% 
      mutate(in_fedlink=1)
  }, 
  error = function(e) {
    print(paste("Error loading fedlink:", e))
  })
  print("Loading fedlink is done")
  return(fedlink)
}

# Get Flanders  ----
LoadVL <- function(){  
  tryCatch({
    VL <- readxl::read_xlsx(paste0(local_folder,flanders_filename))
    names(VL) <- tolower(names(VL))
    VL <- VL %>% rename_with(~ if_else(. == "reference", ., paste0("vl.", .)), -reference)
    VL <- VL %>% mutate(in_VL=1)
    print("Loading VL is done")
    return(VL)
  }, 
  error = function(e) {
    print(paste("Error loading VL:", e))
    return(NULL)
  })
}

# Get Flanders linkfile  ----
LoadVLpolylink <- function(){  
  tryCatch({
    VL_polylink <-readxl::read_xlsx(paste0(local_folder,flanders_linkfile))
    names(VL_polylink) <- tolower(names(VL_polylink))
    colnames(VL_polylink) <- gsub(" ", "", colnames(VL_polylink))
    print("Loading VL polylinks is done")
    return(VL_polylink)
  }, 
  error = function(e) {
    print(paste("Error loading VL_polylink:", e))
    return(NULL)
  })
}



# Get Wallonia  ----
LoadWAL <- function(){  
  tryCatch({
    WAL <-readxl::read_xlsx(paste0(local_folder,wallonia_filename))
    names(WAL) <- tolower(names(WAL))
    WAL <- WAL %>% rename_with(~ if_else(. == "reference", ., paste0("wal.", .)), -reference)
    colnames(WAL) <- gsub(" ", "", colnames(WAL))
    WAL <- WAL %>% mutate(in_WAL=1)
    print("Loading WAL is done")
    return(WAL)
  }, 
  error = function(e) {
    print(paste("Error loading WAL:", e))
    return(NULL)
  })
}

# Get Brussels  ----
LoadBRU <- function(){  
  tryCatch({
    BRU <-readxl::read_xlsx(paste0(local_folder,brussels_filename))
    names(BRU) <- tolower(names(BRU))
    BRU <- BRU %>% rename_with(~ if_else(. == "reference", ., paste0("bru.", .)), -reference)
    BRU <- BRU %>% mutate(in_BRU=1)
    print("Loading BRU is done")
    return(BRU)
  }, 
  error = function(e) {
    print(paste("Error loading BRU:", e))
    return(NULL)
  })
}


# Get "manually linked" fed_id/reference info  ----
LoadOwnlink <- function(){  
  tryCatch({
    ol <- read.csv(paste0(local_folder,"own_links.csv"), sep=",")
    ol <- ol %>% 
      mutate(in_fedlink=0, inspectiondate=NA)
    print("Loading OL is done")
    return(ol)
  }, 
  error = function(e) {
    print(paste("Error loading OL:", e))
    return(NULL)
  })
}

# Run data collection functions ------

fedlink <- LoadFEDlink()
VL <- LoadVL()
VL_polylink <- LoadVLpolylink()
WAL <- LoadWAL()
BRU <- LoadBRU()





# read csv with special cases
OL <- LoadOwnlink()
write.csv(OL, file = paste0(local_folder, "own_links_backup_", Sys.Date(), ".csv"), row.names = FALSE)

# TRANSFORM ----
# """""""""""""""""" ----------------------


# Update the ID number of sites recently added to the federal link dataset but that still have a 99X number ----

# Prepare link file for special cases

fedlink <- rbind(fedlink, OL)

# if a special case became a normal case, change the ID number in seveso0
## prepare special purpose table
duplicates <- fedlink %>%
  group_by(reference) %>%
  filter(n() > 1) %>%
  ungroup()
updater <- duplicates %>%
  group_by(reference) %>%
  mutate(new_fed_id = min(fed_id)) %>%
  filter(fed_id == max(fed_id)) %>%
  ungroup() %>%
  select(fed_id, new_fed_id)



# remove from updater if the new_fed_id already exists in seveso0 (this means a formerly missing ID number is now already known)
updater <- updater %>%
  anti_join(seveso0, by = c("new_fed_id" = "id"))


## manual fix for 999. This won't do anything next time, this is exceptional because we will keep an archive of our own IDs on file from now on.
additional_record <- data.frame(fed_id = 999, new_fed_id = 9999)
updater <- bind_rows(updater, additional_record)

## merge to seveso0
seveso0 <- merge(seveso0, updater, by.x = "id", by.y = "fed_id", all = TRUE)

## update the newly known IDs
seveso0 <- seveso0 %>%
  mutate(id = if_else(!is.na(new_fed_id), new_fed_id, id))

## remove special cases
seveso0 <- seveso0 %>%
  filter(id<9999)


# we added the special cases to fedlink in the first step. These cases might have a real number by now. So then we have created some duplicates. We keep the record with the real number (instead of a 99X number)
fedlink <- merge(fedlink, updater, by.x = "fed_id", by.y = "fed_id", all = TRUE)
fedlink <- fedlink %>%
  filter(is.na(new_fed_id))
fedlink <- fedlink %>%
  select(-new_fed_id)
# if there's still duplicates, keep the version with the lowest fed_id
fedlink <- fedlink %>%
  group_by(reference) %>%
  filter(fed_id == min(fed_id)) %>%
  ungroup()


# now we can merge!

# Add the regional ID and merge the regional data to the previous seveso dataset ----

new_merge <- merge(seveso0, fedlink, by.x = "id", by.y = "fed_id", all = TRUE)
new_merge <- merge(new_merge, VL, by.x = "reference", by.y = "reference", all = TRUE)
new_merge <- merge(new_merge, WAL, by.x = "reference", by.y = "reference", all = TRUE)
new_merge <- merge(new_merge, BRU, by.x = "reference", by.y = "reference", all = TRUE)


# Check duplicates (N should always be 0)
dup_check_id <- new_merge %>%
  group_by(id) %>%
  summarise(frequency = n()) %>%
  filter(frequency > 1 & !is.na(id))
dup_check_reference <- new_merge %>%
  group_by(reference) %>%
  summarise(frequency = n()) %>%
  filter(frequency > 1 & !is.na(reference))

# cat the number of duplicates if there are any records in the datasets
# the process will stop automatically if there are any
if (nrow(dup_check_id) > 0) {
  stop("Duplicates in id column: ", nrow(dup_check_id), "\n")
}
if (nrow(dup_check_reference) > 0) {
  stop("Duplicates in reference column: ", nrow(dup_check_reference), "\n")
}


# Remove cases not found in regional data anymore  ----

## if reference is missing: remove. This means it is not in the regional dataset, and hence not a seveso site
new_merge <- new_merge %>%
  filter(!(is.na(reference)))

## if not in VL nor WAL nor BRU, throw it away
### this is not duplicate with the step above, because the case might still be in the fedlink dataset, even if it is not in the regional dataset anymore
new_merge <- new_merge %>%
  filter(!(is.na(in_VL) & is.na(in_WAL) & is.na(in_BRU)))


# Give a temporary (fed_)id to new objects not in fedlink yet ----
## if there are id = NULL, give it a number lower than the lowest in OL and update OL.csv afterwards

if (any(is.na(new_merge$id))) {
  # Step 1: Find the lowest number in the 'fed_id' column in dataframe OL which is above 900
  lowest_fed_id <- min(OL$fed_id[OL$fed_id >= 900], na.rm = TRUE)
  
  # Step 2: Identify rows in seveso0 where id is NULL
  new_merge_with_null_id <- new_merge %>%
    filter(is.na(id))
  
  # Step 3: Fill in consecutive lower numbers in the 'id' column
  new_ids <- seq(lowest_fed_id-1, by = -1, length.out = nrow(new_merge_with_null_id))
  
  new_merge_with_null_id <- new_merge_with_null_id %>%
    mutate(id = new_ids)
  
  # Step 4: Create new records in OL for filled id values
  new_records_OL <- new_merge_with_null_id %>%
    select(id, reference) %>%
    distinct() %>%
    rename(fed_id = id) %>%
    bind_rows(OL, .)
  
  new_records_OL <- new_records_OL %>%
    select(fed_id, reference)
  
  # visual inspection of the new records, to avoid a previous issue where the file was messed up
  print(new_records_OL)
  
  write.csv(new_records_OL, file = paste0(local_folder,"own_links.csv"), row.names = FALSE)
  
  # Step 5: update new_merge with the new ID numbers
  
  new_merge <- bind_rows(new_merge,new_merge_with_null_id)
  # Filter out rows where 'id' is NULL
  new_merge <- new_merge %>%
    filter(!is.na(id))
  
} else {
  cat("No NULL values found in 'id' column.")
}




# VL - overwrite the contents of the old dataset with what's in the new dataset ----

new_merge <- new_merge %>%
  mutate(
    emanations = ifelse(grepl("^VL", reference), 
                        ifelse(as.logical(vl.risktoxic) == TRUE, 1, 0), 
                        emanations),
    incendie = ifelse(grepl("^VL", reference), 
                      ifelse(as.logical(vl.riskfire) == TRUE, 1, 0), 
                      incendie),
    explosion = ifelse(grepl("^VL", reference), 
                       ifelse(as.logical(vl.riskexplosion) == TRUE, 1, 0), 
                       explosion),
    ecotoxique = ifelse(grepl("^VL", reference), 
                        ifelse(as.logical(vl.riskecotoxic) == TRUE, 1, 0), 
                        ecotoxique)
  )


new_merge <- new_merge %>%
  mutate(
    name = ifelse(!is.na(vl.name), vl.name, name),
    street = ifelse(!is.na(vl.address), vl.address, street),
    nr = ifelse(!is.na(vl.housenr), vl.housenr, nr),
    zip = ifelse(!is.na(vl.zipcode), vl.zipcode, zip),
    city = ifelse(!is.na(vl.city), vl.city, city),
    act_all_nl = ifelse(!is.na(vl.description), vl.description, act_all_nl),
    x = ifelse(!is.na(vl.lambertx), vl.lambertx, x),
    y = ifelse(!is.na(vl.lamberty), vl.lamberty, y),
    type = ifelse(!is.na(vl.sevesostatus), vl.sevesostatus, type)
  )


# WAL - overwrite the contents of the old dataset with what's in the new dataset ----

new_merge <- new_merge %>%
  mutate(
    emanations = ifelse(grepl("^WA", reference), 
                        ifelse(as.logical(wal.risktoxic) == TRUE, 1, 0), 
                        emanations),
    incendie = ifelse(grepl("^WA", reference), 
                      ifelse(as.logical(wal.riskfire) == TRUE, 1, 0), 
                      incendie),
    explosion = ifelse(grepl("^WA", reference), 
                       ifelse(as.logical(wal.riskexplosion) == TRUE, 1, 0), 
                       explosion),
    ecotoxique = ifelse(grepl("^WA", reference), 
                        ifelse(as.logical(wal.riskecotoxic) == TRUE, 1, 0), 
                        ecotoxique)
  )

new_merge <- new_merge %>%
  mutate(
    name = ifelse(!is.na(wal.name), wal.name, name),
    street = ifelse(!is.na(wal.address), wal.address, street),
    nr = ifelse(!is.na(wal.housenr), wal.housenr, nr),
    zip = ifelse(!is.na(wal.zipcode), wal.zipcode, zip),
    city = ifelse(!is.na(wal.city), wal.city, city),
    act_all_fr = ifelse(!is.na(wal.description), wal.description, act_all_fr),
    x = ifelse(!is.na(wal.lambertx), wal.lambertx, x),
    y = ifelse(!is.na(wal.lamberty), wal.lamberty, y),
    type = ifelse(!is.na(wal.sevesostatus), wal.sevesostatus, type)
  )

# BRU - overwrite the contents of the old dataset with what's in the new dataset ----

new_merge <- new_merge %>%
  mutate(
    emanations = ifelse(grepl("^BR", reference), 
                        ifelse(as.logical(bru.risktoxic) == TRUE, 1, 0), 
                        emanations),
    incendie = ifelse(grepl("^BR", reference), 
                      ifelse(as.logical(bru.riskfire) == TRUE, 1, 0), 
                      incendie),
    explosion = ifelse(grepl("^BR", reference), 
                       ifelse(as.logical(bru.riskexplosion) == TRUE, 1, 0), 
                       explosion),
    ecotoxique = ifelse(grepl("^BR", reference), 
                        ifelse(as.logical(bru.riskecotoxic) == TRUE, 1, 0), 
                        ecotoxique)
  )

new_merge <- new_merge %>%
  mutate(
    name = ifelse(!is.na(bru.name), bru.name, name),
    street = ifelse(!is.na(bru.address), bru.address, street),
    nr = ifelse(!is.na(bru.housenr), bru.housenr, nr),
    zip = ifelse(!is.na(bru.zipcode), bru.zipcode, zip),
    city = ifelse(!is.na(bru.city), bru.city, city),
    act_all_nl = ifelse(!is.na(bru.description), bru.description, act_all_nl),
    x = ifelse(!is.na(bru.lambertx), bru.lambertx, x),
    y = ifelse(!is.na(bru.lamberty), bru.lamberty, y),
    type = ifelse(!is.na(bru.sevesostatus), bru.sevesostatus, type)
  )

# test for missing or empty names ----
missing_name <- new_merge %>%
  filter(is.na(name) | name == "")
if (nrow(missing_name) > 0) {
  stop(nrow(dup_check_id)," records have no name", "\n")
}

# to be used when/if we decided to use the nickname
#test_name <- new_merge %>%
#  select(reference,id,name,vl.nickname, wal.nickname, bru.nickname) %>%
#  mutate(nickname=ifelse(!is.na(vl.nickname), vl.nickname, 
#                         ifelse(!is.na(wal.nickname), wal.nickname, bru.nickname))) %>%
#  select(-vl.nickname,-wal.nickname,-bru.nickname) %>%
#  mutate(useful_nick=ifelse(nickname==name, 0, 1))

# save as csv write.csv(test_name, paste0(log_folder,"test_name.xlsx"), row.names = FALSE)


# update the inspection date ----
new_merge <- new_merge %>%
  mutate(date_inspection=inspectiondate)

# geocode new records & fill in location info ----

# example code: how to manually set a location:
# YOU NEED TO ADD commune, region and province manually if the geometry from a new record is added via the raw files
#new_merge <- new_merge %>%  mutate(
    #x = ifelse(reference == "VL0220", 153143, x),
    #y = ifelse(reference == "VL0220", 216740, y),
    #commune = ifelse(reference == "VL0766", "Kortrijk", commune),
    #region = ifelse(reference == "VL0766", "Vlaams Gewest", region),
    #province = ifelse(reference == "VL0766", "Antwerpen", province)  )



# please note: perfect geocoding is needed, if there are missing coordinates, add them manually!
## keep relevant info only and turn into regular dataframe (no geometry)
geocode_input <- new_merge %>%
  filter(is.na(x)) %>%
  select(reference,street,nr,zip)

# stop if there are cases
if(nrow(geocode_input) > 0){
  stop(nrow(geocode_input)," records have no coordinates", "\n")
}


# do if geocode_input is not empty
if(nrow(geocode_input) > 0){
  # extra for geocoding
  library(phacochr)
  phaco_setup_data()
  phacochr::phaco_best_data_update()
  
  
  
  ##geocode_input <- as.data.frame(st_drop_geometry(geocode_input))
  
  ## actual geocode
  geocode_output <- phaco_geocode(data_to_geocode=t_adresse <- geocode_input, colonne_rue= "street", colonne_num="nr", colonne_code_postal="zip")
  
  ## cleanup (extract dataset, keep relevant columns only, create x and y columns and drop geometry)
  geocode_output <- geocode_output[["data_geocoded_sf"]]
  geocode_output <- geocode_output %>%
    select(reference,tx_munty_descr_nl,tx_munty_descr_fr,
           tx_prov_descr_nl,tx_prov_descr_fr,tx_rgn_descr_nl,tx_rgn_descr_fr)
  geocode_output$x_new <- st_coordinates(geocode_output)[, "X"]
  geocode_output$y_new <- st_coordinates(geocode_output)[, "Y"]
  geocode_output <- as.data.frame(st_drop_geometry(geocode_output))
  
  ## add the new info to the main dataset
  new_merge <- merge(new_merge, geocode_output, by.x = "reference", by.y = "reference", all = TRUE)
  
  ## fill in the location info (easy cases)
  new_merge <- new_merge %>%
    mutate(
      city = ifelse(!is.na(tx_munty_descr_nl), tx_munty_descr_nl, city),
      commune = ifelse(!is.na(tx_munty_descr_fr), tx_munty_descr_fr, commune),
      x = ifelse(!is.na(x_new), x_new, x),
      y = ifelse(!is.na(y_new), y_new, y)
    )
  
  ## fill in the region in the proper language
  # this assumes Brussels never gives a new site!
  new_merge <- new_merge %>%
    mutate(
      region = case_when(
        tx_rgn_descr_nl == "Vlaams Gewest" ~ "Vlaams Gewest",
        tx_rgn_descr_nl == "Waals Gewest" ~ "Région wallonne",
        TRUE ~ region  # Keep the original 'region' value for other cases
      )
    )
  
  ## fill in the province in the proper language
  new_merge <- new_merge %>%
    mutate(
      province = case_when(
        tx_rgn_descr_nl == "Vlaams Gewest" ~ tx_prov_descr_nl,
        tx_rgn_descr_nl == "Waals Gewest" ~ tx_prov_descr_fr,
        TRUE ~ province  # Keep the original 'province' value for other cases
      )
    ) %>%
    mutate(
      province = gsub("^(Province de |Province du |Provincie )", "", province)
    ) %>%
    mutate(
      province = str_to_title(province)
    )

  geocode_input_test <- new_merge %>%
    filter(is.na(x)) %>%
    select(reference,street,nr,zip)  
  
  if(nrow(geocode_input_test) > 0){
    stop(nrow(geocode_input_test)," records have no coordinates after geocoding", "\n")
  }
  
}
# end conditional geocoding



## update or create the geometry based on the x and y columns
new_merge <- as.data.frame(st_drop_geometry(new_merge))
new_merge <- st_as_sf(new_merge, coords = c("x", "y"), remove = FALSE, crs = st_crs(31370))


# because there is no translation update process in place, updates in the region will result in errors. So we erase the other language
new_merge <- new_merge %>%
  mutate(
    act_all_fr = ifelse(grepl("^VL", reference), 
                        '', 
                        act_all_fr)
  )
new_merge <- new_merge %>%
  mutate(
    act_all_nl = ifelse(grepl("^WA", reference), 
                        '', 
                        act_all_nl)
  )




# remove unneeded columns ----
new_merge <- new_merge %>%
  select(id,reference,name,type,street,nr,zip,city,commune,province,region,contact_entreprise,site_firme,emanations,incendie,explosion,ecotoxique,act_all_nl,act_all_fr,act_all_de,act_all_en,x,y,date_inspection,geometry)


# make the site names sort of unique
new_merge <- new_merge %>%
  group_by(name) %>%
  mutate(name = case_when(
    n() > 1 ~ paste0(name, " (", city, ")"),
    TRUE ~ name
  )) %>%
  ungroup()



# checking for strange values in the type column. Should always be 1 (low risk) or 2 (high risk). If it's a value like "in review", it should be removed (which will happen in a next step). If it's value like "high risk" or "low risk", it should be changed to 1 or 2. Consult with source or Jonathan.
# the process will stop automatically if this is the case!
check_type <- new_merge %>%
  group_by(type) %>%
  summarise(frequency = n())
check_type_errors<-new_merge %>%
  filter(!(type==1 | type==2))

if(nrow(check_type_errors)>0){
  stop("There are errors in the type column. Please review the data.")
}

# if there are other values than 1 or 2, please review. 
new_merge<-new_merge %>%
  filter(type==1 | type==2)

# if there were some other values, the column will not be integer but text. Explicitly cast as integer.
new_merge$type <- as.integer(new_merge$type)



# write a local testing file
#sf::write_sf(new_merge, paste0(log_folder,"seveso_new_",format(Sys.time(), "%Y%m%d_%H%M%S"),".geojson"), driver = "GeoJSON", delete_layer = TRUE)

# set as date variable
new_merge <- new_merge %>%
  mutate(date_inspection = as.Date(date_inspection, format = "%Y-%m-%d"))


# LOAD ----
# """""""""""""""""" ----------------------


# upload new data to GISGOV ----

gisgov <- as.data.frame(st_drop_geometry(new_merge))

CreateImportTableGG<-function(dataset, schema, table_name){
  if (!exists("dataset")) {
    print(paste0("Error, the geojson you wanted to import into ", table_id_t, "does not exist, try again"))
  }else{
    con_pg<-get_con_gg()
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
                   "ADD PRIMARY KEY (id);")
    dbExecute(con_pg, query)
    
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
  }
}


# update the GISGOV MV ----


gisgov_mv_sql <- c("
DROP MATERIALIZED VIEW IF EXISTS seveso.seveso;",
                   paste0("
CREATE MATERIALIZED VIEW IF NOT EXISTS seveso.seveso
TABLESPACE pg_default
AS
SELECT id,
name,
type,
street,
nr,
zip,
city,
commune,
province,
region,
contact_entreprise,
site_firme,
emanations,
incendie,
explosion,
ecotoxique,
COALESCE(act_all_nl, ''::character varying::text) AS act_all_nl,
COALESCE(act_all_fr, ''::character varying::text) AS act_all_fr,
COALESCE(act_all_de, ''::character varying::text) AS act_all_de,
COALESCE(act_all_en, ''::character varying::text) AS act_all_en,
x::numeric,
y::numeric,
date_inspection,
st_transform(st_setsrid(st_makepoint(x::numeric, y::numeric), 31370), 4326)::geography AS the_geog
FROM seveso.",new_table,
                          " WITH DATA;"),"
ALTER TABLE IF EXISTS seveso.seveso
OWNER TO gisgov;","
GRANT SELECT ON TABLE seveso.seveso TO geoserver;","
GRANT ALL ON TABLE seveso.seveso TO gisgov;","
GRANT SELECT ON TABLE seveso.seveso TO \"github-actions\";","
GRANT SELECT ON TABLE seveso.seveso TO \"joost.schouppe@nccn.fgov.be\";")

GisgovMV <- function() {
  con_pg <- get_con_gg()
  tryCatch(
    {
      for (sql_command in gisgov_mv_sql) {
        dbExecute(con_pg, sql_command)
      }
      print("GISGOV materialized view created/updated")
    },
    error = function(err) {
      print("The SQL functions for GISGOV materialized view failed")
      print(err)  # Print the error message for more details
    }
  )
  dbDisconnect(con_pg)
}


# save an Excel for Communications ----
excel <- new_merge %>%
  select(id,name,type,street,nr,zip,city,commune,province,region,act_all_nl,act_all_fr,act_all_de,act_all_en,date_inspection)
excel <- as.data.frame(st_drop_geometry(excel))

write.xlsx(excel, file = paste0(local_folder,"outputs/",xlsx_output_filename,".xlsx"))


# save a CSV for the police ----
csv <- as.data.frame(st_drop_geometry(new_merge))
write.csv(csv, file = paste0(local_folder,"outputs/",xlsx_output_filename,".csv"), row.names = FALSE)



# create Paragon anchors ----


# add old gisgov data
#con_pg_gg<-get_con_gg()
#old_perimeters<- dbGetQuery(con_pg_gg, "SELECT id,ST_AsText(the_geog::geometry) AS geometry_old FROM seveso.perimeters")
#dbDisconnect(con_pg_gg)


### Convert to sf
#old_perimeters<-st_as_sf(old_perimeters, wkt="geometry_old")
#old_perimeters$geometry_old <- st_set_crs(old_perimeters$geometry_old, 4326)
#old_perimeters <- st_transform(old_perimeters, 31370)


### Add Flemish polygon data ----

gpkg_vla <- "https://datasets.omgeving.vlaanderen.be/be.vlaanderen.omgeving.distribution.geo.55136ef3-4a94-40b9-9321-24f0a58e48f6.pf_seveso_ter_gpkg"

# Define the local file path to save the downloaded GPKG
local_file <- paste0(local_folder,"pf_seveso_ter.gpkg")

# Download the GPKG file
GET(gpkg_vla, write_disk(local_file, overwrite = TRUE))

# Read the GPKG file using sf
gpkg_vla <- st_read(local_file, layer = "pf_seveso_ter")
#plot(gpkg_vla$geom)

# in Flanders, one "exploitant" (federal logic) can have more than one "vergunningshouder" (Flemish logic). To deal with this, we merge polygons that have the same "exploitant".
# in Flanders, one "vergunningshouder" can have more than one exploitant. We "ignore" this. That means we only find one polygon. The other exploitants on the same area will simply retain a point on that polygon. There's only a single case: Campina. Maybe it makes sense to follow the Flemish logic in Paragon, which means we could also merge the organizations 
VL_polylink <- VL_polylink %>%
  select(refpermit="vl-nummervergunninghouder", refexpl="vl-nummerexploitant")
gpkg_vla <- gpkg_vla %>%
  left_join(VL_polylink, by=c("referentie"="refpermit")) %>%
  mutate(referentie = ifelse(!is.na(refexpl), refexpl, referentie)) %>%
  select(referentie, geom) %>%
  group_by(referentie) %>%
  summarise(geom = st_union(geom)) %>%
  ungroup()


gpkg_vla_df<- as.data.frame(gpkg_vla) %>%
  select(referentie, geom) %>%
  rename(geometry_pg = geom)





### Add Brussels polygon data ----
# open Brussels polygons
brussels_pg<-st_read(paste0(local_folder,"polygons-brussels-lambert72.geojson"))
# rename geometry to geometry_pg
brussels_pg<-brussels_pg %>% rename(geometry_pg = geometry)

polygons <-bind_rows(gpkg_vla_df, brussels_pg)


### Add Wallonia polygon data ----
# download 
wallonia_pg <- st_read("https://geoservices.wallonie.be/arcgis/rest/services/INDUSTRIES_SERVICES/SEVESO/MapServer/1/query?where=1%3D1&outFields=*&returnGeometry=true&f=geojson")
wallonia_pg <- wallonia_pg %>% rename(geometry_pg = geometry)
wallonia_pg <- wallonia_pg %>% st_transform(31370)
wallonia_pg <- wallonia_pg %>%
  mutate(referentie = sprintf("WA%04d", REF_SEVESO)) %>%
  select(referentie, geometry_pg)

polygons <-bind_rows(polygons, wallonia_pg)


### Join polygon data to points ----
new_merge_pg <- left_join(new_merge, polygons, by = c("reference" = "referentie"))


new_merge_pg$distance <- mapply(calculate_distance_integrated, new_merge_pg$geometry, new_merge_pg$geometry_pg)


# this logic should not be needed anymore, but is useful as a fallback in case there is an error
# if distance>0, make geometry_pg empty
new_merge_pg$geometry_pg[!is.na(new_merge_pg$distance) & new_merge_pg$distance > 0] <- NA
new_merge_pg<-new_merge_pg %>% 
  select(-distance) %>%
  rename(geometry_pt = geometry)

# select just the records of new_merge_pg that have an invalid geometry_pg
invalid_geometries <- new_merge_pg %>% filter(st_is_valid(geometry_pg) == FALSE)
print(paste0("Invalid geometries: ", nrow(invalid_geometries)))
if (nrow(invalid_geometries)>0) {
new_merge_pg$geometry_pg <- st_make_valid(new_merge_pg$geometry_pg)
} 
invalid_geometries <- new_merge_pg %>% filter(st_is_valid(geometry_pg) == FALSE)
print(paste0("Invalid geometries after fix: ", nrow(invalid_geometries)))
if (nrow(invalid_geometries)>0) {
  stop("ERROR: There are still invalid geometries after fix")
}


### Import to raw data ----



### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.seveso CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.seveso
  (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    original_id text,    
    name jsonb,
    legend_item jsonb,
	data_list_id text,
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
	geometry_pt geometry(geometry, 4326),
	geometry_pg geometry(geometry, 4326),
    CONSTRAINT seveso_pkey PRIMARY KEY (id)
  );
","

WITH cleaned as (SELECT
reference AS original_id,
name,
LTRIM(CONCAT(street,' ', nr, ', ', zip,' ',
			 CASE WHEN region='Vlaams Gewest' THEN city
			 WHEN region='Région wallonne' THEN commune
			 ELSE commune END)) AS address,
id, type, province, region, emanations, incendie, explosion, ecotoxique, act_all_nl, act_all_fr, date_inspection,
CASE WHEN ST_IsEmpty(geometry_pg) THEN ST_Transform(geometry_pt,4326)
	    ELSE ST_Transform(geometry_pg,4326) END as geometry,
ST_Transform(geometry_pt,4326) as geometry_pt,
ST_Transform(geometry_pg,4326) as geometry_pg
FROM raw_data.seveso),
				 
aggregated as (SELECT string_agg(original_id, ';') as original_id,
string_agg(name,'; ') as name,
jsonb_build_object(
	'dut', 'Sevesobedrijf',
	'fre', 'entreprise Seveso',
	'ger', 'Seveso-betriebs') as legend_item,
	min(address) as address,
	string_agg(id::text, ';') as id,
	min(type) as type,
	min(province) as province,
	min(region) as region,
	min(emanations) as emanations,
	min(incendie) as incendie,
	min(explosion) as explosion,
	min(ecotoxique) as ecotoxique,
	min(act_all_nl) as act_all_nl,
	min(act_all_fr) as act_all_fr,
	min(date_inspection) as date_inspection,
	geometry,
	min(geometry_pt)::geometry as geometry_pt,
	min(geometry_pg)::geometry as geometry_pg
FROM cleaned
GROUP by geometry)

INSERT INTO ingestion.seveso 
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, geometry_pt, geometry_pg, created_at)
SELECT
original_id,
jsonb_build_object('und', CASE WHEN name IS NULL THEN 'seveso company' ELSE name END) as name,
legend_item,
'ccd8ec1f-d624-47dd-b24b-1df1e371c4b2' as data_list_id,
CASE WHEN type=2 THEN 4
  WHEN type=1 THEN 3 ELSE null END as risk_level,
JSONB_STRIP_NULLS(JSONB_BUILD_OBJECT(
  'federal_id',id,
	'address', address,
	'risk_toxic',emanations,
	'risk_fire',incendie,
	'risk_explosion',explosion,
	'risk_ecotoxic',ecotoxique,
	'activity_dut',act_all_nl,
	'activity_fre',act_all_fr,
	'date_inspection',date_inspection,
	'seveso_risk', CASE WHEN type=2 THEN 'upper limit'
  WHEN type=1 THEN 'lower limit' ELSE null END)),
geometry,
geometry_pt,
geometry_pg,
CURRENT_DATE as created_at
FROM aggregated;
")

### ONLY IF YOU NEED TO START FROM SCRATCH - Create SQL for transformation table ----
transformation_table_sql <- c("
DROP TABLE IF EXISTS transformation.seveso CASCADE;
","
CREATE TABLE IF NOT EXISTS transformation.seveso
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
    CONSTRAINT seveso_pkey PRIMARY KEY (id)
  );
","
INSERT INTO transformation.seveso
(original_id, name, legend_item, data_list_id, risk_level, properties, geometry, created_at)
SELECT original_id, name, legend_item, data_list_id::uuid, risk_level,properties, geometry, created_at FROM ingestion.seveso;
")



### Execute the SQL commands ----



create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
create_transformation_table <- function() {execute_sql_commands(transformation_table_sql, "Transformation table")}



### ONLY IF YOU NEED TO START FROM SCRATCH
# create_transformation_table()
# create_fdw_views()
### ONLY IF YOU NEED TO START FROM SCRATCH - Create SQL for transformation table ----

# Run the import functions ----


### Generic checks ----

# Analyze the old data

### Get the old data
con_pg_gg <- get_con_gg()
seveso_old <- dbGetQuery(con_pg_gg, "SELECT *, ST_AsText(the_geog::geometry) AS geometry FROM seveso.seveso")
dbDisconnect(con_pg_gg)


### Convert to sf
seveso_old<-st_as_sf(seveso_old, wkt="geometry")
seveso_old$geometry <- st_set_crs(seveso_old$geometry, 4326)


### Create empty dataframe to store check results
check_old_data <- data.frame(name = character(),
                             check = character(),
                             results = numeric(),
                             stringsAsFactors = FALSE)

### Perform checks
check_old_data <- perform_check(seveso_old, "seveso", seveso_old$geometry, check_old_data, "old data")

### Create empty dataframe to store check results
check_new_data <- data.frame(name = character(),
                             check = character(),
                             results = numeric(),
                             stringsAsFactors = FALSE)

### Perform checks
check_new_data <- perform_check(new_merge, "seveso", new_merge$geometry, check_new_data, "new data")


# Full outer join of old & new check data
check_old_data <- full_join(check_old_data, check_new_data, by = c("name", "check"))
print(check_old_data)

# Sidestep: find new and deleted objects
# Merge old & new data to find new/deleted objects
# keep only ID
old_data <- seveso_old %>% select(id)
# add column old=1
old_data$old <- 1
new_data <- new_merge %>% select(id,reference)
new_data$new <- 1
joined <- full_join(old_data, as.data.frame(new_data), by = c("id"))
# select if it's a new or removed case
joined <- joined %>% filter(is.na(joined$old) | is.na(joined$new))
print(joined)

# Calculate the difference between old and new data and decides which checks did not pass
# avoid scientific notation
options(scipen = 999)
# allow scientific notation again
#options(scipen = 0)
check_old_data$diff <- check_old_data[["new data"]] - check_old_data[["old data"]]
check_old_data$diff_p <- abs(check_old_data$diff/check_old_data[["old data"]]*100)


# check failed if any of the following conditions are met
# bbox size or n is different by more than 10% and absolute diff is bigger than 5
check_old_data$failed_check <- ifelse((check_old_data$check == 'bbox size' | check_old_data$check == 'n') & check_old_data$diff > 5 & check_old_data$diff_p > 10, 1, 0)
# invalid or empty geometries are present in the new data
check_old_data$failed_check <- ifelse((check_old_data$check == 'invalid geometry' | check_old_data$check == 'missing geometry') & check_old_data[["new data"]] > 0, 1, check_old_data$failed_check)


### Context-specific checks ----

contextcheck<-ifelse(nrow(geocode_input)>0 | nrow(dup_check_reference)>0 | nrow(check_type_errors)>0, 1, 0)



# Summarize the results
checks_failed <- sum(check_old_data$failed_check) + contextcheck
print(paste0("Checks failed: ", checks_failed))

# Write a report ----
source_identifier <- new_table
filename <- paste0(log_folder,format(Sys.time(), "%Y%m%d_%H%M%S"),"_seveso_gisgov_check_",source_identifier,".txt")

# Report generic checks
write.table(check_old_data, filename, sep = "\t", quote = FALSE, row.names=FALSE, append = TRUE)
cat("# This is the comparison of old and new data after the new data was cleaned.
  The number of cases with missing geometry should always be 0 because Seveso MUST have geometry.
  We provide the number of cases in the old and new data. If the difference is big, the process will fail.
  We also provide the old and new size in km² of the bbox enclosing all the data.\n\n", file = filename, append = TRUE)
print(paste0("Report written to ",filename))

gisgov_update = function() {
  CreateImportTableGG(dataset = gisgov, schema = "seveso", table_name = new_table) 
  GisgovMV()
}

paragon_import = function() {
  CreateImportTable(dataset = new_merge_pg, schema = "raw_data", table_name = "seveso")  
  create_ingestion_table()
}


# set to TRUE if you want to update the transformation table even if the checks fail. 
update_even_if_checks_fail<-FALSE
# Don't forget to also set checks_failed<-0 if there were already some issues in the base data

# note: sites may change quite a bit between versions; the 1m threshold is way too strict for polygons; but is reasonable for points
run_smart_update = function() {
  smart_update_process("seveso", 50, 100, 50, new_table, update_even_if_checks_fail)
}

# in Paragon DB: refresh the MVs all_anchors and the layer seveso in the dev dbase



main_function = function() {
  gisgov_update()
  paragon_import()
  run_smart_update()
}

# uncomment below to ignore failed checks of the raw data (after you have reviewed and verified there is no real issue)
#checks_failed <- 0
# The smart_update_process has built in safeguards on top of the checks_failed. If you want to ignore the checks_failed, you need to set the allow_update_even_if_checks_fail to TRUE in the smart_update_process function (see above).

if(F){
  if (checks_failed == 0) {
    main_function()
  } else {
    stop("Paragon & GISGOV not updated due to failed checks in the initial validation. Please check the reports for details.")
  }
}



