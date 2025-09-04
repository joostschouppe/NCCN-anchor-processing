## ---------------------------
##
## Script name: Import Cobrha data
##
## Purpose of script: Download data from cobrha and make available in an easy to use way in raw data
##
## Author: Joost Schouppe
##
## Date Created: 2024-11-27
##
##
## ---------------------------



# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

temporary_folder <-Sys.getenv("TEMPORARY_STORAGE")
log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")


# run status
run_status<-Sys.getenv("RUN_STATUS")
## this is set to false and prevents any accidental changes to the database by switching off the main_function(). On Airflow, this is set to true.
run_status<-ifelse(tolower(run_status) == "true", TRUE, FALSE)


# SFTP connection parameters

# Define the path to your private key and SFTP server details
private_key <- Sys.getenv("COBRHA_PRIVATE_KEY")
#sftp_server <- "nccn_cobrha@sftp-acpt.ehealth.fgov.be" # this is the test server

# set the server
sftp_server <- Sys.getenv("COBRHA_SFTP_SERVER")


# conditionally create a reference to the private key
if (run_status==FALSE) {
  # if you are working locally, make sure the key location is mentioned in the .renviron
  private_key <- Sys.getenv("COBRHA_PRIVATE_KEY")
} else {
  # if not, read the content of the key from the Airflow variable and create a file from it
  private_key_text <- Sys.getenv("COBRHA_PRIVATE_KEY")
  private_key <- "/opt/airflow/data/id_rsa"
  writeLines(private_key_text, private_key)
  # set the right permissions
  Sys.chmod(private_key, mode = "600")
  print(paste0("Created private key at ", private_key))
  print(paste0("File permissions for private key are set to ",as.octmode(file.info(private_key)$mode)))
}



### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))

# Extra libraries -------------------------------
# """""""""""""""""" ----------------------
library(rvest)
library(readxl)





# EXTRACT ----
# """""""""""""""""" ----

# Function to retry downloading a file with PSFTP
retry_download <- function(sftp_options, max_attempts = 7, intervals = c(3, 10, 30)) {
  for (attempt in seq_len(max_attempts)) {
    message(paste("Attempt", attempt, "of", max_attempts, "to download file via SFTP..."))
    
    # Run SFTP command
    result <- system2("sftp", args = sftp_options, stdout = TRUE, stderr = TRUE)
    
    # Print output for debugging
    print(result)
    
    # Check if the download was successful
    if (!any(grepl("Permission denied|No more authentication methods to try|Connection closed", result))) {
      message("Download successful!")
      return(TRUE)
    }
    
    # If this is the last attempt, throw an error
    if (attempt == max_attempts) {
      stop("Failed to download via SFTP after ", max_attempts, " attempts.")
    }
    
    # Wait before retrying
    Sys.sleep(intervals[min(attempt, length(intervals))])
  }
}



# DOWNLOAD DATA FROM COBRHA SERVER ----

# Define the remote directory
remote_dir <- "/Listing"

# Create a temporary file to store the PSFTP batch commands
batch_file <- tempfile(fileext = ".txt")

# Write the batch commands to the file
writeLines(c(
  paste("cd", remote_dir),
  "ls",  # List files in the directory
  "exit"
), con = batch_file)


# Run the command and capture the output (file listing)
output <- system2("sftp", args = c(
  "-i", private_key,
  "-oBatchMode=yes",
  "-o IdentitiesOnly=yes",
  "-oStrictHostKeyChecking=no",
  "-oHostKeyAlgorithms=+ssh-rsa",
  "-oKexAlgorithms=+diffie-hellman-group-exchange-sha1",
  "-oPubkeyAcceptedAlgorithms=+ssh-rsa",
  "-oMACs=+hmac-sha1",
  sftp_server
), stdin = batch_file, stdout = TRUE, stderr = TRUE)


# Test if the output looks fine; try again if not
# Initialize a counter for retries
retries <- 0
max_retries <- 7

# Loop to check the condition up to 5 times
while (!any(grepl("Connected to sftp.ehealth.fgov.be.", output)) && retries < max_retries) {
  retries <- retries + 1
  # Wait for 5 seconds
  Sys.sleep(5)
  # Re-run the command and update the output
  output <- system2("sftp", args = c(
    "-i", private_key,
    "-oBatchMode=yes",
    "-o IdentitiesOnly=yes",
    "-oStrictHostKeyChecking=no",
    "-oHostKeyAlgorithms=+ssh-rsa",
    "-oKexAlgorithms=+diffie-hellman-group-exchange-sha1",
    "-oPubkeyAcceptedAlgorithms=+ssh-rsa",
    "-oMACs=+hmac-sha1",
    sftp_server
  ), stdin = batch_file, stdout = TRUE, stderr = TRUE)
  message(paste0("Retry ",retries," to get the file list"))
}

# Print the output to check
cat(output, sep = "\n")

# Stop execution if the condition is still not met after 5 retries
if (!any(grepl("Connected to sftp.ehealth.fgov.be.", output))) {
  stop(paste0("Did not manage to get the eHealth file list after ",max_retries+1," tries. Exiting."))
}

# Extract only lines that look like filenames
file_names <- unlist(regmatches(output, gregexpr("HCI_NCCN_\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}\\.zip", output)))

# Sort filenames lexicographically (YYYY-MM-DD_HH-MM-SS ensures correct order)
file_lines_sorted <- sort(file_names, decreasing = TRUE)

# Get the most recent file (first in sorted order)
most_recent_file <- file_lines_sorted[1]

# Print result
print(paste0("File to extract: ",most_recent_file))




# EXTRACT THE DATA FROM THE ZIP ----
# Download the data

# write a new batch file
batch_content <- paste(
  "lcd", shQuote(temporary_folder),
  "\nget", shQuote(paste0(remote_dir,"/",most_recent_file)),
  sep = " "
)
batch_file <- tempfile(fileext = ".txt")
writeLines(batch_content, batch_file)



# Define SFTP options
sftp_options <- c(
  "-i", private_key,
  "-oStrictHostKeyChecking=no",
  "-oHostKeyAlgorithms=+ssh-rsa",
  "-oKexAlgorithms=+diffie-hellman-group-exchange-sha1",
  "-oPubkeyAcceptedAlgorithms=+ssh-rsa",
  "-oMACs=+hmac-sha1",
  "-b", batch_file,  # Batch file with commands
  sftp_server  # Server address must be last
)




# Run the SFTP command

retry_download(sftp_options)

# Clean up the temporary batch file
unlink(batch_file)




# Unzip "most_recent_file" at "temporary_folder"
unzip(paste0(temporary_folder,"/",most_recent_file),exdir = temporary_folder)
csv_file <- paste0(temporary_folder, "/HCI_NCCN.1.csv")

# Load the CSV file ----
### Note: there is an error in the file definition. We manually fix column headers

# Read the file without headers (treat the first line as data)
data <- read.csv(csv_file, sep = ";")

# remove the zip & the csv
unlink(paste0(temporary_folder,"/",most_recent_file))
unlink(csv_file)

# set column names lowercase
data <- data %>% rename_all(tolower)

# set all empty text values as NA
data[data == ""] <- NA

# select duplicates
true_duplicates <- data[duplicated(data) | duplicated(data, fromLast = TRUE), ]


# remove stray spaces
# Trim leading and trailing spaces for all string variables in the data frame
data_cleaned <- data.frame(lapply(data, function(col) {
  if (is.character(col)) {
    trimws(col)
  } else {
    col
  }
}), stringsAsFactors = FALSE)
duplicates_after_removing_stray_spaces <- data[duplicated(data_cleaned) | duplicated(data_cleaned, fromLast = TRUE), ]

# remove duplicates
data_cleaned <- data_cleaned[!duplicated(data_cleaned), ]

# save as csv
#write.csv(duplicates_after_removing_stray_spaces, paste0(temporary_folder, "duplicates_bis.csv"), row.names = FALSE)
#write.csv(true_duplicates, paste0(temporary_folder, "/true_duplicates.csv"), row.names = FALSE)



# add a row number
data_cleaned <- data_cleaned %>% mutate(ad_hoc_id = row_number())

# make a subset of cases that have an empty zip_code in both hco and site
data_empty_zip <- data_cleaned %>% filter((hco_zip_code=="" & site_zip_code=="") | (is.na(hco_zip_code) & is.na(site_zip_code)))
# write to csv
#write.csv(data_empty_zip, paste0(temporary_folder, "/empty_zip.csv"), row.names = FALSE)


#if (nrow(data_empty_zip) > 0) {
# add all possible missing zip codes ----

# this is the query to create a table for the next step. It is not executed here, but merely serves as a backup in case the MV were deleted on the database.
query <- "DROP MATERIALIZED VIEW IF EXISTS ingestion.postal_code_by_muni;

CREATE MATERIALIZED VIEW IF NOT EXISTS ingestion.postal_code_by_muni
TABLESPACE pg_default
AS
WITH names as (SELECT niscode::text as municipality_id, nameger as municipality_name_de, namefre as municipality_name_fr, namedut as municipality_name_nl FROM raw_data.ngi_ign_municipality),

post as (select municipality_id::text as municipality_id, postcode, count (*) from ingestion.best_address_bosa
group by municipality_id,  postcode)

select p.*, n.municipality_name_de, n.municipality_name_fr, n.municipality_name_nl from post p
left join names n
on p.municipality_id=n.municipality_id

WITH DATA;

ALTER TABLE IF EXISTS ingestion.postal_code_by_muni
    OWNER TO pgn_group_data_team_w;

GRANT ALL ON TABLE ingestion.postal_code_by_muni TO pgn_group_data_team_w;
"
con_pg <- get_con()
zipcodes <- dbGetQuery(con_pg, "SELECT * FROM ingestion.postal_code_by_muni")
dbDisconnect(con_pg)


# remove Sint-Niklaas/Saint-Nicolas duplicates
zipcodes <- zipcodes %>% mutate(municipality_name_fr = ifelse(municipality_id == "46021", "", municipality_name_fr))

zip_nl <- zipcodes %>% 
  select(municipality=municipality_name_nl, postcode, municipality_id)
zip_fr <- zipcodes %>% 
  select(municipality=municipality_name_fr, postcode, municipality_id)
zip_de <- zipcodes %>% 
  select(municipality=municipality_name_de, postcode, municipality_id)

zip_all <- bind_rows(zip_nl, zip_fr, zip_de) %>%
  distinct() %>%
  filter(!is.na(municipality) & municipality != "")

nis_all <- bind_rows(zip_nl, zip_fr, zip_de) %>%
  filter(!is.na(municipality) & municipality != "") %>%
  select(municipality_id, municipality) %>%
  distinct()

#}


# TRANSFORM ----
# """""""""""""""""" ----
# Geocode

## Geocoding libraries
library(devtools)
library(phacochr)
phaco_setup_data()
phacochr::phaco_best_data_update()


#data_to_geocode <- data %>% select (ad_hoc_id,hco_street,hco_house_number, hco_zip_code, hco_municipality, site_street, site_house_number, site_zip_code, site_municipality)
# set address with data from site, but if it is missing, use hco
data_to_geocode <- data_cleaned %>% mutate(municipality = ifelse(site_municipality=="", hco_municipality, site_municipality))
data_to_geocode <- data_to_geocode %>% mutate(zip_code = ifelse(site_zip_code=="", hco_zip_code, site_zip_code))
data_to_geocode <- data_to_geocode %>% mutate(street = ifelse(site_street=="", hco_street, site_street))
data_to_geocode <- data_to_geocode %>% mutate(house_number = ifelse(site_house_number=="", hco_house_number, site_house_number))

#if (nrow(data_empty_zip) > 0) {
  # select the ones with missing zip
  data_to_geocode_missing_zip <- data_to_geocode %>% filter(zip_code=="" | is.na(zip_code))
  # join with zip_all
  data_to_geocode_missing_zip <- left_join(data_to_geocode_missing_zip, zip_all, by = c("municipality" = "municipality"), relationship= "many-to-many")
  # set ad_hoc_id_bis to concat ad_hoc_id and row_number
  data_to_geocode_missing_zip <- data_to_geocode_missing_zip %>% mutate(ad_hoc_id_bis = paste0(ad_hoc_id, "_", row_number()))
  # set identifier as missing zip
  data_to_geocode_missing_zip <- data_to_geocode_missing_zip %>% mutate(missing_zip=1)
  # select objects with known zip code
  data_to_geocode_known_zip <- data_to_geocode %>% filter(zip_code!="" & !is.na(zip_code))
  # add ad_hoc_id_bis
  data_to_geocode_known_zip <- data_to_geocode_known_zip %>% mutate(ad_hoc_id_bis = paste0(ad_hoc_id, "_0"))
  # set identifier as not missing zip
  data_to_geocode_known_zip <- data_to_geocode_known_zip %>% mutate(missing_zip=0)
  # add nis code from nis_all
  data_to_geocode_known_zip <- left_join(data_to_geocode_known_zip, nis_all, by = c("municipality" = "municipality"))
  # recreate data_to_geocode from both parts
  data_to_geocode <- bind_rows(data_to_geocode_missing_zip, data_to_geocode_known_zip)
#}

# set zip_code to the postcode value if it is na or missing
data_to_geocode <- data_to_geocode %>% mutate(zip_code = ifelse(is.na(zip_code) | zip_code=="", postcode, zip_code))
 #%>% select(-postcode, -municipality_id)

data_input_geocode <- data_to_geocode %>% select(ad_hoc_id,ad_hoc_id_bis, street, house_number, zip_code, municipality)


data_geocoded <- phaco_geocode(data_to_geocode=t_adresse <- data_input_geocode, colonne_rue= "street", colonne_num="house_number", colonne_code_postal="zip_code")

## change geometry column name and add geocode results to all records
simple_geocode <- data_geocoded$data_geocoded_sf[, c("ad_hoc_id_bis", "cd_munty_refnis")]
data_merged <- left_join(data_to_geocode, simple_geocode, by = "ad_hoc_id_bis")
data_merged <- st_set_geometry(data_merged, "geometry")

# select only data with a non-empty geometry
data_merged <- data_merged %>% filter(!is.na(geometry) & st_is_empty(geometry)==FALSE)

# select only records where the result is in the expected nis area
data_merged <- data_merged %>% filter(municipality_id == cd_munty_refnis)

# select the first occurrence of any ad_hoc_id
data_merged <- data_merged %>% distinct(ad_hoc_id, .keep_all = TRUE)


# add some stops: 
## if geocoded data is much shorter than original data
## if the number of cases dropped significantly from historical runs (revise this if query becomes larger)
if(nrow(data_merged) < nrow(data_cleaned) * 0.8 | nrow(data_merged) < 16000){
  stop("The number of geocoded cases is significantly lower than the original data")
}



# LOAD ----
# """""""""""""""""" ----
# upload to raw data without geocoding
CreateImportTable(dataset = data, schema = "raw_data", table_name = "ehealth_cobrha")  
CreateImportTable(dataset = data_cleaned, schema = "raw_data", table_name = "ehealth_cobrha_deduplicated")

# upload geocoded to raw data
CreateImportTable(dataset = data_merged, schema = "raw_data", table_name = "ehealth_cobrha_geocoded")  
print("Script finished without errors.")
