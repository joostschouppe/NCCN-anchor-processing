#
# Script name: Data ingestion "Sociale Kaart" API 
#
# Script purpose: Get nursing homes and hospitals for Flanders 
#
# Author: Janssens Ric
#
# Date: 2023-10-10
#


# Documentation -----------------------------------------------------------

# https://dev.azure.com/NCCN-Paragon/Paragon/_wiki/wikis/Paragon.wiki/428/Source-Sociale-Kaart
# https://www.desocialekaart.be/api/cddc4568-4104-414f-ba98-68742e5446d7/swagger-ui/index.html?configUrl=/api/cddc4568-4104-414f-ba98-68742e5446d7/swagger-config


# Libraries ---------------------------------------------------------------

library(httr)
library(jsonlite)
library(dplyr)
library(rvest)
library(DBI)
library(RPostgres)


# Database functions ------------------------------------------------------

source(paste0(Sys.getenv("LOCAL_RSCRIPT_PATH"),'/utils.R'))
run_status <- Sys.getenv("RUN_STATUS")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name <- "curated_dev_playground"

# Construct URL -----------------------------------------------------------

getHeader<-function(query){
  
  endpoint = "https://www.desocialekaart.be/api"
  
  response <- GET(url = paste0(endpoint, "/generic-list/rubric?query=", query))
  json_val <- content(response)
  
  # We zoeken eerst uit onder welke rubriek de woonzorgcentra zitten
  rubriek_naam <- json_val[["content"]][[1]][["name"]] #14.04.01.%20Woonzorgcentra%20%28WZC%29 & 02.06.01.%20Ziekenhuizen
  rubriek_url <- URLencode(rubriek_naam)
  
  query_url <- paste0(endpoint, "/health-offers?rubrics=", rubriek_url)
  
  return(query_url)
  
}


# Loop through pages and content ------------------------------------------

getPagesAndContent<-function(query_url){
  
  response <- GET(url = query_url)
  json_val <- content(response)
  total_pages <- json_val$page$totalPages
  
  df <- data.frame()
  for (page in 1:total_pages){
    
    print(page)
    
    # pagenumber is zero-based. totalPages is one-based
    page_url = page - 1
    next_page <- paste0(query_url, "&page=", page_url)
    response <- GET(url = next_page)
    
    content <- fromJSON(rawToChar(response$content))$content 
    content <- flatten(content, recursive = TRUE)
    
    df <- rbind(df, content)
  }
  return(df)
  
}


# Define columns & unnest json --------------------------------------------

transformTable<-function(df){
  
  columns <- c(
    "id"="id.id",
    "authentic_source"="id.authenticSource",
    "legal_name"="legalName.description",
    "addresses"="addresses",
    "activities"="activities",
    "type"="type",
    "state"="state",
    "verified"="verified",
    "contactinfo_phones"="contactInfo.phones",
    "contactinfo_emails"="contactInfo.emails",
    "contactinfo_websites"="contactInfo.websites",
    "links"="links"
  )
  
  raw_data <- df %>%
    rowwise %>%
    mutate(addresses = toJSON(addresses, auto_unbox = TRUE)) %>%
    mutate(activities = toJSON(activities, auto_unbox = TRUE)) %>%
    mutate(contactInfo.phones = toJSON(contactInfo.phones, auto_unbox = TRUE)) %>%
    mutate(contactInfo.emails = toJSON(contactInfo.emails, auto_unbox = TRUE)) %>%
    mutate(contactInfo.websites = toJSON(contactInfo.websites, auto_unbox = TRUE)) %>%
    mutate(links = toJSON(links, auto_unbox = TRUE)) %>%
    select_at(columns)
  
  return(raw_data)
  
}


# Export to curation database ---------------------------------------------

writeTableToCuration<-function(raw_data, schema_name, table_name){
  
  con_pg <- get_con()
  dbWriteTable(con_pg, Id(schema = schema_name, table = table_name),
               value = raw_data, overwrite = T)
  
  alter_column_data_type_to_json(con_pg, schema_name, table_name, "addresses")
  alter_column_data_type_to_json(con_pg, schema_name, table_name, "activities")
  alter_column_data_type_to_json(con_pg, schema_name, table_name, "contactinfo_phones")
  alter_column_data_type_to_json(con_pg, schema_name, table_name, "contactinfo_emails")
  alter_column_data_type_to_json(con_pg, schema_name, table_name, "contactinfo_websites")
  alter_column_data_type_to_json(con_pg, schema_name, table_name, "links")
  # id is not valid uuid
  
  dbDisconnect(con_pg)
  
}


# Main functions ----------------------------------------------------------

run_ETL_nursinghomes<-function(){
  
  url <- getHeader(query = "woonzorgcentra")
  df_tmp <- getPagesAndContent(query_url = url)
  nursinghomes <- transformTable(df = df_tmp)
  writeTableToCuration(raw_data = nursinghomes, schema_name = "raw_data", table_name = "vla_depzorg_socialekaart_nursinghomes")
  
}

run_ETL_hospitals<-function(){
  
  url <- getHeader(query = "ziekenhuizen")
  df_tmp <- getPagesAndContent(query_url = url)
  hospitals <- transformTable(df = df_tmp)
  writeTableToCuration(raw_data = hospitals, schema_name = "raw_data", table_name = "vla_depzorg_socialekaart_hospitals")
  
}

run_all<-function(){
  run_ETL_nursinghomes()
  run_ETL_hospitals()
}
if(run_status){
  run_all()
}

