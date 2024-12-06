##
## Script name: 
##
## Purpose of script: Import Bosa Best adress and insert inside the POstgresql database (lake)
##
## Author: Cosaert François
##
## Date Created: 2023-10-16
##
##


# Library -----------------------------------------------------------------
# """""""""""""""""" ----------------------

library(httr)
library(utils)
library(DBI)
library(RPostgres)

# Load variable -----------------------------------------------------------
# """""""""""""""""" ----------------------

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

# URL des fichiers ZIP -----------------------------------------------------------------
# """""""""""""""""" ----------------------

url1 <- "https://opendata.bosa.be/download/best/openaddress-bevlg.zip"
url2 <- "https://opendata.bosa.be/download/best/openaddress-bebru.zip"
url3 <- "https://opendata.bosa.be/download/best/openaddress-bewal.zip"

# Fonction pour télécharger et extraire les fichiers CSV -----------------------------------------------------------------
# """""""""""""""""" ----------------------

download_and_extract <- function(url) {
  ## Téléchargement du fichier ZIP -----------------------------------------------------------------
  zip_file <- tempfile()
  GET(url, write_disk(zip_file))
  
  ## Extraction du fichier CSV -----------------------------------------------------------------
  csv_file <- unzip(zip_file, exdir = tempdir(), overwrite = TRUE)
  
  ## Lecture du fichier CSV dans un data frame -----------------------------------------------------------------
  df <- read.csv(csv_file)
  
  ## Suppression du fichier ZIP -----------------------------------------------------------------
  file.remove(zip_file)
  
  return(df)
}
# Téléchargement et extraction des fichiers CSV -----------------------------------------------------------------
# """""""""""""""""" ----------------------

downloadAllBest<-function(){
  best_ndl <- download_and_extract(url1)
  best_bru <- download_and_extract(url2)
  best_wall <- download_and_extract(url3)
  
  best_all<-rbind(best_ndl,best_bru,best_wall)
  
  ## Clean column EPSG. ------------------------------
  
  best_all["31370_x"]<-best_all$EPSG.31370_x
  best_all["31370_y"]<-best_all$EPSG.31370_y
  best_all["4326_lat"]<-best_all$EPSG.4326_lat
  best_all["4326_lon"]<-best_all$EPSG.4326_lon
  best_all$EPSG.31370_x<-NULL
  best_all$EPSG.31370_y<-NULL
  best_all$EPSG.4326_lat<-NULL
  best_all$EPSG.4326_lon<-NULL
  
  ## id column ------------------------------
  best_all$id<- seq_len(nrow(best_all))
  return(best_all)
}



# Function to get a connection to Postgresql ------------------------------
# """""""""""""""""" ----------------------
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



# Function to create a table BestAdress in postgresql ---------------------
# """""""""""""""""" ----------------------
CreateImportTableBestAddressBose<-function(best_all){
  if(exists("best_all")){
    con_pg<-get_con()
    table_id <- DBI::Id(
      schema  = "ingestion",
      table   = "best_address_bosa"
    )
    start<-Sys.time()
    print(paste0("Start :",format(Sys.time(), "%a %b %d %X %Y")))
    print("Import best_all into table best_address_bosa into postgresql")
    dbWriteTable(con_pg, table_id, best_all, overwrite = TRUE)
    
    print("ID primary key")
    query <- "ALTER TABLE ingestion.best_address_bosa ADD PRIMARY KEY (id);"
    dbExecute(con_pg, query)
    print("Column Geom")
    
    ## Creaction of column geom ------------------------------------------------
    query <- "ALTER TABLE ingestion.best_address_bosa ADD COLUMN geom geometry(Point, 4326);"
    dbExecute(con_pg, query)
    print("Index address_id")
    ## Create index on address_id ----------------------------------------
    query <- "CREATE INDEX idx_address_id_bosa ON ingestion.best_address_bosa (address_id);"
    dbExecute(con_pg, query)
    print("update geom from lat/long")
    
    ## Update geom from lat/lon columns ----------------------------------------
    query <- 'UPDATE ingestion.best_address_bosa SET geom = ST_SetSRID(ST_MakePoint("4326_lon", "4326_lat"), 4326);'
    dbExecute(con_pg, query)
    print("index geom")
    ## Create index on geom ----------------------------------------
    query <- "CREATE INDEX idx_spatial_geom_bosa ON ingestion.best_address_bosa USING GIST(geom)"
    dbExecute(con_pg, query)
    print("close con")
    ## Close connection --------------------------------------------------------
    dbDisconnect(con_pg)
    print(paste0("End :",format(Sys.time(), "%a %b %d %X %Y")))
    print(Sys.time()-start)
    
  }else{
    print("Error, best_all not exists, load with downloadAllBest")
  }
}


# Main function -----------------------------------------------------------
# """""""""""""""""" ----------------------

mainFunction=function(){

    best_all<-downloadAllBest()
    CreateImportTableBestAddressBose(best_all=best_all)
  
}

if(F){
  mainFunction()
}