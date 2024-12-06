# Libraries ---------------------------------------------------------------

library(httr)
library(jsonlite)
library(dplyr)
library(rvest)
library(DBI)
library(RPostgres)

# Temporary fix to get version phacochr: 0.9.1.14
devtools::install_github("phacochr/phacochr")

library(phacochr)

cat("version phacochr:", as.character(packageVersion("phacochr")), "\n")

# update the best address data
phacochr::phaco_best_data_update()

# Load the utils.R file
source(paste0(Sys.getenv("LOCAL_RSCRIPT_PATH"),'/utils.R'))


# Database functions ------------------------------------------------------
db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name <- Sys.getenv("POSTGRES_DB_NAME")


get_kbo_address<-function(){
  con_pg <- get_con()
  query <- "SELECT * FROM raw_data.kbo_address"
  df <- dbGetQuery(con_pg, query)
  dbDisconnect(con_pg)
  return(df)
}


writeTableToCuration<-function(dataset, schema, table_name){
    con_pg <- get_con();
    table_id <- DBI::Id(
      schema  = schema,
      table   = table_name
    );
    table_id_t <- paste0(schema,".",table_name)
    start<-Sys.time()
    print(paste0("Start :",format(Sys.time(), "%a %b %d %X %Y")))
    # The table should not exist yet, but if it does (usually because the script is run more than once) we can drop it
    query_drop_table <- paste(
      "DROP TABLE IF EXISTS ", table_id_t, " CASCADE;"
    )
    dbExecute(con_pg, query_drop_table)
    print(paste0("Table ", table_id_t, " dropped (if existed)"))

    print(paste0("Insert new data into postgresql table ", table_id_t))
    dbWriteTable(con_pg, table_id, value = dataset, overwrite = TRUE, row.names = FALSE )

    dbDisconnect(con_pg)
}


# GeoCode functions for a batch of addresses --------------------------------
phaco_geocode_batch<-function(batch_to_geocode){

    # Geocode the address for the first batch
    result <- phaco_geocode(
        data_to_geocode = batch_to_geocode,
        colonne_num = "HouseNumber",
        colonne_rue = "StreetFR",
        colonne_code_postal = "Zipcode")

    return(result$data_geocoded)
}



run_all<-function(){
    print("Get the address from the database")
    entries <- get_kbo_address()
    # compute the number of entries
    n_entries <- nrow(entries)
    print(paste0("Number of entries: ", n_entries))


    # split the entries in 6 parts
    entries_batch <- split(entries, 1:nrow(entries) %% 6)
    print(paste0("Number of batches: ", length(entries_batch)))


    # create an empty df
    df_geocoded <- data.frame()

    for (i in seq_along(entries_batch)) {
        print(paste0("Batch ", i))
        geocoded_address_batch <- phaco_geocode_batch(entries_batch[[i]])
        df_geocoded <- rbind(df_geocoded, geocoded_address_batch)
        print(paste0("Batch ", i, " done"))
    }

    print("Write the geocoded address to the database")
    writeTableToCuration(dataset = df_geocoded , schema = "raw_data", table_name = "kbo_address_geocoded")

    print("Done -------------------")
}


run_all()
