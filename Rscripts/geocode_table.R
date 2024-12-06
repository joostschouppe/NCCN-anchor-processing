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



get_rows_to_geocode<-function(input_table_name){
  con_pg <- get_con()
  query <- paste0("SELECT * FROM ", input_table_name)
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
phaco_geocode_batch<-function(batch_to_geocode, input_opt_list) {
    args_list <- list(data_to_geocode = batch_to_geocode)
    args_list <- c(args_list, input_opt_list)
    result <- do.call(phaco_geocode, args_list)
    return(result$data_geocoded)
}



run_all<-function(){
    # load the env variables
    input_table_name_env <- Sys.getenv("INPUT_TABLE_NAME")
    input_schema_name_env <- Sys.getenv("INPUT_SCHEMA_NAME")
    output_table_name_env <- Sys.getenv("OUTPUT_TABLE_NAME")
    output_schema_name_env <- Sys.getenv("OUTPUT_SCHEMA_NAME")

    input_opt_list <- list()

    input_colonne_num_env <- Sys.getenv("INPUT_COLUMN_NUM")
    if (!is.null(input_colonne_num_env) && input_colonne_num_env != "") {
        input_opt_list$colonne_num <- input_colonne_num_env
    }

    input_colonne_rue_env <- Sys.getenv("INPUT_COLUMN_STREET")
    if (!is.null(input_colonne_rue_env) && input_colonne_rue_env != "") {
        input_opt_list$colonne_rue <- input_colonne_rue_env
    }

    input_colonne_zipcode_env <- Sys.getenv("INPUT_COLUMN_ZIPCODE")
    if (!is.null(input_colonne_zipcode_env) && input_colonne_zipcode_env != "") {
        input_opt_list$colonne_code_postal <- input_colonne_zipcode_env
    }

    input_colonne_num_rue_env <- Sys.getenv("INPUT_COLUMN_NUM_RUE")
    if (!is.null(input_colonne_num_rue_env) && input_colonne_num_rue_env != "") {
        input_opt_list$colonne_num_rue <- input_colonne_num_rue_env
    }

    input_colonne_rue_code_postal_env <- Sys.getenv("INPUT_COLUMN_RUE_CODE_POSTAL")
    if (!is.null(input_colonne_rue_code_postal_env) && input_colonne_rue_code_postal_env != "") {
        input_opt_list$colonne_rue_code_postal <- input_colonne_rue_code_postal_env
    }

    input_colonne_num_rue_code_postal_env <- Sys.getenv("INPUT_COLUMN_NUM_RUE_CODE_POSTAL")
    if (!is.null(input_colonne_num_rue_code_postal_env) && input_colonne_num_rue_code_postal_env != "") {
        input_opt_list$colonne_num_rue_code_postal <- input_colonne_num_rue_code_postal_env
    }


    number_of_batch_env <- Sys.getenv("NUMBER_OF_BATCH")
    number_of_batch <- as.integer(number_of_batch_env)


    print("Get the address from the database")
    table_name <- paste0(input_schema_name_env,".",input_table_name_env)
    entries <- get_rows_to_geocode(table_name)
    # compute the number of entries
    n_entries <- nrow(entries)
    print(paste0("Number of entries: ", n_entries))


    # split the entries in X parts
    entries_batch <- split(entries, 1:nrow(entries) %% number_of_batch)
    print(paste0("Number of batches: ", length(entries_batch)))


    # create an empty df
    df_geocoded <- data.frame()

    for (i in seq_along(entries_batch)) {
        print(paste0("Batch ", i))
        geocoded_address_batch <- phaco_geocode_batch(entries_batch[[i]], input_opt_list)
        df_geocoded <- rbind(df_geocoded, geocoded_address_batch)
        print(paste0("Batch ", i, " done"))
    }

    print("Write the geocoded address to the database")
    writeTableToCuration(dataset = df_geocoded , schema = output_schema_name_env, table_name = output_table_name_env)

    print("Done -------------------")
}


run_all()
