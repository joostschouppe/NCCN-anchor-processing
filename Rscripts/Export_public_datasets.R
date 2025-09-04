## ---------------------------
##
## Script name: Export needed layers as dataset
##
## Purpose of script: download data from postgres and upload to github for public publishing via the support page
##
## Author: Joost Schouppe
##
## Date Created: 2025-07-07
##
##
## ---------------------------




# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# Datasets to process

subcategory_stringids <- c("schools", "seveso_international")
data_list_stringids <- c("seveso_be")


print("setting up environment")

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

temporary_folder <-Sys.getenv("TEMPORARY_STORAGE")


github_token <- Sys.getenv("GITHUB_TOKEN")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(file.path(rscript_folder,"utils_updated_check_protoanchors.R"))
source(file.path(rscript_folder,"utils.R"))


### Add more libraries ----
library(openxlsx)
library(git2r)

### Connect to PROD db ----

db_name_prod<- Sys.getenv("POSTGRES_DB_NAME_PROD")
db_host_name_prod <- Sys.getenv("POSTGRES_HOST_NAME_PROD")

get_con_prod<-function(){
  con_pg_prod <- dbConnect(Postgres(),
                           user=postgres_user, 
                           password=postgres_password,
                           host=db_host_name_prod,
                           dbname=db_name_prod,
                           port=5432, 
                           sslmode = 'prefer')
  return(con_pg_prod)
}


print("updating on github")

## prepare connection to GitHub


# set branch (by default we push straight to master)
target_branch <- "update-public-data" 


## prepare local location
# create clear a local folder to work in
git_folder <- file.path(temporary_folder, "pgn-data-vectortiles")

# If the folder exists, delete it and everything in it
if (dir.exists(git_folder)) {
  unlink(git_folder, recursive = TRUE, force = TRUE)
}

# Now (re)create the folder
dir.create(git_folder, recursive = TRUE)

# Set up credentials
cred <- cred_user_pass(
  username = "github_user", # in this construction, username is required, but it can be any nonsense
  password = github_token             # token goes in the password field
)

# Clone the repo
repo <- clone(
  url = "https://github.com/NCCN-Paragon/pgn-data-vectortiles",
  local_path = git_folder,
  credentials = cred,
  branch = target_branch
)

# Set the author
config(repo, user.name = "Airflow User", user.email = "paragon-data@nccn.fgov.be")

# Folder to store the data
data_folder <- file.path(git_folder, "public-data")

# EXTRACT -----------------------------------------------------------
#  """""""""""""""""""""""""""""""""""""""" ----------------------

# Generic data
## Load all properties 
con_pg <- get_con()
all_properties <- dbGetQuery(con_pg, paste0("SELECT property_name, data_list_ids, definition FROM reporting.properties WHERE deleted_at IS NULL"))
dbDisconnect(con_pg)


# RAW FUNCTION

download_anchor_sets <- function(
    subcategory_stringids = character(),
    data_list_stringids   = character()
) {
  con <- get_con()      # metadata connection
  
  # ------------------------------------------------------------------
  # 1) Resolve sub‑categories → vector<subcategory_uuid, string_id>
  # ------------------------------------------------------------------
  if (length(subcategory_stringids)) {
    sql_vec <- paste(DBI::dbQuoteString(con, subcategory_stringids), collapse = ",")
    subcat_df <- dbGetQuery(
      con,
      sprintf("SELECT id, string_id
                 FROM parameterization.subcategory
                WHERE string_id IN (%s)", sql_vec)
    )
  } else {
    subcat_df <- data.frame(id = character(), string_id = character())
  }
  
  # ------------------------------------------------------------------
  # 2) For each sub‑category uuid, get ALL data‑list uuids (JSONB ?| )
  #    create a named list  subcat_map[[ 'schools' ]] = c(uuid, uuid, …)
  # ------------------------------------------------------------------
  subcat_map <- list()
  for (i in seq_len(nrow(subcat_df))) {
    sc_uuid <- subcat_df$id[i]
    sc_name <- subcat_df$string_id[i]
    
    dl_vec_sql <- DBI::dbQuoteLiteral(con, sc_uuid)
    q <- sprintf(
      "SELECT id AS data_list_id
         FROM parameterization.data_list
        WHERE subcategory_id ?| ARRAY[%s]", dl_vec_sql)
    uuids <- dbGetQuery(con, q)$data_list_id
    subcat_map[[sc_name]] <- uuids
  }
  
  # ------------------------------------------------------------------
  # 3) Resolve explicit data‑list string_ids → uuids   (one‑to‑one)
  #    store as  dl_map[['seveso']] = 'uuid‑here'
  # ------------------------------------------------------------------
  dl_map <- list()
  if (length(data_list_stringids)) {
    sql_vec <- paste(DBI::dbQuoteString(con, data_list_stringids), collapse = ",")
    dl_df <- dbGetQuery(
      con,
      sprintf("SELECT id AS data_list_id, string_id
                 FROM parameterization.data_list
                WHERE string_id IN (%s)", sql_vec)
    )
    for (i in seq_len(nrow(dl_df))) {
      dl_map[[ dl_df$string_id[i] ]] <- dl_df$data_list_id[i]
    }
  }
  dbDisconnect(con)
  
  # ------------------------------------------------------------------
  # 4) Loop over every task  (sub‑cat group  OR  single data‑list)
  # ------------------------------------------------------------------
  con_d <- get_con_prod()
  
  process_one <- function(out_name, uuid_vector) {
    # ----- pick only property rows whose data_list_ids contain ANY uuid
    prop_rows <- all_properties[
      sapply(all_properties$data_list_ids,
             function(x) any(sapply(uuid_vector, grepl, x = x, fixed = TRUE))),
    ]
    prop_definitions <- prop_rows %>%
      select(property_name, definition)
    
    dyn_cols <- if (nrow(prop_rows) == 0) {
      ""
      # No dynamic properties, so no extra columns
    } else {
      paste0(
        "properties->>'", prop_rows$property_name,
        "' AS ", prop_rows$property_name
      )
    }
    base_cols <- c(
      "*", 
      "name->>'und' AS name_und",
      "name->>'eng' AS name_eng",
      "name->>'dut' AS name_dut",
      "name->>'fre' AS name_fre",
      "name->>'ger' AS name_ger",
      "legend_item->>'und' AS legend_item_und",
      "legend_item->>'eng' AS legend_item_eng",
      "legend_item->>'dut' AS legend_item_dut",
      "legend_item->>'fre' AS legend_item_fre",
      "legend_item->>'ger' AS legend_item_ger",
      "ST_AsText(geometry_pt) AS geometry_pt_wkt",
      "ST_AsText(geometry)     AS geometry_wkt"
    )
    
    full_sql <- sprintf(
      "SELECT %s
         FROM anchor.all_anchors
        WHERE data_list_id IN (%s)",
      paste(c(base_cols, dyn_cols), collapse = ",\n  "),
      paste(DBI::dbQuoteLiteral(con_d, uuid_vector), collapse = ",")
    )
    message("Querying → ", out_name, " (", length(uuid_vector), " uuid)")
    df <- dbGetQuery(con_d, full_sql)
    
    # Convert to sf
    downloaded_dataset <- df %>% 
      select(-geometry, -geometry_pt) %>%
      rename(geometry = geometry_wkt) %>%
      rename(geometry_pt = geometry_pt_wkt) %>%
      st_as_sf(wkt="geometry") %>%
      st_set_crs(4326)
    
    # order by name fields
    downloaded_dataset <- downloaded_dataset %>%
      arrange(name_und, name_eng, name_dut, name_fre, name_ger)
    
    # simplify
    ## removing fields that we don't actually use
    downloaded_dataset <- downloaded_dataset %>%
      filter(is.na(deleted_at)) %>%
      select(-geometry_s,-geometry_pg,-best_address_id, -tags, -capakey_id, -deleted_at, -imported_at, -created_by, -updated_by)
    
    # save column definitions
    write.csv(
      prop_definitions,
      file = file.path(data_folder, paste0(out_name, "_columns.csv")),
      row.names = FALSE
    )
    
    # save an Excel with just the centroid
    excel <- as.data.frame(downloaded_dataset) %>%
      # removing fields that are not relevant in an Excel download
      select(-geometry,-name,-legend_item,-properties,-properties_secondary) %>%
      st_as_sf(wkt="geometry_pt") %>%
      st_set_crs(4326)
    #extract coordinates to x and y column
    excel$x <- st_coordinates(excel$geometry_pt)[,1]
    excel$y <- st_coordinates(excel$geometry_pt)[,2]
    
    excel <- as.data.frame(st_drop_geometry(excel))
    
    write.xlsx(excel, file = file.path(data_folder,paste0(out_name,".xlsx")))
    
    # save as geojson and gpkg
    st_write(downloaded_dataset, file.path(data_folder,paste0(out_name,".gpkg")), delete_dsn = TRUE)
    st_write(downloaded_dataset, file.path(data_folder,paste0(out_name,".geojson")), delete_dsn = TRUE)
    
  }
  
  # a) sub‑categories
  for (nm in names(subcat_map)) {
    process_one(nm, subcat_map[[nm]])
  }
  # b) individual data‑lists
  for (nm in names(dl_map)) {
    process_one(nm, dl_map[[nm]])
  }
  
  dbDisconnect(con_d)
  invisible(NULL)
}

download_anchor_sets(
  subcategory_stringids,
  data_list_stringids
)



# Files larger than 100 MB are not allowed on Github. Here we remove them.

# List all files with full path
files <- list.files(data_folder, full.names = TRUE)

# Get file sizes in MB
sizes_mb <- file.info(files)$size / 1e6

# Find files over 100 MB
too_large <- files[sizes_mb > 100]

# Remove them
if (length(too_large) > 0) {
  file.remove(too_large)
  message("Removed ", length(too_large), " file(s) > 100 MB:")
  print(too_large)
} else {
  message("No files over 100 MB found.")
}




# LOAD ----
# """""""""""""""""" ----

# Push to github
# Commit

status <- status(repo)

has_changes <- length(status$untracked) > 0 | length(status$staged) > 0 | length(status$unstaged) > 0

if (has_changes) {
  add(repo, path = ".")
  commit(
    repo,
    message = paste0("Automated push of Anchor downloadables (", format(Sys.time(), "%Y-%m-%d %H:%M"), ")"),
    all = TRUE
  )
  push(repo,
       name = "origin",
       refspec = paste0("refs/heads/", target_branch),
       credentials = cred)
  local_sha <- commits(repo, n = 1)[[1]]$sha
  fetch(repo, name = "origin", credentials = cred)
  remote_sha <- commits(repo, n = 1, ref = paste0("origin/", target_branch))[[1]]$sha
  if (local_sha == remote_sha) {
    message("Push succeeded: remote is up to date")
  } else {
    stop("Push did not update remote. Are all files smaller than 100 MB?")
  }
} else {
  print("No changes to commit.")
}

print("All done")
