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
subcategory_stringids <- c("schools", "airports", "asylum_detention", "asylum_reception_centres", 
                           "bus_tram_metro_routes", "bus_tram_metro_stops", "emergency_response",
                           "cultural_centre", "embassy", "firestations", "hospitals", "hospital_emergency",
                           "hospital_helipads", "inland_ports", "handicap_care", "locks_dams",
                           "place_of_worship", "power_plants", "jails", "seaports", "mall", "sports", "stations",
                           "town_halls", "wind_generators", "rivers_official", "high_voltage", "hectometer_poles",
                           "accommodation", "nuclear_block", "seveso_international","railway_km_marker","railway_segment", "embassy_abroad"
)
data_list_stringids <- c("seveso_be")
exclude_data_list_stringids <- c("seveso_nrw")


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
library(git2r)
library(writexl)

# Keep the connection alive when the git clone gets a little slow - otherwise some network setting might close the connection without properly terminating it
system2("git", c("config", "--global", "http.lowSpeedLimit", "0"))
system2("git", c("config", "--global", "http.lowSpeedTime", "999"))


### Connect to Curation db ----

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


### Helper functions ----

# Small function to convert string true/false to boolean
as_pg_bool <- function(x) tolower(x) %in% c("true","t")


print("updating on github")

## prepare connection to GitHub ----


# set branch (by default we would like to push straight to master)
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



# Clone the repo (default branch), then checkout or create target branch
repo <- clone(
  url = "https://github.com/NCCN-Paragon/pgn-data-vectortiles",
  local_path = git_folder,
  credentials = cred
)

remote_branches <- sapply(branches(repo, flags = "remote"), function(b) b$name)
if (paste0("origin/", target_branch) %in% remote_branches) {
  checkout(repo, branch = target_branch, create = TRUE,
           tracking = paste0("origin/", target_branch))
  cat("Existing branch", target_branch, "checked out\n")
} else {
  head_commit <- lookup(repo, branch_target(repository_head(repo)))
  branch_create(commit = head_commit, name = target_branch)
  checkout(repo, branch = target_branch)
  push(repo, name = "origin", refspec = paste0("refs/heads/", target_branch), credentials = cred, set_upstream = TRUE)
  cat("New branch", target_branch, "created and checked out\n")
}


# Set the author
git2r::config(repo, user.name = "Airflow User", user.email = "paragon-data@nccn.fgov.be")

# Folder to store the data
data_folder <- file.path(git_folder, "public-data")

# EXTRACT -----------------------------------------------------------
#  """""""""""""""""""""""""""""""""""""""" ----------------------

# Generic data
## Load all properties 
con_pg <- get_con()
all_properties <- dbGetQuery(con_pg, paste0("SELECT property_name, data_list_ids, definition,
      name->>'eng' AS definition_eng,
      name->>'dut' AS definition_dut,
      name->>'fre' AS definition_fre,
      name->>'ger' AS definition_ger FROM reporting.properties WHERE deleted_at IS NULL"))
dbDisconnect(con_pg)

# remove some properties that should not exist
all_properties <- all_properties %>%
  filter(property_name!='id' & property_name!='original_id')



# Function to loop through subcategories and create downloadables for them
download_anchor_sets <- function(
    subcategory_stringids         = character(),
    data_list_stringids           = character(),
    exclude_data_list_stringids   = character()
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
  # 3a) Resolve excluded data‑list string_ids → uuids and filter subcat_map
  # ------------------------------------------------------------------
  if (length(exclude_data_list_stringids)) {
    sql_vec <- paste(DBI::dbQuoteString(con, exclude_data_list_stringids), collapse = ",")
    excl_uuids <- dbGetQuery(
      con,
      sprintf("SELECT id AS data_list_id FROM parameterization.data_list WHERE string_id IN (%s)", sql_vec)
    )$data_list_id
    subcat_map <- lapply(subcat_map, function(uuids) setdiff(uuids, excl_uuids))
  }

  # ------------------------------------------------------------------
  # 3b) Resolve explicit data‑list string_ids → uuids   (one‑to‑one)
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
      select(property_name, definition_eng,definition_dut,definition_fre,definition_ger) %>%
      arrange(tolower(property_name))
    
    dyn_cols <- if (nrow(prop_rows) == 0) {
      character(0)
    } else {
      paste0(
        "properties->>'", prop_rows$property_name,
        "' AS ", prop_rows$property_name
      )
    }
    first_cols <- c(
      "id",
      "original_id",
      "name->>'und' AS name_und",
      "name->>'eng' AS name_eng",
      "name->>'dut' AS name_dut",
      "name->>'fre' AS name_fre",
      "name->>'ger' AS name_ger",
      "legend_item->>'eng' AS legend_item_eng",
      "legend_item->>'dut' AS legend_item_dut",
      "legend_item->>'fre' AS legend_item_fre",
      "legend_item->>'ger' AS legend_item_ger"
    )
    final_columns <- c(
      "legend_item_id",
      "icon",
      "subcategory_id",
      "data_list_id",
      "risk_level",
      "updated_at",
      "created_at",
      "properties",
      "properties_secondary",
      "deleted_at",
      "ST_AsHEXEWKB(geometry_pt) AS geometry_pt",
      "ST_AsHEXEWKB(geometry) AS geometry"
    )
    
    full_sql <- sprintf(
      "SELECT %s
         FROM anchor.all_anchors
        WHERE data_list_id IN (%s)",
      paste(c(first_cols, dyn_cols, final_columns), collapse = ",\n  "),
      paste(DBI::dbQuoteLiteral(con_d, uuid_vector), collapse = ",")
    )
    message("Querying → ", out_name, " (", length(uuid_vector), " uuid)")
    
    df <- dbGetQuery(con_d, full_sql)
    
    # Convert to sf
    downloaded_dataset <- df %>% 
      mutate(
        geometry    = st_as_sfc(structure(geometry,    class = "WKB"), EWKB = TRUE),
        geometry_pt = st_as_sfc(structure(geometry_pt, class = "WKB"), EWKB = TRUE)
      ) %>%
      st_as_sf(sf_column_name = "geometry", crs = 4326) %>%
      mutate(geometry_pt = st_set_crs(geometry_pt, 4326))
    
    # order by name fields
    downloaded_dataset <- downloaded_dataset %>%
      arrange(name_und, name_eng, name_dut, name_fre, name_ger)
    
    # save column definitions
    write.csv(
      prop_definitions,
      file = file.path(data_folder, paste0(out_name, "_columns.csv")),
      row.names = FALSE
    )
    
    # save as geojson with full history
    st_write(downloaded_dataset, file.path(data_folder,paste0(out_name,"_full_history.geojson")), delete_dsn = TRUE)
    
    downloaded_dataset <- downloaded_dataset %>%
      filter(is.na(deleted_at)) %>%
      select(-deleted_at)
    
    # save an Excel with just the centroid, just current data
    excel <- downloaded_dataset %>%
      st_drop_geometry() %>%                       # drops the active "geometry" column
      select(-properties, -properties_secondary) %>%
      st_as_sf(sf_column_name = "geometry_pt")     # no crs= needed, the sfc already carries 4326
    
    excel$x <- st_coordinates(excel)[, 1]
    excel$y <- st_coordinates(excel)[, 2]
    
    excel <- as.data.frame(st_drop_geometry(excel))
    
    write_xlsx(excel, file.path(data_folder,paste0(out_name,".xlsx")))
    
    # save as geojson and gpkg
    #st_write(downloaded_dataset, file.path(data_folder,paste0(out_name,".gpkg")), delete_dsn = TRUE)
    st_write(downloaded_dataset, file.path(data_folder,paste0(out_name,".geojson")), delete_dsn = TRUE)
    
    # write a simplified file for the Seveso website
    if (out_name=="seveso_be") {
      print(paste0("Special case for ",out_name,": this dataset is saved in a simplified format to ", file.path(data_folder,paste0(out_name,"_web.xlsx"))))
      excel <- excel %>%
        select(name_und,address,original_id,federal_id,activity_fre,activity_dut,date_inspection,
               risk_ecotoxic,risk_explosion,risk_fire,risk_toxic,seveso_risk,
               product_ecotoxic=as_pg_bool(product_ecotoxic),
               product_toxic=as_pg_bool(product_toxic),
               product_fire=as_pg_bool(product_fire),
               product_explosion=as_pg_bool(product_explosion),
               product_oxidising=as_pg_bool(product_oxidising),
               updated_at,created_at,x,y)
      write_xlsx(excel, file.path(data_folder,paste0(out_name,"_web.xlsx")))
      downloaded_dataset_before_mutate<<-downloaded_dataset
      downloaded_dataset <<- downloaded_dataset %>%
        mutate(
          risk_ecotoxic_nl=ifelse(as_pg_bool(risk_ecotoxic),"Risico voor het milieu",NA),
          risk_explosion_nl=ifelse(as_pg_bool(risk_explosion),"Risico op explosie",NA),
          risk_fire_nl=ifelse(as_pg_bool(risk_fire),"Risico op brand",NA),
          risk_toxic_nl=ifelse(as_pg_bool(risk_toxic),"Risico op intoxicatie",NA),
          product_ecotoxic_nl=ifelse(as_pg_bool(product_ecotoxic),"Aanwezigheid ecotoxische producten",NA),
          product_toxic_nl=ifelse(as_pg_bool(product_toxic),"Aanwezigheid toxische producten",NA),
          product_fire_nl=ifelse(as_pg_bool(product_fire),"Aanwezigheid brandbare producten",NA),
          product_explosion_nl=ifelse(as_pg_bool(product_explosion),"Aanwezigheid explosieve producten",NA),
          product_oxidising_nl=ifelse(as_pg_bool(product_oxidising),"Aanwezigheid oxiderende producten",NA),
          risk_ecotoxic_fr=ifelse(as_pg_bool(risk_ecotoxic),"Risque pour l'environnement",NA),
          risk_explosion_fr=ifelse(as_pg_bool(risk_explosion),"Risque d'explosion",NA),
          risk_fire_fr=ifelse(as_pg_bool(risk_fire),"Risque d'incendie",NA),
          risk_toxic_fr=ifelse(as_pg_bool(risk_toxic),"Risque d'intoxication",NA),
          product_ecotoxic_fr=ifelse(as_pg_bool(product_ecotoxic),"Présence de produits écotoxiques",NA),
          product_toxic_fr=ifelse(as_pg_bool(product_toxic),"Présence de produits toxiques",NA),
          product_fire_fr=ifelse(as_pg_bool(product_fire),"Présence de produits inflammables",NA),
          product_explosion_fr=ifelse(as_pg_bool(product_explosion),"Présence de produits explosifs",NA),
          product_oxidising_fr=ifelse(as_pg_bool(product_oxidising),"Présence de produits oxydants",NA),
          risk_ecotoxic_de=ifelse(as_pg_bool(risk_ecotoxic),"Risiko für die Umwelt",NA),
          risk_explosion_de=ifelse(as_pg_bool(risk_explosion),"Explosionsgefahr",NA),
          risk_fire_de=ifelse(as_pg_bool(risk_fire),"Brandgefahr",NA),
          risk_toxic_de=ifelse(as_pg_bool(risk_toxic),"Vergiftungsrisiko",NA),
          product_ecotoxic_de=ifelse(as_pg_bool(product_ecotoxic),"Vorhandensein ökotoxischer Produkte",NA),
          product_toxic_de=ifelse(as_pg_bool(product_toxic),"Vorhandensein toxischer Produkte",NA),
          product_fire_de=ifelse(as_pg_bool(product_fire),"Vorhandensein brennbarer Produkte",NA),
          product_explosion_de=ifelse(as_pg_bool(product_explosion),"Vorhandensein explosiver Produkte",NA),
          product_oxidising_de=ifelse(as_pg_bool(product_oxidising),"Vorhandensein oxidierender Produkte",NA),
          risk_ecotoxic_en=ifelse(as_pg_bool(risk_ecotoxic),"Risk to the environment",NA),
          risk_explosion_en=ifelse(as_pg_bool(risk_explosion),"Risk of explosion",NA),
          risk_fire_en=ifelse(as_pg_bool(risk_fire),"Risk of fire",NA),
          risk_toxic_en=ifelse(as_pg_bool(risk_toxic),"Risk of intoxication",NA),
          product_ecotoxic_en=ifelse(as_pg_bool(product_ecotoxic),"Presence of ecotoxic products",NA),
          product_toxic_en=ifelse(as_pg_bool(product_toxic),"Presence of toxic products",NA),
          product_fire_en=ifelse(as_pg_bool(product_fire),"Presence of flammable products",NA),
          product_explosion_en=ifelse(as_pg_bool(product_explosion),"Presence of explosive products",NA),
          product_oxidising_en=ifelse(as_pg_bool(product_oxidising),"Presence of oxidising products",NA))
      st_write(downloaded_dataset, file.path(data_folder,paste0(out_name,"_web.geojson")), delete_dsn = TRUE)
      
      
    }
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
  data_list_stringids,
  exclude_data_list_stringids
)


con_pg <- get_con()
seveso_new <- dbGetQuery(con_pg, paste0("SELECT original_id,
      properties->>'risk_ecotoxic' AS risk_ecotoxic,
      properties->>'risk_toxic' AS risk_toxic,
      properties->>'risk_fire' AS risk_fire,
      properties->>'risk_explosion' AS risk_explosion,
      properties->>'product_ecotoxic' AS product_ecotoxic,
      properties->>'product_toxic' AS product_toxic,
      properties->>'product_fire' AS product_fire,
      properties->>'product_explosion' AS product_explosion,
      properties->>'product_oxidising' AS product_oxidising
      FROM transformation.seveso WHERE deleted_at IS NULL"))
dbDisconnect(con_pg)

seveso_working_set <- downloaded_dataset_before_mutate %>%
  select(-risk_ecotoxic,-risk_explosion,-risk_fire,-risk_toxic) %>%
  left_join(seveso_new, by = "original_id") %>%
  mutate(
    risk_ecotoxic_nl=ifelse(as_pg_bool(risk_ecotoxic),"Risico voor het milieu",""),
    risk_explosion_nl=ifelse(as_pg_bool(risk_explosion),"Risico op explosie",""),
    risk_fire_nl=ifelse(as_pg_bool(risk_fire),"Risico op brand",""),
    risk_toxic_nl=ifelse(as_pg_bool(risk_toxic),"Risico op intoxicatie",""),
    risk_ecotoxic_fr=ifelse(as_pg_bool(risk_ecotoxic),"Risque pour l'environnement",""),
    risk_explosion_fr=ifelse(as_pg_bool(risk_explosion),"Risque d'explosion",""),
    risk_fire_fr=ifelse(as_pg_bool(risk_fire),"Risque d'incendie",""),
    risk_toxic_fr=ifelse(as_pg_bool(risk_toxic),"Risque d'intoxication",""),
    risk_ecotoxic_de=ifelse(as_pg_bool(risk_ecotoxic),"Risiko für die Umwelt",""),
    risk_explosion_de=ifelse(as_pg_bool(risk_explosion),"Explosionsgefahr",""),
    risk_fire_de=ifelse(as_pg_bool(risk_fire),"Brandgefahr",""),
    risk_toxic_de=ifelse(as_pg_bool(risk_toxic),"Vergiftungsrisiko",""),
    risk_ecotoxic_en=ifelse(as_pg_bool(risk_ecotoxic),"Risk to the environment",""),
    risk_explosion_en=ifelse(as_pg_bool(risk_explosion),"Risk of explosion",""),
    risk_fire_en=ifelse(as_pg_bool(risk_fire),"Risk of fire",""),
    risk_toxic_en=ifelse(as_pg_bool(risk_toxic),"Risk of intoxication",""),
    ecotoxic=ifelse(as_pg_bool(product_ecotoxic),"{{https://nccn-paragon.github.io/pgn-data-vectortiles/icon/seveso/ecotoxicrelease-1.png}}",""),
    toxic=ifelse(as_pg_bool(product_toxic),"{{https://nccn-paragon.github.io/pgn-data-vectortiles/icon/seveso/toxicrelease-1.png}}",""),
    fire=ifelse(as_pg_bool(product_fire),"{{https://nccn-paragon.github.io/pgn-data-vectortiles/icon/seveso/fire-1.png}}",""),
    explosion=ifelse(as_pg_bool(product_explosion),"{{https://nccn-paragon.github.io/pgn-data-vectortiles/icon/seveso/explosion-1.png}}",""),
    oxidising=ifelse(as_pg_bool(product_oxidising),"{{https://nccn-paragon.github.io/pgn-data-vectortiles/icon/seveso/oxidising-1.png}}",""),
    product_ecotoxic=ifelse(as_pg_bool(product_ecotoxic),1,0),
    product_toxic=ifelse(as_pg_bool(product_toxic),1,0),
    product_fire=ifelse(as_pg_bool(product_fire),1,0),
    product_explosion=ifelse(as_pg_bool(product_explosion),1,0),
    product_oxidising=ifelse(as_pg_bool(product_oxidising),1,0)
    ) %>%
  rowwise() %>%
  mutate(
    risks_combined_nl = paste(
      Filter(function(x) x != "", c(risk_ecotoxic_nl, risk_explosion_nl, risk_fire_nl, risk_toxic_nl)),
      collapse = "\n"
    ),
    risks_combined_fr = paste(
      Filter(function(x) x != "", c(risk_ecotoxic_fr, risk_explosion_fr, risk_fire_fr, risk_toxic_fr)),
      collapse = "\n"
    ),
    risks_combined_de = paste(
      Filter(function(x) x != "", c(risk_ecotoxic_de, risk_explosion_de, risk_fire_de, risk_toxic_de)),
      collapse = "\n"
    ),
    risks_combined_en = paste(
      Filter(function(x) x != "", c(risk_ecotoxic_en, risk_explosion_en, risk_fire_en, risk_toxic_en)),
      collapse = "\n"
    ),
    products = paste(
      Filter(function(x) x != "", c(ecotoxic, explosion, fire, toxic, oxidising)),
      collapse = " "
    )
  ) %>%
  ungroup() %>%
  select(original_id, name_und, activity_dut, activity_fre, address, federal_id, date_inspection, risk_level, 
         risks_combined_nl, risks_combined_fr, risks_combined_de, risks_combined_en, products, geometry
         )
st_write(seveso_working_set, file.path(data_folder,"seveso_web.geojson"), delete_dsn = TRUE)




# Files larger than 100 MB are not allowed on Github. Here we remove them.

# List all files with full path
files <- list.files(data_folder, full.names = TRUE)

# Get file sizes in MB
sizes_mb <- file.info(files)$size / 1e6

# Find files over 100 MB
too_large <- files[sizes_mb > 100]
if (length(too_large) > 0) {
  message("Found ", length(too_large), " files larger than 100 MB. Make sure the following geojson links in the support pages reference a zip file (unless that is also deleted!)")
}

zip_paths <- character()

for (f in too_large) {
  zipfile <- paste0(f, ".zip")   # e.g. "schools.geojson.zip"
  zip(zipfile, files = f, flags = "-j")  # -j prevents storing folder structure
  zip_paths <- c(zip_paths, zipfile)
}

if (length(zip_paths) > 0) {
  zip_sizes <- file.info(zip_paths)$size / 1e6
  zip_too_large <- zip_paths[zip_sizes > 100]
  if (length(zip_too_large) > 0) {
    stop("Found ", length(zip_too_large), " ZIP files larger than 100 MB.")
  }
}

# remove any file larger than 100 MB
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
  git2r::fetch(repo, name = "origin", credentials = cred)
  remote_sha <- commits(repo, n = 1, ref = paste0("origin/", target_branch))[[1]]$sha
  if (local_sha == remote_sha) {
    message("Push succeeded: remote is up to date")
  } else {
    stop("Push did not update remote. Are all files smaller than 100 MB?")
  }
} else {
  print("No changes to commit.")
}

# Clean up local copy
if (dir.exists(git_folder)) {
  unlink(git_folder, recursive = TRUE, force = TRUE)
}

print(paste0("All done. However, you must still do a PR for the newly created ",target_branch," branch"))
