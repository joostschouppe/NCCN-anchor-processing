## ---------------------------
##
## Script name: Export recency info about Anchors
##
## Purpose of script: Combine all_anchors, datalist & subcategory to create metadata about proto-anchors for Paragon-support website
##
## Author: Joost Schouppe
##
## Date Created: 2025-04-14
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

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


library(git2r)

# EXTRACT ----
# """""""""""""""""" ----

print("extracting current website")


# set branch (by default we push straight to master)
target_branch <- "master" 


## prepare local location
# create clear a local folder to work in
git_folder <- file.path(temporary_folder, "nccn-support-site")

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
  url = "https://github.com/NCCN-Paragon/pgn-website",
  local_path = git_folder,
  credentials = cred,
  branch = target_branch
)


print("extract anchor information")

# download stats from all anchors as published right now ----

con_pg <- get_con()
all_anchors_prod <- dbGetQuery(con_pg, "select data_list_id, to_char(greatest(max(updated_at),max(created_at),max(deleted_at)),'YYYY-MM-DD') as latest_change,
	sum(case when deleted_at is not null then 0 else 1 end) as count_existing, count(*) as count_total from import_fdw.prod_all_anchors
	group by data_list_id")
dbDisconnect(con_pg)

# only keep rows where there are still actual anchors
all_anchors_prod <- all_anchors_prod %>%
  filter(count_existing > 0) %>%
  filter(!is.na(data_list_id))

# download the data list ----
con_pg <- get_con()
datalist <- dbGetQuery(con_pg, "select id as data_list_id, string_id as data_list_string_id, source_string_id, subcategory_id
 from parameterization.data_list
 where subcategory_id IS NOT NULL")
dbDisconnect(con_pg)

# download the subcategory ----

con_pg <- get_con()
subcategory <- dbGetQuery(con_pg, "select id as subcategory_id, string_id as subcategory_string_id, name->>'dut' as name_dut, name->>'fre' as name_fre, name->>'ger' as name_ger, name->>'eng' as name_eng, has_support_page from parameterization.subcategory")
dbDisconnect(con_pg)

# TRANSFORM ----
# """""""""""""""""" ----

# Merge content and prepare for conversion to Markdown ----

# count number of NAs in name_dut-name_eng
if (any(is.na(subcategory$name_dut) | 
        is.na(subcategory$name_fre) | 
        is.na(subcategory$name_ger) | 
        is.na(subcategory$name_eng))) {
  stop("Stopping: there were empty values in the subcategory names")
}

# add data list info to the anchors
anchors_dl <- all_anchors_prod %>%
  left_join(datalist, by = "data_list_id") %>%
  filter(!is.na(data_list_string_id))



# create a row for each item in the subcategories list
# Step 1: Convert JSONB strings into list
anchors_dl <- anchors_dl %>%
  mutate(subcategory_id = lapply(subcategory_id, fromJSON))  # Convert JSON strings into list objects

# Step 2: Unnest the list into separate rows
anchors_dl_expanded <- anchors_dl %>%
  unnest(subcategory_id)  # "Blow up" the list into multiple rows

# add the info from the subcategory
anchors_dl_expanded <- anchors_dl_expanded %>%
  left_join(subcategory, by = "subcategory_id") %>%
  rowwise() %>%
  mutate(
    file_slug = str_to_lower(source_string_id),
    file_path = file.path(git_folder, "content", "data", "sources", paste0(file_slug, ".en.md")),
    source_string_id = if (file.exists(file_path)) {
      paste0("[", source_string_id, "]({{% ref \"/data/sources/", file_slug, "\" %}})")
    } else {
      source_string_id
    }
  ) %>%
  ungroup()


# summarize by data list id
anchors_dl_expanded <- anchors_dl_expanded %>%
  group_by(data_list_id) %>%
  summarise(
    latest_change = first(latest_change),  # Keep the first 'latest_change'
    count_existing = first(count_existing),  # Keep the first 'count_existing'
    source_string_id = first(source_string_id),  # Keep the first 'source_string_id'
    name_dut = paste(unique(name_dut), collapse = ", "),  # Concatenate unique 'name_dut'
    name_fre = paste(unique(name_fre), collapse = ", "),  # Concatenate unique 'name_fre'
    name_ger = paste(unique(name_ger), collapse = ", "),  # Concatenate unique 'name_ger'
    name_eng = paste(unique(name_eng), collapse = ", "),  # Concatenate unique 'name_eng'
    has_support_page = first(has_support_page),  # Keep the first 'has_support_page'
    subcategory_string_id = first(subcategory_string_id),  # Keep the first 'layer_string_id'
    .groups = "drop"  # Drop grouping to return a clean dataframe
  )

anchors_dl_expanded <- anchors_dl_expanded %>%
  mutate(
    name_dut = gsub('"', '', name_dut),
    name_fre = gsub('"', '', name_fre),
    name_ger = gsub('"', '', name_ger),
    name_eng = gsub('"', '', name_eng)
  )

# Transform to markdown ----


table_dut <- anchors_dl_expanded %>%
  group_by("Naam (aantal)"=name_dut) %>%
  reframe(
    "Naam (aantal)" = {
      name <- unique(name_dut)
      count <- format(as.numeric(sum(count_existing)), big.mark = ".", decimal.mark = ",", scientific = FALSE)
      has_page <- any(has_support_page, na.rm = TRUE)
      subcat_id <- unique(subcategory_string_id)
      
      if (has_page && !is.na(subcat_id)) {
        paste0("[**", name, "**]({{% ref \"/data/anchor-status/", subcat_id, "\" %}}) (", count, ")")
      } else {
        paste0("**", name, "** (", count, ")")
      }},
    "Laatste aanpassing per bron" = paste(
      unique(paste(latest_change, source_string_id)),
      collapse = "<br>"
    )
  )
# Convert to markdown string
nl <- knitr::kable(table_dut, format = "markdown")


table_fre <- anchors_dl_expanded %>%
  group_by("Nom (numéro)" = name_fre) %>%
  reframe(
    "Nom (numéro)" = {
      name <- unique(name_fre)
      count <- format(as.numeric(sum(count_existing)), big.mark = ".", decimal.mark = ",", scientific = FALSE)
      has_page <- any(has_support_page, na.rm = TRUE)
      subcat_id <- unique(subcategory_string_id)
      
      if (has_page && !is.na(subcat_id)) {
        paste0("[**", name, "**]({{% ref \"/data/anchor-status/", subcat_id, "\" %}}) (", count, ")")
      } else {
        paste0("**", name, "** (", count, ")")
      }},
    "Dernière modification par source" = paste(
      unique(paste(latest_change, source_string_id)),
      
      collapse = "<br>"
    ),
    name_fre = unique(name_fre)
  ) %>%
  slice(stri_order(name_fre, locale = "fr")) %>%
  select(-name_fre)
fr <- knitr::kable(table_fre, format = "markdown")

table_ger <- anchors_dl_expanded %>%
  group_by("Name (Nummer)"=name_ger) %>%
  reframe(
    "Name (Nummer)" = {
      name <- unique(name_ger)
      count <- format(as.numeric(sum(count_existing)), big.mark = ".", decimal.mark = ",", scientific = FALSE)
      has_page <- any(has_support_page, na.rm = TRUE)
      subcat_id <- unique(subcategory_string_id)
      
      if (has_page && !is.na(subcat_id)) {
        paste0("[**", name, "**]({{% ref \"/data/anchor-status/", subcat_id, "\" %}}) (", count, ")")
      } else {
        paste0("**", name, "** (", count, ")")
      }},
    "Letzte Änderung pro Quelle" = paste(
      unique(paste(latest_change, source_string_id)),
      collapse = "<br>"
    )
  )
de <- knitr::kable(table_ger, format = "markdown")

table_eng <- anchors_dl_expanded %>%
  group_by("Name (number)"=name_eng) %>%
  reframe(
    "Name (number)" = {
      name <- unique(name_eng)
      count <- format(as.numeric(sum(count_existing)), big.mark = ".", decimal.mark = ",", scientific = FALSE)
      has_page <- any(has_support_page, na.rm = TRUE)
      subcat_id <- unique(subcategory_string_id)
      
      if (has_page && !is.na(subcat_id)) {
        paste0("[**", name, "**]({{% ref \"/data/anchor-status/", subcat_id, "\" %}}) (", count, ")")
      } else {
        paste0("**", name, "** (", count, ")")
      }},
    "Last change per source" = paste(
      unique(paste(latest_change, source_string_id)),
      collapse = "<br>"
    )
  )
en <- knitr::kable(table_eng, format = "markdown")



# LOAD ----
# """""""""""""""""" ----

print("updating on github")

# Load original MD files from Github ----
## prepare connection to GitHub


# Set the author
config(repo, user.name = "Airflow User", user.email = "paragon-data@nccn.fgov.be")


# Replace table with new content ----
update_anchor_status_table <- function(lang, table) {
  file<-readLines(file.path(git_folder, "content", "data", "anchor-status", paste0("_index.",lang,".md")))
  pattern <- "(?s)(?<=<!-- START-TABLE-ANCHOR-RECENCY -->).*?(?=<!-- END-TABLE-ANCHOR-RECENCY -->)"
  test <- sub(pattern, "TEST IS OK", paste(file, collapse = "\n"), perl = TRUE)
  if (nchar(test)>=sum(nchar(file))) {
    stop(paste0("File in language ",lang," is not updated because the expected HTML comments were not found."))
  }
  file_updated <- sub(pattern, paste("\n",paste(table, collapse="\n"),"\n"), paste(file, collapse = "\n"), perl = TRUE)
  writeLines(file_updated, file.path(git_folder, "content", "data", "anchor-status", paste0("_index.",lang,".md")))
}

update_anchor_status_table("nl", nl)
update_anchor_status_table("fr", fr)
update_anchor_status_table("de", de)
update_anchor_status_table("en", en)

print("Files updated, now making a commit")

# Push to github
# Commit

status <- status(repo)

has_changes <- length(status$staged) > 0 || length(status$unstaged) > 0

if (has_changes) {
  commit(
    repo,
    message = paste0("Automated anchor table update (", format(Sys.time(), "%Y-%m-%d %H:%M"), ")"),
    all = TRUE
  )
  push(repo,
       name = "origin",
       refspec = paste0("refs/heads/", target_branch),
       credentials = cred)
  print("Commit done and pushed to Github")
} else {
  print("No changes to commit.")
}

print("SUCCES! All done")

