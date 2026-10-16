## ---------------------------
##
## Script name: Export info about Anchors properties
##
## Purpose of script: Combine all_anchors, datalist & subcategory to create the property information about proto-anchors for Paragon-support website
##
## Author: Joost Schouppe
##
## Date Created: 2025-10-28
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

### Add more libraries ----
library(git2r)
# Keep connection alive when the git clone gets a little slow - otherwise some network setting might close to connection without properly terminating it
system2("git", c("config", "--global", "http.lowSpeedLimit", "0"))
system2("git", c("config", "--global", "http.lowSpeedTime", "999"))


# EXTRACT ----
# """""""""""""""""" ----


# set up GIT environment ----
print("extracting current website")


## prepare local location
# clear and create a local folder to work in
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
  credentials = cred
)

# get the default branch
target_branch <- repository_head(repo)$name


# Set the author
git2r::config(repo, user.name = "Airflow User", user.email = "paragon-data@nccn.fgov.be")


print("extract anchor property information")


# Get the list of unique subcategories that are documented ----

## Load all properties 
con_pg <- get_con()
properties <- dbGetQuery(con_pg, paste0("with properties as (SELECT 
    property_name,
    jsonb_array_elements_text(data_list_ids::jsonb)::uuid AS data_list_id,
	definition,
    description->>'eng' AS definition_eng,
    description->>'dut' AS definition_dut,
    description->>'fre' AS definition_fre,
    description->>'ger' AS definition_ger
FROM reporting.properties 
WHERE deleted_at IS NULL AND property_name != 'id' AND property_name != 'original_id'),

subcat as (
select p.*, jsonb_array_elements_text(dl.subcategory_id)::uuid as subcategory_id from properties p
left join parameterization.data_list dl on p.data_list_id=dl.id)

select s.string_id, c.property_name, c.definition, c.definition_eng, c.definition_fre, c.definition_dut, c.definition_ger from subcat c
left join parameterization.subcategory s on s.id=c.subcategory_id
group by string_id, property_name, definition, definition_eng, definition_fre, definition_dut, definition_ger
order by string_id"))
dbDisconnect(con_pg)



# For each subcategory get the relevant markdown file and replace the existing table with the current one ----

# Get all unique string_ids
unique_subjects <- properties %>%
  filter(!is.na(string_id)) %>%
  dplyr::pull(string_id) %>%
  unique()

# Replace existing attribute table with new content ----
update_anchor_property_table <- function(lang, table, subject) {
  filename <- file.path(git_folder, "content", "data", "anchor-status", paste0(subject, ".", lang, ".md"))
  
  # Check if file exists
  if (!file.exists(filename)) {
    print(paste0("File for ", subject, " in language ", lang, " does not exist. Skipping."))
    return(invisible(NULL))
  }
  
  file <- readLines(filename)
  pattern <- "(?s)(?<=\\[//\\]: # \\(START PROPERTIES\\)).*?(?=\\[//\\]: # \\(END PROPERTIES\\))"
  test <- sub(pattern, "TEST IS OK", paste(file, collapse = "\n"), perl = TRUE)
  
  if (nchar(test) >= sum(nchar(file))) {
    print(paste0("File for ", subject, " in language ", lang, " is not updated because the expected MD comments were not found."))
    return(invisible(NULL))
  }
  
  file_updated <- sub(pattern, paste("\n", paste(table, collapse = "\n"), "\n\n"), paste(file, collapse = "\n"), perl = TRUE)
  writeLines(file_updated, filename)
  print(paste0("✓ Updated ", subject, ".", lang, ".md"))
}

# Loop through all subjects
for (subject in unique_subjects) {
  # Create tables for each language
  table_dut <- properties %>%
    filter(string_id == subject) %>%
    select("Eigenschap" = property_name, "Definitie" = definition_dut)
  nl <- knitr::kable(table_dut, format = "markdown")
  
  table_fre <- properties %>%
    filter(string_id == subject) %>%
    select("Attribut" = property_name, "Définition" = definition_fre)
  fr <- knitr::kable(table_fre, format = "markdown")
  
  table_ger <- properties %>%
    filter(string_id == subject) %>%
    select("Attribut" = property_name, "Definition" = definition_ger)
  de <- knitr::kable(table_ger, format = "markdown")
  
  table_eng <- properties %>%
    filter(string_id == subject) %>%
    select("Attribute" = property_name, "Definition" = definition_eng)
  en <- knitr::kable(table_eng, format = "markdown")
  
  # Update all language versions
  update_anchor_property_table("nl", nl, subject)
  update_anchor_property_table("fr", fr, subject)
  update_anchor_property_table("de", de, subject)
  update_anchor_property_table("en", en, subject)
}

print("Files updated, now making a commit")


# LOAD ----
# """""""""""""""""" ----

print("updating on github")

# Load original MD files from Github ----
## prepare connection to GitHub



# Push to github
# Commit

status <- status(repo)
print(status(repo))
has_changes <- length(status$staged) > 0 || length(status$unstaged) > 0

if (has_changes) {
  commit(
    repo,
    message = paste0("Automated anchor attributes table update (", format(Sys.time(), "%Y-%m-%d %H:%M"), ")"),
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

# Clean up local copy
if (dir.exists(git_folder)) {
  unlink(git_folder, recursive = TRUE, force = TRUE)
}

print("SUCCESS! All done")
