## ---------------------------
##
## Script name: Check some websites for updates of datasets we need to process
##
## Purpose of script: Check if a new dataset is available, potentially compare content of two PDFs
##
## Author: Joost Schouppe
##
## Date Created: 2026-01-14
##
##
## ---------------------------

# Add libraries ----

library(rvest)
library(dplyr)

# Define variables ----

### Define the URLs for firestations ----
webpage_url <- "https://civieleveiligheid.be/nl/zonewerking"

# The current PDF url (should be listed on the "zonewerking" page)
pdf_url <- "https://civieleveiligheid.be/sites/default/files/explorer/partie_s_incendie/fiche-hulpverleningsposten-postes-des-zones-de-secours-07_2026.sx_.pdf"
# The previously published PDF url (the one most recently removed from the "zonewerking" page)
old_pdf_url <- "https://civieleveiligheid.be/sites/default/files/explorer/partie_s_incendie/fiche-hulpverleningsposten-postes-des-zones-de-secours-05_2026.sx_.pdf"
# When the script stops with the Error, that means it's time to find a new URL on the zonewerking page. Put the new url in "pdf_url" and put the url that was in pdf_url in the old_pdf_url variable.
# For now: keep a local copy at C:\temp\civieleveiligheid-firestations\, because they do not keep the old versions online anymore. In hopes of there becoming an Excel available soon, it seems unpractical to adjust the script to work with local copies now

### Define the URLS for statbel ----

statbel_webpage <- "https://statbel.fgov.be/nl/themas/datalab/grid-met-cellen-van-variabele-grootte#figures"
statbel_old_url <- "/nl/open-data/datalab-grid-van-de-bevolking-met-cellen-van-variabele-grootte-2025"
statbel_new_url <- "/nl/open-data/datalab-grid-van-de-bevolking-met-cellen-van-variabele-grootte-2026"


# Check firestation PDF ----
### Check if last known link is still mentioned on CivieleVeiligheid.be ----
# Read the webpage
page <- read_html(webpage_url)

# Extract all links from the page
all_links <- page %>%
  html_nodes("a") %>%
  html_attr("href")

# Check if the PDF URL is in the links
if (pdf_url %in% all_links) {
  message("✓ PDF link found on the civiele veiligheid webpage: that means there is likely no updated PDF with firestation updates")
} else {
  stop("ERROR: The PDF URL is no longer referenced on the page! Visit the website and update the script with the new URL. For the actual analysis, run the script locally.")
}
# This means the script will be considered "successfully run" if the page was not updated, and throw an error when it is.
# When it throws an error, it's time to update the URLs and run the part below to check what actually changed.


# Check the Statbel population grid page for updates ----

page_statbel <- read_html(statbel_webpage)

# Extract all links from the page
all_links_statbel <- page_statbel %>%
  html_nodes("a") %>%
  html_attr("href")


# Check if the PDF URL is in the links
if (statbel_old_url %in% all_links_statbel & !(statbel_new_url %in% all_links_statbel)  ) {
  message("✓ Old population grid still mentioned on statbel, but no new dataset mentioned yet")
  } else if (!(statbel_new_url %in% all_links_statbel)) {
  stop("Old population grid not mentioned anymore on statbel, maybe the URLs changed. Please check & update URLs in the script")
  } else {
  stop("It looks like the new statbel population grid data has been published. Time to run Import_population_grid.R script & update the map layer ")
}



# end statbel

# Read the firestation PDF & create structured table ----

# Commented out these libraries, because we expect this to be run locally by hand. If you want to run it on Airflow, first install pdftools to the machine.
#library(pdftools)
#library(stringr)

# Function to extract text from PDF
extract_pdf_text <- function(pdf_path) {
  text <- pdf_text(pdf_path)
  full_text <- paste(text, collapse = "\n")
  return(full_text)
}

# Function to parse Belgian fire station data
parse_fire_stations <- function(pdf_text) {
  
  # Split into lines
  lines <- str_split(pdf_text, "\n")[[1]]
  
  # Remove empty lines and trim whitespace
  lines <- trimws(lines)
  lines <- lines[lines != ""]
  
  # Combine lines that were split (lines ending with - or that don't have commander info)
  combined_lines <- c()
  i <- 1
  while (i <= length(lines)) {
    line <- lines[i]
    
    # Check if this line contains a zone pattern but no commander (incomplete)
    if (str_detect(line, "Brandweer|Hulpverleningszone|HVZ|Zone") && 
        !str_detect(line, regex("Zonecommandant|Commandant de zone|Cdt de zone|Officier", ignore_case = TRUE))) {
      # Look at next line to see if it completes the zone header
      if (i < length(lines)) {
        next_line <- lines[i + 1]
        # Only combine if next line has the commander indicator
        if (str_detect(next_line, regex("Zonecommandant|Commandant de zone|Cdt de zone|Officier", ignore_case = TRUE))) {
          line <- paste(line, next_line)
          i <- i + 1  # Skip the next line since we combined it
        }
      }
    }
    
    combined_lines <- c(combined_lines, line)
    i <- i + 1
  }
  
  lines <- combined_lines
  
  # Initialize results
  results <- list()
  current_zone <- NA
  
  # Pattern to match zip codes (4 digits at start of line)
  zip_pattern <- "^\\d{4}\\s+"
  
  for (line in lines) {
    
    # Skip page headers/footers and regional headers
    if (str_detect(line, "Hulpverleningsposten en posten van Brandweer Brussel") ||
        str_detect(line, "Postes des zones de secours et des Pompiers de Bruxelles") ||
        str_detect(line, "Adressen hulpverleningsposten") ||
        str_detect(line, "Adresses des postes de secours") ||
        str_detect(line, "^\\d+\\s*/\\s*\\d+$")) {  # Page numbers like "1 / 17"
      next
    }
    
    # Check if this is a zone header line by looking for commander indicators
    # Any line with commander titles is a zone header
    if (str_detect(line, regex("Zonecommandant|Commandant de zone|Cdt de zone|Officier chef de service|Officier-dienstchef", ignore_case = TRUE))) {
      
      # Extract zone name (everything before the commander title)
      current_zone <- str_extract(line, regex(".*(?=\\s+Zonecommandant|\\s+Commandant de zone|\\s+Cdt de zone|\\s+Officier)", ignore_case = TRUE))
      current_zone <- trimws(current_zone)
      next
    }
    
    # Skip subsection headers that don't indicate new zones
    if (str_detect(line, "^(SECTEUR|District|Groupement|Nombre de postes)")) {
      next
    }
    
    # Check if this line starts with a zip code
    if (str_detect(line, zip_pattern)) {
      
      # Split by multiple spaces (2 or more) to separate columns
      parts <- str_split(line, "\\s{2,}")[[1]]
      parts <- trimws(parts)
      parts <- parts[parts != ""]
      
      if (length(parts) >= 3) {
        zip <- parts[1]
        location <- parts[2]
        address <- parts[3]
      } else if (length(parts) == 2) {
        zip <- parts[1]
        location <- parts[2]
        address <- NA
      } else {
        next  # Skip malformed lines
      }
      
      # Add to results
      results[[length(results) + 1]] <- list(
        zone = current_zone,
        zip = zip,
        name = location,
        address = address
      )
    }
  }
  
  # Convert to dataframe
  df <- do.call(rbind, lapply(results, as.data.frame, stringsAsFactors = FALSE))
  
  return(df)
}

# Main function to process the PDF
process_fire_stations_pdf <- function(pdf_path) {
  
  cat("Extracting text from PDF...\n")
  pdf_text <- extract_pdf_text(pdf_path)
  
  cat("Parsing fire station data...\n")
  df <- parse_fire_stations(pdf_text)
  
  cat(sprintf("Found %d fire stations\n", nrow(df)))
  
  return(df)
}

### load data ----

load_data <- function() {
  firestations_new <<- process_fire_stations_pdf(pdf_url)
  firestations_old <<- process_fire_stations_pdf(old_pdf_url)
}

### Analyse results ----

analyse_results <- function() {
  
  # compare versions ----
  # compare number of records total
  
  if (nrow(firestations_old)==nrow(firestations_new)) {
    message("✓ The number of rows did not change (but that doesn't mean nothing changed!)")
  } else {
    message(paste0("The old data had ",nrow(firestations_old)," rows, the new data has ", nrow(firestations_new)))
  }
  
  firestations_new$is_new=1
  firestations_old$is_old=1
  
  firestations_merged <- firestations_new %>%
    full_join(firestations_old, by = c("zip", "name"))
  
  # remove boring cases
  firestations_merged <- firestations_merged %>%
    filter(!(!is.na(is_new) & !is.na(is_old) & zone.x==zone.y & address.x==address.y))
  
  # show all imperfect matches (both old & new)
  firestations_merged <- firestations_merged %>%
    mutate(
      status = case_when(
        is_new == 1 & is.na(is_old) ~ "new object",
        is_old == 1 & is.na(is_new) ~ "removed object",
        is_new == 1 & is_old == 1   ~ "updated object",
        TRUE                       ~ NA_character_
      )
    )
  print(table(firestations_merged$status))
  
  
  firestations_merged <- firestations_merged %>%
    mutate(
      address_changed = if_else(address.x==address.y,0,1),
      zone_changed = if_else(zone.x==zone.y,0,1))
  
  print(firestations_merged %>% count(address_changed, zone_changed))
  
  firestations_merged <<- firestations_merged
  print("Now review the records in firestations_merged")
}

# Main function


main_function = function() {
  load_data()
  analyse_results()
}
# Compare the information in the resulting dataset with the currently mapped objects in OSM. Some deletions/additions might be needed! Refer to https://wiki.openstreetmap.org/wiki/WikiProject_Belgium/Firestations for tagging guidelines.