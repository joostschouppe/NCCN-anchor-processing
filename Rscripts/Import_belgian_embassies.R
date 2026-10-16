## ---------------------------
##
## Script name: Import Belgian embassies and consulates abroad
##
## Purpose of script: Download the list of Belgian diplomatic institutions abroad
##                     from the FPS Foreign Affairs public API and load them into
##                     the raw_data schema.
##

##
## Author: Joost Schouppe
##
## Date Created: 2026-08-17
##
## ---------------------------


# Libraries -----------------------------------------------------------------
# """""""""""""""""" ----------------------
library(countrycode)
library(tidygeocoder)
options(tidygeocoder.progress_bar = FALSE)
options(tidygeocoder.quiet = TRUE)

# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# connection details
db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name <- Sys.getenv("POSTGRES_DB_NAME_CURATED")

# Load external IDs
legend_item_id_be <- "448ab4fe-a17f-4b66-89b4-83b6f4126fa2" #includes cases where VL is in same office
legend_item_id_vl <- "17407431-caec-4f9e-a938-2618292e307c" #applied only where VL is alone
data_list_id_be <- "8f222212-f0c2-4ce3-991d-d530ba33ed71"
data_list_id_vl <- "12fbe86c-928e-4227-a4f3-c8ba3f2f9673"

# language to request from the API: 1 = NL, 2 = EN, 3 = FR, 4 = DE
api_language <- 2

### Load external functions ------

# run status
run_status<-Sys.getenv("RUN_STATUS")
## this is set to false and prevents any accidental changes to the database by switching off the main_function(). On Airflow, this is set to true.
run_status<-ifelse(tolower(run_status) == "true", TRUE, FALSE)

# overrule the checks
overrule_checks<-Sys.getenv("OVERRULE_CHECKS")
## Set to FALSE by default. That means we do not update the anchors if some tests fail. Those tests include "the data has grown or shrunk by a lot of objects". If, after review of the log, you decide that nothing is wrong, set this manually to TRUE.
# If the input is not correctly understood as boolean, this will force it to it.
overrule_checks<-ifelse(tolower(overrule_checks) == "true", TRUE, FALSE)

# Do not run the main part of the processing, but just do an update based on the ingestion table already in the dbase
reuse_ingestion_data<-Sys.getenv("REUSE_INGESTION_DATA")
reuse_ingestion_data<-ifelse(tolower(reuse_ingestion_data) == "true", TRUE, FALSE)

# Only run the comparison script & update the ingestion table, but do not attempt to update the transformation table
do_dry_run<-Sys.getenv("DO_DRY_RUN")
do_dry_run<-ifelse(tolower(do_dry_run) == "true", TRUE, FALSE)

log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")

### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))
library(tibble)


# EXTRACT ----
# """""""""""""""""" ----




country_name_translations <- tribble(
  ~countryname,                          ~countryname_fr,                        ~countryname_de,                          ~countryname_nl,
  "Albania",                             "Albanie",                              "Albanien",                               "Albanië",
  "Algeria",                             "Algérie",                              "Algerien",                               "Algerije",
  "Andorra",                             "Andorre",                              "Andorra",                                "Andorra",
  "Angola",                              "Angola",                               "Angola",                                 "Angola",
  "Argentina",                           "Argentine",                            "Argentinien",                            "Argentinië",
  "Armenia",                             "Arménie",                              "Armenien",                               "Armenië",
  "Australia",                           "Australie",                            "Australien",                             "Australië",
  "Austria",                             "Autriche",                             "Österreich",                             "Oostenrijk",
  "Azerbaijan",                          "Azerbaïdjan",                          "Aserbaidschan",                          "Azerbeidzjan",
  "Bahamas",                             "Bahamas",                              "Bahamas",                                "Bahama's",
  "Bahrain",                             "Bahreïn",                              "Bahrain",                                "Bahrein",
  "Bangladesh",                          "Bangladesh",                           "Bangladesch",                            "Bangladesh",
  "Barbados",                            "Barbade",                              "Barbados",                               "Barbados",
  "Belgium",                             "Belgique",                             "Belgien",                                "België",
  "Belize",                              "Belize",                               "Belize",                                 "Belize",
  "Benin",                               "Bénin",                                "Benin",                                  "Benin",
  "Bermuda",                             "Bermudes",                             "Bermuda",                                "Bermuda",
  "Bhutan",                              "Bhoutan",                              "Bhutan",                                 "Bhutan",
  "Bolivia",                             "Bolivie",                              "Bolivien",                               "Bolivia",
  "Bosnia and Herzegovina",              "Bosnie-Herzégovine",                   "Bosnien und Herzegowina",                "Bosnië en Herzegovina",
  "Botswana",                            "Botswana",                             "Botsuana",                               "Botswana",
  "Brazil",                              "Brésil",                               "Brasilien",                              "Brazilië",
  "Brunei",                              "Brunei",                               "Brunei",                                 "Brunei",
  "Bulgaria",                            "Bulgarie",                             "Bulgarien",                              "Bulgarije",
  "Burkina Faso",                        "Burkina Faso",                         "Burkina Faso",                           "Burkina Faso",
  "Burundi",                             "Burundi",                              "Burundi",                                "Burundi",
  "Cabo Verde",                          "Cap-Vert",                             "Kap Verde",                              "Kaapverdië",
  "Cambodia",                            "Cambodge",                             "Kambodscha",                             "Cambodja",
  "Cameroon",                            "Cameroun",                             "Kamerun",                                "Kameroen",
  "Canada",                              "Canada",                               "Kanada",                                 "Canada",
  "Central African Republic",            "République centrafricaine",           "Zentralafrikanische Republik",           "Centraal-Afrikaanse Republiek",
  "Chile",                               "Chili",                                "Chile",                                  "Chili",
  "China",                               "Chine",                                "China",                                  "China",
  "Colombia",                            "Colombie",                             "Kolumbien",                              "Colombia",
  "Congo (Democratic Republic of the)",  "République démocratique du Congo",    "Demokratische Republik Kongo",           "Democratische Republiek Congo",
  "Congo (Republic of the)",             "République du Congo",                 "Republik Kongo",                         "Republiek Congo",
  "Costa Rica",                          "Costa Rica",                          "Costa Rica",                             "Costa Rica",
  "Côte d'Ivoire",                       "Côte d'Ivoire",                        "Elfenbeinküste",                         "Ivoorkust",
  "Croatia",                             "Croatie",                              "Kroatien",                               "Kroatië",
  "Curaçao",                             "Curaçao",                              "Curaçao",                                "Curaçao",
  "Cyprus",                              "Chypre",                               "Zypern",                                 "Cyprus",
  "Czech Republic",                      "République tchèque",                  "Tschechische Republik",                  "Tsjechië",
  "Denmark",                             "Danemark",                             "Dänemark",                               "Denemarken",
  "Djibouti",                            "Djibouti",                             "Dschibuti",                              "Djibouti",
  "Dominica",                            "Dominique",                            "Dominica",                               "Dominica",
  "Dominican Republic",                  "République dominicaine",              "Dominikanische Republik",                "Dominicaanse Republiek",
  "Ecuador",                             "Équateur",                             "Ecuador",                                "Ecuador",
  "Egypt",                               "Égypte",                               "Ägypten",                                "Egypte",
  "El Salvador",                         "Salvador",                             "El Salvador",                            "El Salvador",
  "Eritrea",                             "Érythrée",                             "Eritrea",                                "Eritrea",
  "Estonia",                             "Estonie",                              "Estland",                                "Estland",
  "Ethiopia",                            "Éthiopie",                             "Äthiopien",                              "Ethiopië",
  "Fiji",                                "Fidji",                                "Fidschi",                                "Fiji",
  "Finland",                             "Finlande",                             "Finnland",                               "Finland",
  "France",                              "France",                               "Frankreich",                             "Frankrijk",
  "French Guiana",                       "Guyane française",                    "Französisch-Guayana",                    "Frans-Guyana",
  "French Polynesia",                    "Polynésie française",                 "Französisch-Polynesien",                 "Frans-Polynesië",
  "Gabon",                               "Gabon",                                "Gabun",                                  "Gabon",
  "Gambia",                              "Gambie",                               "Gambia",                                 "Gambia",
  "Georgia",                             "Géorgie",                              "Georgien",                               "Georgië",
  "Germany",                             "Allemagne",                            "Deutschland",                            "Duitsland",
  "Ghana",                               "Ghana",                                "Ghana",                                  "Ghana",
  "Greece",                              "Grèce",                                "Griechenland",                           "Griekenland",
  "Greenland",                           "Groenland",                            "Grönland",                               "Groenland",
  "Guadeloupe",                          "Guadeloupe",                           "Guadeloupe",                             "Guadeloupe",
  "Guatemala",                           "Guatemala",                            "Guatemala",                              "Guatemala",
  "Guinea",                              "Guinée",                               "Guinea",                                 "Guinee",
  "Guinea-Bissau",                       "Guinée-Bissau",                        "Guinea-Bissau",                          "Guinee-Bissau",
  "Guyana",                              "Guyana",                               "Guyana",                                 "Guyana",
  "Haiti",                               "Haïti",                                "Haiti",                                  "Haïti",
  "Holy See (Vatican City State)",       "Saint-Siège (État de la Cité du Vatican)", "Heiliger Stuhl (Vatikanstadt)",     "Heilige Stoel (Vaticaanstad)",
  "Honduras",                            "Honduras",                             "Honduras",                               "Honduras",
  "Hungary",                             "Hongrie",                              "Ungarn",                                 "Hongarije",
  "Iceland",                             "Islande",                              "Island",                                 "IJsland",
  "India",                               "Inde",                                 "Indien",                                 "India",
  "Indonesia",                           "Indonésie",                            "Indonesien",                             "Indonesië",
  "Iran, Islamic Republic of",           "Iran (République islamique d')",      "Iran, Islamische Republik",              "Iran, Islamitische Republiek",
  "Ireland",                             "Irlande",                              "Irland",                                 "Ierland",
  "Israel",                              "Israël",                               "Israel",                                 "Israël",
  "Italy",                               "Italie",                               "Italien",                                "Italië",
  "Jamaica",                             "Jamaïque",                             "Jamaika",                                "Jamaica",
  "Japan",                               "Japon",                                "Japan",                                  "Japan",
  "Jersey",                              "Jersey",                               "Jersey",                                 "Jersey",
  "Jordan",                              "Jordanie",                             "Jordanien",                              "Jordanië",
  "Kazakhstan",                          "Kazakhstan",                           "Kasachstan",                             "Kazachstan",
  "Kenya",                               "Kenya",                                "Kenia",                                  "Kenia",
  "Kuwait",                              "Koweït",                               "Kuwait",                                 "Koeweit",
  "Kyrgyzstan",                          "Kirghizistan",                         "Kirgisistan",                            "Kirgizië",
  "Laos",                                "Laos",                                 "Laos",                                   "Laos",
  "Latvia",                              "Lettonie",                             "Lettland",                               "Letland",
  "Lebanon",                             "Liban",                                "Libanon",                                "Libanon",
  "Liberia",                             "Liberia",                              "Liberia",                                "Liberia",
  "Liechtenstein",                       "Liechtenstein",                        "Liechtenstein",                          "Liechtenstein",
  "Lithuania",                           "Lituanie",                             "Litauen",                                "Litouwen",
  "Luxembourg",                          "Luxembourg",                           "Luxemburg",                              "Luxemburg",
  "Madagascar",                          "Madagascar",                           "Madagaskar",                             "Madagaskar",
  "Malawi",                              "Malawi",                               "Malawi",                                 "Malawi",
  "Malaysia",                            "Malaisie",                             "Malaysia",                               "Maleisië",
  "Maldives",                            "Maldives",                             "Malediven",                              "Malediven",
  "Malta",                               "Malte",                                "Malta",                                  "Malta",
  "Martinique",                          "Martinique",                           "Martinique",                             "Martinique",
  "Mauritania",                          "Mauritanie",                           "Mauretanien",                            "Mauritanië",
  "Mauritius",                           "Maurice",                              "Mauritius",                              "Mauritius",
  "Mexico",                              "Mexique",                              "Mexiko",                                 "Mexico",
  "Micronesia, Federated States of",     "Micronésie (États fédérés de)",       "Mikronesien",                            "Micronesië",
  "Moldova, Republic of",                "Moldova (République de)",             "Republik Moldau",                        "Moldavië",
  "Monaco",                              "Monaco",                               "Monaco",                                 "Monaco",
  "Mongolia",                            "Mongolie",                             "Mongolei",                               "Mongolië",
  "Morocco",                             "Maroc",                                "Marokko",                                "Marokko",
  "Mozambique",                          "Mozambique",                           "Mosambik",                               "Mozambique",
  "Myanmar",                             "Myanmar",                              "Myanmar",                                "Myanmar",
  "Namibia",                             "Namibie",                              "Namibia",                                "Namibië",
  "Nepal",                               "Népal",                                "Nepal",                                  "Nepal",
  "Netherlands",                         "Pays-Bas",                             "Niederlande",                            "Nederland",
  "New Caledonia",                       "Nouvelle-Calédonie",                   "Neukaledonien",                          "Nieuw-Caledonië",
  "New Zealand",                         "Nouvelle-Zélande",                     "Neuseeland",                             "Nieuw-Zeeland",
  "Niger",                               "Niger",                                "Niger",                                  "Niger",
  "Nigeria",                             "Nigeria",                              "Nigeria",                                "Nigeria",
  "North Macedonia",                     "Macédoine du Nord",                   "Nordmazedonien",                         "Noord-Macedonië",
  "Norway",                              "Norvège",                              "Norwegen",                               "Noorwegen",
  "Oman",                                "Oman",                                 "Oman",                                   "Oman",
  "Pakistan",                            "Pakistan",                             "Pakistan",                               "Pakistan",
  "Palau",                               "Palaos",                               "Palau",                                  "Palau",
  "Palestinian Territory",               "Territoire palestinien",              "Palästinensische Gebiete",               "Palestijns Gebied",
  "Panama",                              "Panama",                               "Panama",                                 "Panama",
  "Papua New Guinea",                    "Papouasie-Nouvelle-Guinée",           "Papua-Neuguinea",                        "Papoea-Nieuw-Guinea",
  "Paraguay",                            "Paraguay",                             "Paraguay",                               "Paraguay",
  "Peru",                                "Pérou",                                "Peru",                                   "Peru",
  "Philippines",                         "Philippines",                         "Philippinen",                            "Filipijnen",
  "Poland",                              "Pologne",                              "Polen",                                  "Polen",
  "Portugal",                            "Portugal",                             "Portugal",                               "Portugal",
  "Puerto Rico",                         "Porto Rico",                           "Puerto Rico",                            "Puerto Rico",
  "Qatar",                               "Qatar",                                "Katar",                                  "Qatar",
  "Réunion",                             "Réunion",                              "Réunion",                                "Réunion",
  "Romania",                             "Roumanie",                             "Rumänien",                               "Roemenië",
  "Russia",                              "Russie",                               "Russland",                               "Rusland",
  "Rwanda",                              "Rwanda",                               "Ruanda",                                 "Rwanda",
  "São Tomé and Principe",               "Sao Tomé-et-Principe",                "São Tomé und Príncipe",                  "Sao Tomé en Principe",
  "Saudi Arabia",                        "Arabie saoudite",                     "Saudi-Arabien",                          "Saoedi-Arabië",
  "Senegal",                             "Sénégal",                              "Senegal",                                "Senegal",
  "Serbia",                              "Serbie",                               "Serbien",                                "Servië",
  "Seychelles",                          "Seychelles",                          "Seychellen",                             "Seychellen",
  "Sierra Leone",                        "Sierra Leone",                        "Sierra Leone",                           "Sierra Leone",
  "Singapore",                           "Singapour",                            "Singapur",                               "Singapore",
  "Slovakia",                            "Slovaquie",                            "Slowakei",                               "Slowakije",
  "Slovenia",                            "Slovénie",                             "Slowenien",                              "Slovenië",
  "South Africa",                        "Afrique du Sud",                      "Südafrika",                              "Zuid-Afrika",
  "South Korea",                         "Corée du Sud",                        "Südkorea",                               "Zuid-Korea",
  "Spain",                               "Espagne",                              "Spanien",                                "Spanje",
  "Sri Lanka",                           "Sri Lanka",                            "Sri Lanka",                              "Sri Lanka",
  "Sudan",                               "Soudan",                               "Sudan",                                  "Soedan",
  "Suriname",                            "Suriname",                             "Suriname",                               "Suriname",
  "Sweden",                              "Suède",                                "Schweden",                               "Zweden",
  "Switzerland",                         "Suisse",                               "Schweiz",                                "Zwitserland",
  "Syrian Arab Republic",                "République arabe syrienne",           "Arabische Republik Syrien",              "Syrische Arabische Republiek",
  "Taiwan",                              "Taïwan",                               "Taiwan",                                 "Taiwan",
  "Tajikistan",                          "Tadjikistan",                          "Tadschikistan",                          "Tadzjikistan",
  "Tanzania, United Republic of",        "Tanzanie (République-Unie de)",       "Vereinigte Republik Tansania",           "Tanzania",
  "Thailand",                            "Thaïlande",                            "Thailand",                               "Thailand",
  "Togo",                                "Togo",                                 "Togo",                                   "Togo",
  "Trinidad and Tobago",                 "Trinité-et-Tobago",                   "Trinidad und Tobago",                    "Trinidad en Tobago",
  "Tunisia",                             "Tunisie",                              "Tunesien",                               "Tunesië",
  "Türkiye",                             "Türkiye (Turquie)",                   "Türkei",                                 "Turkije",
  "Uganda",                              "Ouganda",                              "Uganda",                                 "Oeganda",
  "Ukraine",                             "Ukraine",                              "Ukraine",                                "Oekraïne",
  "United Arab Emirates",                "Émirats arabes unis",                 "Vereinigte Arabische Emirate",           "Verenigde Arabische Emiraten",
  "United Kingdom",                      "Royaume-Uni",                          "Vereinigtes Königreich",                 "Verenigd Koninkrijk",
  "United States",                       "États-Unis",                          "Vereinigte Staaten",                    "Verenigde Staten",
  "Uruguay",                             "Uruguay",                              "Uruguay",                                "Uruguay",
  "Uzbekistan",                          "Ouzbékistan",                          "Usbekistan",                             "Oezbekistan",
  "Vanuatu",                             "Vanuatu",                              "Vanuatu",                                "Vanuatu",
  "Vietnam",                             "Viêt Nam",                             "Vietnam",                                "Vietnam",
  "Zambia",                              "Zambie",                               "Sambia",                                 "Zambia",
  "Zimbabwe",                            "Zimbabwe",                             "Simbabwe",                               "Zimbabwe"
)



## Source page: https://diplomatie.belgium.be/en/embassies-and-consulates
## API endpoint (per country, ISO3 code):
## https://publicapps.diplomatie.be/api/belgianInstitutions?language={lang}&countryIso={iso3}
## language: 0 = NL, 1 = FR, 2 = EN,  3 = DE (English used by default here)

# Full list of ISO3 country codes to iterate over (excludes NA / non-standard codes)
get_iso3_list <- function() {
  iso3_codes <- countrycode::codelist$iso3c
  iso3_codes <- unique(iso3_codes[!is.na(iso3_codes)])
  return(iso3_codes)
}

# Download the Belgian institutions for a single country ----
download_belgian_institutions_for_country <- function(iso3, language = api_language) {
  url <- paste0(
    "https://publicapps.diplomatie.be/api/belgianInstitutions?language=",
    language, "&countryIso=", iso3
  )

  result <- tryCatch(
    {
      response <- GET(url, timeout(30))
      if (status_code(response) != 200) {
        message(paste0("Non-200 status (", status_code(response), ") for country ", iso3, ", skipping"))
        return(NULL)
      }
      content_text <- content(response, as = "text", encoding = "UTF-8")
      if (!nzchar(content_text)) {
        return(NULL)
      }
      parsed <- fromJSON(content_text, flatten = FALSE)
      if (length(parsed) == 0 || (is.data.frame(parsed) && nrow(parsed) == 0)) {
        return(NULL)
      }
      parsed <- as_tibble(parsed)
      parsed$country_iso3 <- iso3
      return(parsed)
    },
    error = function(e) {
      message(paste0("Failed to download data for country ", iso3, ": ", e$message))
      return(NULL)
    }
  )
  return(result)
}

# Download the Belgian institutions for all countries ----
download_all_belgian_institutions <- function() {
  iso3_codes <- get_iso3_list()
  print(paste0("Downloading Belgian institutions abroad for ", length(iso3_codes), " countries"))

  all_results <- map(iso3_codes, function(iso3) {
    Sys.sleep(0.1) # be polite to the API
    download_belgian_institutions_for_country(iso3)
  })

  all_results <- compact(all_results) # drop NULL (empty) results

  if (length(all_results) == 0) {
    stop("No data was downloaded for any country")
  }

  belgian_institutions <- bind_rows(all_results)
  print(paste0(
    "Downloaded ", nrow(belgian_institutions), " Belgian institutions abroad across ",
    length(all_results), " countries with at least one result"
  ))

  return(belgian_institutions)
}


# TRANSFORM ----
# """""""""""""""""" ----




# The API returns a nested meansOfContact list per institution, with the type
# encoded as meansOfContactType_ID. Empirically, only 4 types occur in the data:
#   1 = email, 2 = phone, 7 = fax, 8 = website
# These are turned into their own named columns below (collapsing multiple
# values of the same type per institution with "; ").

# Belgian foreign service ----

means_of_contact_type_map <- c(
  "1" = "email",
  "2" = "phone",
  "7" = "fax",
  "8" = "website"
)

# Extract all values for a given meansOfContactType_ID from a single institution's
# nested meansOfContact data frame (or NULL if the institution has none)
extract_contact_value <- function(contacts, type_id) {
  if (is.null(contacts) || !is.data.frame(contacts) || nrow(contacts) == 0) {
    return(NA_character_)
  }
  values <- contacts$value[contacts$meansOfContactType_ID == type_id]
  values <- values[!is.na(values) & nzchar(values)]
  if (length(values) == 0) {
    return(NA_character_)
  }
  paste(unique(values), collapse = "; ")
}

# Flag if any meansOfContactType_ID occurs that isn't in the known map above,
# so we notice if the API starts returning new types
check_for_unknown_contact_types <- function(belgian_institutions) {
  all_type_ids <- belgian_institutions$meansOfContact %>%
    keep(~ is.data.frame(.x) && nrow(.x) > 0) %>%
    map(~ unique(.x$meansOfContactType_ID)) %>%
    unlist() %>%
    unique()

  unknown_type_ids <- setdiff(as.character(all_type_ids), names(means_of_contact_type_map))
  if (length(unknown_type_ids) > 0) {
    warning(paste0(
      "Unknown meansOfContactType_ID(s) found that are not mapped to a column: ",
      paste(unknown_type_ids, collapse = ", ")
    ))
  }
}

# Collapse the nested staff list per institution into a single readable text column
extract_staff_summary <- function(staff) {
  if (is.null(staff) || !is.data.frame(staff) || nrow(staff) == 0) {
    return(NA_character_)
  }
  summaries <- apply(staff, 1, function(row) {
    parts <- c(row[["title_Name"]], row[["firstName"]])
    parts <- parts[!is.na(parts) & nzchar(parts)]
    if (length(parts) == 0) {
      return(NA_character_)
    }
    paste(parts, collapse = " - ")
  })
  summaries <- unique(summaries[!is.na(summaries)])
  if (length(summaries) == 0) {
    return(NA_character_)
  }
  paste(summaries, collapse = "; ")
}



process_belgian_institutions <- function(belgian_institutions) {
  check_for_unknown_contact_types(belgian_institutions)
  # NOTE: the API already has top-level "email" and "url" fields with the
  # institution's primary contact details, but these are empty.
  belgian_institutions <- belgian_institutions %>%
    select(-email,-url) %>%
    mutate(
      local_email = map_chr(meansOfContact, extract_contact_value, type_id = 1),
      local_phone = map_chr(meansOfContact, extract_contact_value, type_id = 2),
      local_fax = map_chr(meansOfContact, extract_contact_value, type_id = 7),
      local_website = map_chr(meansOfContact, extract_contact_value, type_id = 8),
      staff_summary = map_chr(staff, extract_staff_summary)
    ) %>%
    select(-meansOfContact, -staff)

  belgian_institutions <- clean_empty_strings(belgian_institutions)
  
  names(belgian_institutions) <- tolower(names(belgian_institutions))
  
  
  # create address
  belgian_institutions <- belgian_institutions %>%
    rowwise() %>%
    mutate(full_address = paste(
      str_squish(na.omit(c(addressline1, paste(na.omit(zipcode), na.omit(city_name)), countryname))),
      collapse = ", "
    )) %>%
    ungroup()
  
  # add main personnel info
  belgian_institutions <- belgian_institutions %>%
    rowwise() %>%
    mutate(ambassador_info=paste(na.omit(c(title_name,firstname,lastname)),
                                 collapse = " "))%>%
    ungroup()
  
  belgian_institutions <- belgian_institutions %>%
    mutate(row_id=row_number())
  
  belgian_institutions <- belgian_institutions %>%
    mutate(
      country_served = countrycode(country_iso3, origin = "iso3c", destination = "country.name")
    )
  
  # get localized country names
  belgian_institutions <- belgian_institutions %>%
     left_join(country_name_translations, by = "countryname")
  
  
  
  belgian_institutions <- summarize_by_value(
    df           = belgian_institutions,
    group_col    = "full_address",
    id_col       = "row_id",
    summary_cols = c("institutiontypename", "full_address", "addressline1", "zipcode", "city_name", "pobox", "countryname", "countryname_nl", "countryname_fr", "countryname_de", "country_served", "local_email", "local_phone", "local_fax", "local_website", "ambassador_info")
  )

  # remove Belgian embassies in Belgium
  belgian_institutions <- belgian_institutions %>%
    filter(countryname !="Belgium")  
  

  # some country names are not recognized by the geocoder. Here we turn them into a more readable name, or the name of the mother country
  country_recode_map <- c(
    "Congo (Democratic Republic of the)" = "Democratic Republic of the Congo",
    "Congo (Republic of the)"            = "Republic of the Congo",
    "Holy See (Vatican City State)"      = "Vatican City",
    "Iran, Islamic Republic of"          = "Iran",
    "Moldova, Republic of"               = "Moldova",
    "Palestinian Territory"              = "Palestinian Territories",
    "Curaçao"                            = "The Netherlands",
    "French Guiana"                      = "France",
    "French Polynesia"                   = "France",
    "Guadeloupe"                         = "France",
    "Martinique"                         = "France",
    "New Caledonia"                      = "France",
    "Réunion"                            = "France",
    "Micronesia, Federated States of"    = "Micronesia",
    "Puerto Rico"                        = "United States",
    "Syrian Arab Republic"               = "Syria"
  )
  belgian_institutions <- belgian_institutions %>%
    mutate(country_for_geocoding = recode(countryname, !!!country_recode_map, .default = countryname))

  belgian_institutions <- belgian_institutions %>%
    rowwise() %>%
    mutate(geocode_full_address = paste(
      na.omit(c(addressline1, paste(na.omit(zipcode), na.omit(city_name)), country_for_geocoding)),
      collapse = ", "
    )) %>%
    ungroup()  
  
  # some special cases are not recognized because of overly detailed address descriptions. Here we overrule some addresses where no results turned up at all.
  belgian_institutions <- belgian_institutions %>%
    mutate(geocode_full_address=
             case_when(
               grepl('Ponce de Leon, Centro Europa',geocode_full_address) ~ 'Centro Europa, San Juan, Puerto Rico',
               grepl('Karbouchian',geocode_full_address) ~ 'Mutanabbi Street, Lattakia, Syria',
               grepl('NAPA Auto Parts',geocode_full_address) ~ 'Kolonia, Micronesia',
               grepl('Baibars',geocode_full_address) ~ 'Beibars 5, Jerusalem',
               grepl('Avenida calle 92',geocode_full_address) ~ 'Avenida Calle 92, 11, Bogota, Colombia',
               grepl('Saint Paul Minneapolis',geocode_full_address) ~ 'Saint Paul, Minnesota',
               grepl('Kizilcikli Mahmut Pehlivan',geocode_full_address) ~ 'Kızılcıklı Mahmut Pehlivan Caddesi, Eskisehir, Türkiye',
               grepl('Via Giuseppe de Notaris 6',geocode_full_address) ~ '6, Via Giuseppe De Notaris, Roma Capitale, Italië',
               TRUE ~ geocode_full_address
             ))

  geocode_passes <- list(
    list(name = "full address (structured)", args = list(street = "addressline1", city = "city_name", postalcode = "zipcode", country = "country_for_geocoding")),
    list(name = "full address", args = list(address = "geocode_full_address")),
    list(name = "street address and city", args = list(street = "addressline1", city = "city_name")),
    list(name = "city and country only", args = list(city = "city_name", country = "country_for_geocoding"))
  )
  # Geocode a single row, retrying on transient connection errors (e.g. Nominatim
  # timeouts / connection resets). Waits `wait_seconds` between attempts, up to
  # `max_attempts` total. If all attempts fail, the row is returned with NA
  # lat/long so it falls through to the next fallback pass instead of stopping
  # the whole loop.
  geocode_row_with_retry <- function(row_data, geocode_static_args, max_attempts = 3, wait_seconds = 60) {
    attempt <- 1
    repeat {
      result <- tryCatch(
        {
          args <- c(list(.tbl = row_data), geocode_static_args, list(lat = "lat_tmp", long = "long_tmp"))
          do.call(geocode, args)
        },
        error = function(e) {
          message(paste0(
            "Row geocoding attempt ", attempt, "/", max_attempts, " failed: ", e$message
          ))
          NULL
        }
      )
      if (!is.null(result)) {
        return(result)
      }
      if (attempt >= max_attempts) {
        message(paste0("Giving up on this row after ", max_attempts, " attempts, treating it as ungeocoded for this pass"))
        row_data$lat_tmp <- NA_real_
        row_data$long_tmp <- NA_real_
        return(row_data)
      }
      message(paste0("Waiting ", wait_seconds, " seconds before retrying this row..."))
      Sys.sleep(wait_seconds)
      attempt <- attempt + 1
    }
  }

  geocode_with_fallback <- function(data, passes, method = "osm") {
    data <- data %>%
      mutate(row_id = row_number(), latitude = NA_real_, longitude = NA_real_, geocode_method = NA_character_)
    remaining <- data
    for (pass in passes) {
      if (nrow(remaining) == 0) break
      print(paste0("Geocoding pass '", pass$name, "': ", nrow(remaining), " rows remaining"))
      geocode_static_args <- c(pass$args, list(method = method, limit = 1))
      # geocode row by row (rather than as one batch) so a single connection
      # error / timeout can be retried without losing progress on the rest
      # of the batch
      geocoded <- map_dfr(seq_len(nrow(remaining)), function(i) {
        if (i > 1) Sys.sleep(1)  # Nominatim usage policy: max 1 request/second
        geocode_row_with_retry(remaining[i, ], geocode_static_args)
      })
      found <- geocoded %>% filter(!is.na(lat_tmp))
      # write results back into the main dataset by row_id
      data <- data %>%
        rows_update(
          found %>% transmute(row_id, latitude = lat_tmp, longitude = long_tmp, geocode_method = pass$name),
          by = "row_id"
        )
      remaining <- geocoded %>% filter(is.na(lat_tmp)) %>% select(-lat_tmp, -long_tmp)
    }
    return(data)
  }
  belgian_institutions <- geocode_with_fallback(belgian_institutions, geocode_passes)
  
  
  # select ungeocoded
  failed <- belgian_institutions %>%
    filter(is.na(latitude) | is.na(longitude)) 
  
  print(paste0("Failed to geocode ",nrow(failed)," records:"))
  print(failed)
  
  # remove cases we couldn't geocode
  belgian_institutions <- belgian_institutions %>%
    filter(!is.na(latitude) & !is.na(longitude))
  

  # make SF dataset based on lat and lon
  belgian_institutions <- st_as_sf(
    belgian_institutions,
    coords = c("longitude", "latitude"),
    crs = 4326
  )
  
  # summarize identical results
  belgian_institutions <- summarize_by_value(
    df           = belgian_institutions,
    group_col    = "geometry",
    id_col       = "row_id",
    summary_cols = c("institutiontypename", "full_address", "countryname", "countryname_nl", "countryname_fr", "countryname_de", "country_served", "local_email", "local_phone", "local_fax", "local_website", "ambassador_info", "geocode_method", "city_name")
  )

  belgian_institutions <- belgian_institutions %>%
    add_count(countryname, name = "country_occurrences") %>%
    mutate(
      location = if_else(
        country_occurrences == 1,
        countryname,
        paste(city_name, countryname, sep = ", ")
      ),
      location_nl = if_else(
        country_occurrences == 1,
        countryname_nl,
        paste(city_name, countryname_nl, sep = ", ")
      ),
      location_fr = if_else(
        country_occurrences == 1,
        countryname_fr,
        paste(city_name, countryname_fr, sep = ", ")
      ),
      location_de = if_else(
        country_occurrences == 1,
        countryname_de,
        paste(city_name, countryname_de, sep = ", ")
      )

    ) %>%
    select(-country_occurrences)
  
  # Flemish foreign service ----
  wfs_service <- "https://geo.api.vlaanderen.be/GebouwenVLO/wfs"
  url <- parse_url(wfs_service)
  url$query <- list(
    SERVICE = "WFS",
    VERSION = "2.0.0",
    REQUEST = "GetFeature",
    TYPENAMES = "GebouwenVLO:Gebvlo",
    SRSNAME = "urn:ogc:def:crs:EPSG::4326"
  )
  final_url <- build_url(url)
  gebouwen_raw <- read_sf(final_url)
  
  names(gebouwen_raw) <- tolower(names(gebouwen_raw))
  
  flemish_embassies <- gebouwen_raw %>%
    filter(subfunctie %in% c("Ambassade","Buitenlandse vertegenwoordiger","Consulaat")) %>%
    filter(idgebouw != '77D429B5-DDAB-FF73-09C8-4C22487681BF') # remove a duplicate in the Flemish data
  
  st_geometry(flemish_embassies) <- "shape"  
  
  flemish_embassies <- flemish_embassies %>%
    select(original_id=idgebouw, naamgeb, subfunctie, straat, huisnr, postcode, gemeente, land, operator=behgeb)

    
  # Merge ----
  # join nearby objects from both datasets
  options(scipen = 999)
  join <- st_join(belgian_institutions, flemish_embassies, join = st_is_within_distance, dist = 5000)
  join <- as.data.frame(join)
  
  # add the geometry from the geometry providing objects
  geom_features_geo<-flemish_embassies %>% select(original_id, shape)
  geom_features_geo<-as.data.frame(geom_features_geo)
  join <- left_join(join, geom_features_geo, by = "original_id")
  
  # Calculate distance between potential matches
  #join$distance <- mapply(calculate_distance, join$geometry, join$off_geometry)
  # we use euclidian distance, as our default Hausdorf distance isn't great for point vs polygon comparisons (de facto it gives the furthest point from the nearby polygons)
  join$distance <- mapply(calculate_eucl_distance, join$shape, join$geometry)
  
  # longest common substring between full_address (embassy) and straat (Flanders)
    #set names to lowercase
    join$full_address<-tolower(join$full_address)
    join$straat<-tolower(join$straat)
    
    # remove accents
    join <- join %>%
      mutate(
        full_address=stri_trans_general(full_address, "Latin-ASCII"),
        straat=stri_trans_general(straat, "Latin-ASCII")
      )
    # Small function to use LCS without throwing errors if one of the values is NA
    safe_lcs <- function(a, b) {
      if (is.na(a) | is.na(b)) return(NA_character_)
      LCSn(c(tolower(a), tolower(b)))
    }

  # select only cases where the address overlaps at least for 10 digits, and from those candidates, select the nearest one
  join <- join %>%
    rowwise() %>%
    mutate(
      lcs = trimws(safe_lcs(full_address, straat)),
      longest_common_str_length = nchar(lcs)
    ) %>%
    filter(longest_common_str_length >= 10) %>%
    ungroup() %>%
    group_by(row_id) %>%
    arrange(distance, .by_group = TRUE) %>%
    slice_head(n = 1) %>%
    ungroup() %>%
    select(row_id, original_id)
  
  # merge results to Belgian set
  belgian_institutions <- belgian_institutions %>%
    left_join(join, by = "row_id")
    
  ## Create dataset of merged objects, where only the official attributes are interesting ----
  belgian_institutions_attr_only <- as.data.frame(belgian_institutions) %>%
    filter(!is.na(original_id)) %>%
    select(-geometry)
  
  belgian_institutions_attr_only <- belgian_institutions_attr_only %>%
    rename(address=full_address, 
      embassy_type=institutiontypename,
      ambassador_info=ambassador_info,
      embassy_country_located=countryname,
      embassy_country_served=country_served,
      local_email=local_email, 
      local_phone=local_phone, 
      local_fax=local_fax, 
      local_website=local_website)
  
  
  # attribute-only objects should be merged on their shares geometry id while keeping a summary of the relevant columns
  # the attributes source needs to be just a DF and the geometry should be lost
  belgian_institutions_attr_only <- summarize_by_value(
    df           = belgian_institutions_attr_only,
    group_col    = "original_id",
    id_col       = "row_id",
    summary_cols = c("embassy_type","ambassador_info","embassy_country_located","embassy_country_served","local_email","local_phone","local_fax","local_website", "location")
  )
  
  belgian_institutions_attr_only <- belgian_institutions_attr_only %>%
    rename(other_names=location)
 

  # create properties columns
  ## in official_attributes_source, the only needed columns are properties_secondary & osm_id, afterwards this can be joined to the OSM dataset
  ## attribute to be inserted within a data list id object. "name" is also a property, (hopefully) already provided on the Anchor by OSM
  belgian_institutions_attr_only <- build_json_column(
    df   = belgian_institutions_attr_only,
    output_col = "properties_secondary",
    cols = c("embassy_type","ambassador_info","embassy_country_located","embassy_country_served","local_email","local_phone","local_fax","local_website","other_names"),
    data_list_id = data_list_id_be
  )
  
  #re-introduce name, as we consider the name of the Belgian level institution to be leading
  name_only <- as.data.frame(belgian_institutions) %>%
    select(row_id, location, location_nl, location_fr, location_de)
  belgian_institutions_attr_only <- belgian_institutions_attr_only %>%
    left_join(name_only, by = "row_id")
  
  ## Create dataset of unmerged objects, where the official geometry is also kept ----
  belgian_institutions <- belgian_institutions %>%
    filter(is.na(original_id)) %>%
    select(-original_id)
  
  # change geometry name shape to geometry
  flemish_embassies <- flemish_embassies %>%
    rename(geometry = shape) %>%
    st_set_geometry("geometry")
    

  # add records from be to vla
  belgian_institutions <- bind_rows(flemish_embassies,belgian_institutions)
  
  # add attributes to vla
  belgian_institutions <- belgian_institutions %>%
    left_join(belgian_institutions_attr_only, by="original_id" )
  
  belgian_institutions <- belgian_institutions %>%
    mutate(name_en=case_when(!is.na(location.x) ~ paste0("Diplomatic mission ",location.x),
                             !is.na(location.y) ~ paste0("Diplomatic mission ",location.y),
                             TRUE ~ NA
                             ),
           name_fr=case_when(!is.na(location_fr.x) ~ paste0("Mission diplomatique ",location_fr.x),
                             !is.na(location_fr.y) ~ paste0("Mission diplomatique ",location_fr.y),
                             TRUE ~ NA),
           name_de=case_when(!is.na(location_de.x) ~ paste0("Auslandsvertretung ",location_de.x),
                             !is.na(location_de.y) ~ paste0("Auslandsvertretung ",location_de.y),
                             TRUE ~ NA),
           name_nl=case_when(!is.na(location_nl.x) ~ paste0("Diplomatieke missie ",location_nl.x),
                             !is.na(location_nl.y) ~ paste0("Diplomatieke missie ",location_nl.y),
                             TRUE ~ naamgeb),
           name=case_when(!is.na(location.x) ~ paste0("Diplomatic mission ",location.x),
                             !is.na(location.y) ~ paste0("Diplomatic mission ",location.y),
                             TRUE ~ naamgeb
           ))
  
  
  return(belgian_institutions)
}



# LOAD ----
# """""""""""""""""" ----

# Upload to raw data ----
# CreateImportTable is loaded via utils.R and called in the main function


### Import to raw data ----
### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("
DROP TABLE IF EXISTS ingestion.embassy_abroad CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.embassy_abroad
  (
    id uuid NOT NULL DEFAULT gen_random_uuid(),
    original_id text,    
    name jsonb,
    legend_item jsonb,
    legend_item_id uuid,
  	data_list_id uuid,
  	risk_level integer,
    properties jsonb,
  	properties_secondary jsonb,
  	imported_at timestamptz,
  	tags jsonb,
  	deleted_at timestamptz,
  	updated_at timestamptz,
  	created_at timestamptz,
  	created_by uuid,
  	updated_by uuid,
    geometry geometry(geometry, 4326),
    CONSTRAINT embassy_abroad_pkey PRIMARY KEY (id)
  );
",paste0("
INSERT INTO ingestion.embassy_abroad
(original_id, name, legend_item_id, data_list_id, risk_level, properties, properties_secondary, created_at, geometry)
SELECT
CASE WHEN original_id IS NULL THEN \"row_id.x\" ELSE original_id END AS original_id, 
jsonb_build_object(
  'und', name,
  'eng', name_en,
  'fre', name_fr,
  'ger', name_de,
  'dut', name_nl
  ) as name,
CASE WHEN \"row_id.x\" IS NOT NULL or \"row_id.y\" IS NOT NULL THEN '", legend_item_id_be, "'::uuid 
	ELSE '", legend_item_id_vl, "'::uuid END
	AS legend_item_id,
CASE WHEN \"row_id.x\" IS NOT NULL THEN '", data_list_id_be, "'::uuid 
	ELSE '", data_list_id_vl, "'::uuid END
	AS data_list_id,
1 as risk_level,
jsonb_strip_nulls(jsonb_build_object(
  'address', CASE WHEN full_address IS NOT NULL THEN full_address ELSE concat(straat,' ',huisnr,', ', postcode,' ', gemeente,', ', land) END ,
  'embassy_type', CASE WHEN institutiontypename IS NOT NULL THEN institutiontypename ELSE subfunctie END,
  'ambassador_info', ambassador_info,
  'embassy_country_located', countryname,
  'embassy_country_served', country_served,
  'local_email',local_email, 
  'local_phone',local_phone, 
  'local_fax',local_fax, 
  'local_website',local_website,
  'geocode_method',geocode_method 
)) as properties,
properties_secondary::jsonb,
CURRENT_DATE as created_at,
ST_ReducePrecision(geometry, 1e-8)
FROM raw_data.diplomatie_belgian_institutions_abroad;"),
"ALTER TABLE ingestion.embassy_abroad OWNER to pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.embassy_abroad TO pgn_group_data_team_w;",
"GRANT ALL ON TABLE ingestion.embassy_abroad TO pgn_user_airflow;")





### Execute the SQL commands ----
create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}


# set to TRUE if you want to update the transformation table even if the checks fail. 
#overrule_checks<-TRUE


run_smart_update = function() {
  smart_update_process("embassy_abroad", 1000, 1500, 1000, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
}

# Main function -----------------------------------------------------------
# """"""""""""""""""""----


main_function = function() {
  if (!reuse_ingestion_data) {
    belgian_institutions_raw <- run_process(
      download_all_belgian_institutions(),
      "Download Belgian embassies and consulates abroad"
    )
    belgian_institutions <- process_belgian_institutions(belgian_institutions_raw)
    CreateImportTable(
      dataset = belgian_institutions,
      schema = "raw_data",
      table_name = "diplomatie_belgian_institutions_abroad"
    )
    create_ingestion_table()
  }
  run_smart_update()
}


if(run_status){
  main_function()
}