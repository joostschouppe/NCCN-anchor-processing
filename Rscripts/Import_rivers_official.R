## ---------------------------
##
## Script name: Import official rivers
##
## Purpose of script: load official sources of rivers and try to add classification & operator
##
## Author: Joost Schouppe
##
## Date Created: 15/07/2025
##
##
## ---------------------------



# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# Set external IDs

data_list_id_bru <- "b92261bd-5bda-4fee-85ab-ca1f343c4fba"
data_list_id_wal <- "e721bd94-d9df-45c2-a86b-37e5a5ebc683"
data_list_id_vla <- "452a56f6-94dc-48a7-91a9-01e0649a72bc"

li_river_class_1 <- "23a2c764-c68e-4401-8407-e6c6ae4adf3e"
li_river_class_2 <- "3da0b75d-77e3-4d42-a51e-25514a2af79a"
li_river_class_3 <- "8d29ae2d-9ff5-4585-aa4d-02cd11230535"
li_river_navigable <- "b299344f-29f4-4659-8ad0-cba46612b791"
li_river_unclasified <- "c5cb53a8-101c-4353-a79f-ec9c41621655"
li_river_ditch <- "094094b5-3c5a-4249-9a05-5ebabdde52ca"

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

# connection details
db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

# run status
run_status<-Sys.getenv("RUN_STATUS")
## this is set to false and prevents any accidental changes to the database by switching off the main_function(). On Airflow, this is set to true.

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

# Set location for files that need to be downloaded manually
offline_storage <-Sys.getenv("OFFLINE_STORAGE")

log_folder <- Sys.getenv("RSCRIPT_LOG_FOLDER")



### Load external functions ------

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))

# extra libraries
library(xml2)



# EXTRACT -----------------------------------------------------------
# """"""""""""""""""----------------------

# Function to download fresh data ----
process_fresh_data <- function(){
  # Default: download fresh data
  if (reuse_ingestion_data==FALSE) {
    
    # Download Wallonia data ----
    
    # Proper ArcGIS REST API query for all data as GeoJSON
    query_url <- paste0(
      "https://geoservices.wallonie.be/arcgis/rest/services/EAU/RES_HYDRO_WAL/MapServer/1/query?",
      "where=1=1",
      "&outFields=*",
      "&f=geojson"
    )
    
    # Download and read spatial data
    axes_hydro_wal <- st_read(query_url)
    
    ## Available fields
    ### Note: there is no info about who is the manager of the object
    # OBJECTID ( type: esriFieldTypeOID, alias: OBJECTID )
    # SEGMENT_ID ( type: esriFieldTypeString, alias: Identifiant du segment géométrique, length: 200 )
    # SEGMENT_VERSION ( type: esriFieldTypeString, alias: Version du segment géométrique, length: 200 )
    # FROM_POINT_ID ( type: esriFieldTypeString, alias: Identifiant du point d’origine du segment, length: 200 )
    # TO_POINT_ID ( type: esriFieldTypeString, alias: Identifiant du point de fin du segment, length: 200 )
    # GEOM_DATE ( type: esriFieldTypeDate, alias: Date de dernière modification de la géométrie d'origine, length: 8 )
    # GEOM_SOURCE ( type: esriFieldTypeString, alias: Source de la géométrie d'origine (Code), length: 40 )
    # GEOM_SOURCE_DESC ( type: esriFieldTypeString, alias: Source de la géométrie d'origine, length: 255 )
    # GEOM_PRECISION ( type: esriFieldTypeSingle, alias: Précision de la géométrie d'origine (en mètre) )
    # ORIENT ( type: esriFieldTypeSmallInteger, alias: Orientation de l’axe réalisé )
    # ORI ( type: esriFieldTypeInteger, alias: Code ORI du cours d’eau )
    # CATEG ( type: esriFieldTypeString, alias: Catégorie du cours d’eau (Code), length: 10 )
    # CATEG_DESC ( type: esriFieldTypeString, alias: Catégorie du cours d’eau, length: 150 )
    # NOMA ( type: esriFieldTypeString, alias: Nom du cours d’eau dans l’Atlas des cours d’eau non navigables, length: 100 )
    # NOMB ( type: esriFieldTypeString, alias: Prénom relatif au nom du cours d’eau, length: 100 )
    # NUMATLAS ( type: esriFieldTypeInteger, alias: Numéro officiel à l'Atlas des cours d’eau non navigables )
    # NUMATLAS2 ( type: esriFieldTypeString, alias: Numéro conforme à l'Atlas des cours d’eau non navigables, length: 40 )
    # SAV ( type: esriFieldTypeString, alias: Descriptif physique de l’axe (Code), length: 5 )
    # SAV_DESC ( type: esriFieldTypeString, alias: Descriptif physique de l’axe, length: 80 )
    # ANCINS1 ( type: esriFieldTypeInteger, alias: Code INS de la commune avant fusions où se trouve le cours d‘eau )
    # ANCINS2 ( type: esriFieldTypeInteger, alias: Code INS de la seconde commune avant fusions où se trouve le cours d‘eau (Cours d’eau mitoyen) )
    # NOVINS1 ( type: esriFieldTypeInteger, alias: Code INS de la commune où se trouve le cours d‘eau )
    # NOVINS2 ( type: esriFieldTypeInteger, alias: Code INS de la seconde commune où se trouve le cours d‘eau (Cours d’eau mitoyen) )
    # ANCOM1 ( type: esriFieldTypeString, alias: Nom de l’ancienne commune avant fusion, length: 50 )
    # ANCOM2 ( type: esriFieldTypeString, alias: Nom de la seconde ancienne commune avant fusion (Cours d’eau mitoyen), length: 50 )
    # NEWCOM1 ( type: esriFieldTypeString, alias: Nom de la commune après fusion, length: 50 )
    # NEWCOM2 ( type: esriFieldTypeString, alias: Nom de la seconde commune après fusion (Cours d’eau mitoyen), length: 50 )
    # ID_BASSIN_PG ( type: esriFieldTypeString, alias: Identifiant du bassin du plan de gestion, length: 21 )
    # NOM_BASSIN_PG ( type: esriFieldTypeString, alias: Nom du bassin du plan de gestion, length: 20 )
    # ID_BASSIN_PR ( type: esriFieldTypeString, alias: Identifiant du bassin principal, length: 24 )
    # NOM_BASSIN_PR ( type: esriFieldTypeString, alias: Nom du bassin principal, length: 20 )
    # LONGUEUR ( type: esriFieldTypeDouble, alias: Longueur de l’axe (en mètre) ) 
    
    # aggregate by segment_id, categ, noma, nomb
    
    # test file
    # export to gpkg
    #st_write(axes_hydro_wal, "C:/temp/rivers/axes_hydro_wal.gpkg", delete_dsn = TRUE)
    
    # set all columns to lowercase
    axes_hydro_wal <<- axes_hydro_wal %>% 
      rename_all(tolower)
    cat("Total WAL river segments retrieved:", nrow(axes_hydro_wal), "\n")
    
    
    # Download Vlaanderen data ----
    
    ### Get the operator classification ----
    
    
    
    # Read the XML from the URL
    url <- "https://metadata.vlaanderen.be/srv/api/records/72947ce8-c050-4158-b65b-58180161614f/formatters/xml?&attachment=true"
    xml <- read_xml(url)
    
    # Find all FC_FeatureAttribute nodes
    attributes <- xml_find_all(xml, ".//gfc:FC_FeatureAttribute", xml_ns(xml))
    
    # Filter only the one where memberName is "Code Waterloopbeheerder"
    beheerder_node <- attributes[xml_text(xml_find_first(attributes, ".//gco:LocalName", xml_ns(xml))) == "Code Waterloopbeheerder"]
    
    # Extract all listedValue entries (gfc:FC_ListedValue)
    listed_values <- xml_find_all(beheerder_node, ".//gfc:listedValue/gfc:FC_ListedValue", xml_ns(xml))
    
    # Extract label and code for each listed value
    operator_codes <- lapply(listed_values, function(node) {
      operator <- xml_text(xml_find_first(node, ".//gfc:label/gco:CharacterString", xml_ns(xml)))
      operator_code <- xml_text(xml_find_first(node, ".//gfc:code/gco:CharacterString", xml_ns(xml)))
      data.frame(operator_code = operator_code, operator = operator, stringsAsFactors = FALSE)
    }) %>%
      bind_rows()
    
    
    ### Get the geodata ----
    ## WFS via https://www.vlaanderen.be/datavindplaats/catalogus/wfs-vlaamse-hydrografische-atlas-waterlopen
    
    # Define the base WFS URL
    wfs <- "https://geo.api.vlaanderen.be/VHAWaterlopen/wfs"
    
    # Build the request URL
    build_request_url <- function(start_index) {
      url <- parse_url(wfs)
      url$query <- list(
        service = "wfs",
        request = "GetFeature",
        typename = "VHAWaterlopen:Wlas",
        srsName = "EPSG:31370",
        startIndex = start_index,
        maxFeatures = 10000,
        outputFormat = "application/json"
      )
      return(build_url(url))
    }
    
    # Initialize variables
    all_features <- list()
    start_index <- 0
    batch_size <- 10000
    has_more_features <- TRUE
    
    # Loop to fetch data in batches
    while (has_more_features) {
      # Build the request URL for the current batch
      request_url <- build_request_url(start_index)
      
      # Fetch the data
      batch <- read_sf(request_url)
      
      # Check if there are no more features to fetch
      if (nrow(batch) == 0) {
        has_more_features <- FALSE
      } else {
        # Append the fetched features to the list
        all_features <- append(all_features, list(batch))
        # Increment the start index for the next batch
        start_index <- start_index + batch_size
      }
    }
    
    # Combine all fetched features into a single data frame
    vha <- do.call(rbind, all_features)
    
    vha_raw<<-vha
    
    # set all columns to lowercase
    vha <- vha %>% 
      rename_all(tolower) %>%
      rename(operator_code=beheer)
    
    vha <<- vha %>% 
      left_join(operator_codes, by="operator_code")
    
    
    # Print the number of features retrieved
    cat("Total VL river segments retrieved:", nrow(vha), "\n")
    #st_write(vha, "C:/temp/rivers/vha.gpkg", delete_dsn = TRUE)
    
    
    
    # Download Brussels data ----
    
    # Define the base WFS URL
    wfs <- "https://ows.environnement.brussels/belb"
    
    # Build the request URL
    build_request_url <- function(start_index) {
      url <- parse_url(wfs)
      url$query <- list(
        service = "wfs",
        request = "GetFeature",
        typename = "water_hydro_netwerk",
        srsName = "EPSG:31370",
        startIndex = start_index,
        maxFeatures = 10000,
        outputFormat = "application/json"
      )
      return(build_url(url))
    }
    
    # Initialize variables
    all_features <- list()
    start_index <- 0
    batch_size <- 10000
    has_more_features <- TRUE
    
    # Loop to fetch data in batches
    while (has_more_features) {
      # Build the request URL for the current batch
      request_url <- build_request_url(start_index)
      
      # Fetch the data
      batch <- read_sf(request_url)
      
      # Check if there are no more features to fetch
      if (nrow(batch) == 0) {
        has_more_features <- FALSE
      } else {
        # Append the fetched features to the list
        all_features <- append(all_features, list(batch))
        # Increment the start index for the next batch
        start_index <- start_index + batch_size
      }
    }
    
    # Combine all fetched features into a single data frame
    bru_segments <<- do.call(rbind, all_features)
    
    
    
    # Define the base WFS URL
    wfs <- "https://ows.environnement.brussels/water"
    
    # Build the request URL
    build_request_url <- function(start_index) {
      url <- parse_url(wfs)
      url$query <- list(
        service = "wfs",
        request = "GetFeature",
        typename = paste(c("water_atlas_classed_watercourse", "water_atlas_unclassed_watercourse"), collapse = ","),
        srsName = "EPSG:31370",
        startIndex = start_index,
        maxFeatures = 10000,
        outputFormat = "application/json"
      )
      return(build_url(url))
    }
    
    
    # Initialize variables
    all_features <- list()
    start_index <- 0
    batch_size <- 10000
    has_more_features <- TRUE
    
    # Loop to fetch data in batches
    while (has_more_features) {
      # Build the request URL for the current batch
      request_url <- build_request_url(start_index)
      
      # Fetch the data
      batch <- read_sf(request_url)
      
      # Check if there are no more features to fetch
      if (nrow(batch) == 0) {
        has_more_features <- FALSE
      } else {
        # Append the fetched features to the list
        all_features <- append(all_features, list(batch))
        # Increment the start index for the next batch
        start_index <- start_index + batch_size
      }
    }
    
    # Combine all fetched features into a single data frame
    bru_classification <<- do.call(rbind, all_features)
    
    
  } else {
    print("No fresh data downloaded because user requested to re-use existing data")
  }
} # end process_fresh_data function  


# TRANSFORM ----
# """""""" ----


### Create SQL for proper ingestion table ----

ingestion_table_sql <- c("DROP TABLE IF EXISTS ingestion.rivers_official CASCADE;
","
CREATE TABLE IF NOT EXISTS ingestion.rivers_official
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
  CONSTRAINT rivers_official_pkey PRIMARY KEY (id)
);",paste0("
WITH prep0_names AS (
	SELECT segment_id as original_id, INITCAP(NULLIF(newcom1,' ')) as municipality, 
		CASE WHEN LEFT(novins1::text,1)::integer=2 then 'Province du Brabant Wallon' 
		WHEN LEFT(novins1::text,1)::integer=5 then 'Province du Hainaut' 
		WHEN LEFT(novins1::text,1)::integer=6 then 'Province de Liège' 
		WHEN LEFT(novins1::text,1)::integer=8 then 'Province du Luxembourg' 
		WHEN LEFT(novins1::text,1)::integer=9 then 'Province de Namur' 
	END AS province
FROM raw_data.wal_river_official
WHERE segment_id IS NOT NULL and newcom1 != ' '
GROUP BY segment_id, municipality, province
UNION ALL
	SELECT segment_id as original_id, INITCAP(NULLIF(newcom2,' ')) as municipality, 
		CASE WHEN LEFT(novins2::text,1)::integer=2 then 'Province du Brabant Wallon' 
		WHEN LEFT(novins2::text,1)::integer=5 then 'Province du Hainaut' 
		WHEN LEFT(novins2::text,1)::integer=6 then 'Province de Liège' 
		WHEN LEFT(novins2::text,1)::integer=8 then 'Province du Luxembourg' 
		WHEN LEFT(novins2::text,1)::integer=9 then 'Province de Namur'
	END AS province
FROM raw_data.wal_river_official
WHERE segment_id IS NOT NULL and newcom2 != ' '
GROUP BY segment_id, municipality, province),

prep_names AS (SELECT original_id, 
			   	string_agg(DISTINCT municipality, '; ') as municipality,
			   	string_agg(DISTINCT province, '; ') as province 
			   FROM prep0_names
			  GROUP BY original_id),

joined AS (
	select r.segment_id as original_id, 
	CASE 
		WHEN categ='NA' then 0
		WHEN categ in ('01', 'N1') then 1
		WHEN categ in ('02', 'N2') then 2
		WHEN categ in ('03', 'N3') then 3
		WHEN categ in ('NC') then 9
		ELSE 9 END --undefined and unclassified are grouped together
	as river_class_code, 
	categ_desc as river_classification_raw,
	CASE WHEN CONCAT((NULLIF(nomb, ' ') || ' '),INITCAP(noma)) IN (' ','','.') THEN 'eaux courantes sans nom' ELSE CONCAT((NULLIF(nomb, ' ') || ' '),INITCAP(noma)) END as name_fr,
	nom_bassin_pg AS river_basin_planification,
	nom_bassin_pr AS river_basin_general,
	p.municipality,
	p.province,
	geometry
	from raw_data.wal_river_official r
	left join prep_names p on r.segment_id=p.original_id),

flattened_segments AS (select original_id, min(river_class_code) as river_class_code, 
	STRING_AGG(distinct river_classification_raw, '; ') as river_classification_raw,
	MIN(name_fr) as name_fr,
	MIN(river_basin_planification) as river_basin_planification,
	MIN(river_basin_general) as river_basin_general,
	MIN(municipality) as municipality,
	MIN(province) as province,
	ST_Union(geometry) as geometry
from joined
where original_id IS NOT null
group by original_id),

regrouped AS (
select * from flattened_segments
union all
select original_id, river_class_code, river_classification_raw, name_fr, river_basin_planification, river_basin_general, municipality, province, geometry
from joined where original_id is null
),

operated AS (select CASE WHEN river_class_code = 0 THEN 'SPW Mobilité et Infrastructures'
	WHEN river_class_code = 1 THEN 'SPW Agriculture, Ressources naturelles et Environnement (Direction des Cours d''Eau non navigables)'
	WHEN river_class_code = 2 THEN province
	WHEN river_class_code = 3 THEN municipality
	ELSE 'Riverain'	END as operator, * 
	from regrouped
	ORDER BY name_fr, operator),
			
clusternumber AS (
  SELECT ST_ClusterDBSCAN(geometry, eps => 0.0000001, minpoints=>1) over (PARTITION BY name_fr, operator) AS clusters,
  		*
  FROM operated
    ORDER BY name_fr, clusters, operator
),
  
wal AS (SELECT 
  CASE WHEN string_agg(DISTINCT original_id,',') IS NOT NULL THEN string_agg(DISTINCT original_id,',') ELSE 'none provided' END as original_id,
  name_fr, operator,
  MIN(river_class_code) AS river_class_code,
  MIN(river_classification_raw) AS river_classification_raw,
  MIN(river_basin_planification) AS river_basin_planification,
  MIN(river_basin_general) AS river_basin_general,
  '",data_list_id_wal,"'::uuid as data_list_id,
  ST_LineMerge(ST_Union(geometry)) AS geometry
FROM clusternumber
GROUP BY name_fr, clusters, operator),

vlabasic AS (
select id as original_id,
CASE WHEN naam IN (' ','','.') THEN 'waterloop zonder naam' ELSE naam END as name_nl,
operator_code,
operator,
beknaam as river_basin_planification,
strmgeb as river_basin_general,
catc as river_class_code,
lblcatc as river_classification_raw,
geometry
from raw_data.vla_river_official),


clustervla AS (
  SELECT ST_ClusterDBSCAN(geometry, eps => 0.0000001, minpoints=>1) over (PARTITION BY name_nl, operator) AS cluster,
  		*
  FROM vlabasic
  ORDER BY original_id, name_nl, operator, cluster
),

vla AS (select string_agg(DISTINCT original_id,',') as original_id, name_nl, operator,
MIN(operator_code) as operator_code,
MIN(river_basin_planification) as river_basin_planification, 
MIN(river_basin_general) as river_basin_general,
MIN(river_class_code) as river_class_code,
MIN(river_classification_raw) as river_classification_raw,
'",data_list_id_vla,"'::uuid as data_list_id,
ST_Union(geometry) as geometry from clustervla
group by name_nl, operator, cluster),

bru as (select 
	id as original_id,
	beheerder_wl || ' / ' || gestionnaire_ce as operator,
	categorie_fr  || ' / ' || categorie_nl as river_classification_raw,
	CASE WHEN codeclass_atlas ='NC' then 9
		WHEN codeclass_atlas ='C' then 3 END as river_class_code,
   	naam as name_nl,
	nom as name_fr,
	'",data_list_id_bru,"'::uuid as data_list_id,
	geometry
from raw_data.bru_river_official
UNION ALL
select 'CAN' as original_id,
	'Haven van Brussel / Port de Bruxelles' as operator,
	NULL as river_classification_raw,
	1 as river_class_code,
	min(naam) as name_nl,
	min(nom) as name_fr,
	'",data_list_id_bru,"'::uuid as data_list_id,
	ST_Union(geometry) as geometry
from raw_data.bru_waterway_segments
where naam = 'Kanaal'
GROUP BY id_code),

all_data AS 
(select original_id, data_list_id, operator, NULL as operator_code, river_classification_raw, river_class_code, 
	NULL as name_nl, name_fr, 
	river_basin_planification, river_basin_general, 
	ST_MakeValid(ST_Transform(geometry,4326)) as geometry from wal
union all
select original_id, data_list_id, operator, operator_code, river_classification_raw, river_class_code, 
	name_nl, NULL as name_fr, 
	river_basin_planification, river_basin_general,
	ST_MakeValid(ST_Transform(geometry,4326)) as geometry from vla
UNION ALL
select original_id, data_list_id, operator, NULL as operator_code, river_classification_raw, river_class_code, 
	name_nl, name_fr, 
	NULL AS river_basin_planification, NULL AS river_basin_general,
	ST_MakeValid(ST_Transform(geometry,4326)) as geometry from bru)

INSERT INTO ingestion.rivers_official
(original_id, name, legend_item, legend_item_id, data_list_id, risk_level, properties, geometry, created_at)
SELECT
original_id,
jsonb_strip_nulls(jsonb_build_object(
  'dut', name_nl,
  'fre', name_fr,
  'und', CONCAT((NULLIF(name_nl, ' ') || ' '),name_fr)))
as name,
jsonb_build_object(
  'dut', CASE WHEN river_class_code = 0 then 'navigeerbare waterloop'
             WHEN river_class_code = 1 then 'waterloop klasse 1'
             WHEN river_class_code = 2 then 'waterloop klasse 2'
             WHEN river_class_code = 3 then 'waterloop klasse 3'
             WHEN river_class_code = 9 then 'niet gedefinieerde waterloop'
             WHEN river_class_code = 99 then 'publieke gracht'
             ELSE 'onbekende waterloop' END,
  'fre', CASE WHEN river_class_code = 0 then 'cours d''eau navigable'
             WHEN river_class_code = 1 then 'cours d''eau de classe 1'
             WHEN river_class_code = 2 then 'cours d''eau de classe 2'
             WHEN river_class_code = 3 then 'cours d''eau de classe 3'
             WHEN river_class_code = 9 then 'cours d''eau non défini'
             WHEN river_class_code = 99 then 'drain public'
             ELSE 'cours d''eau inconnu' END,
    'eng', CASE WHEN river_class_code = 0 then 'navigable waterway'
             WHEN river_class_code = 1 then 'waterway class 1'
             WHEN river_class_code = 2 then 'waterway class 2'
             WHEN river_class_code = 3 then 'waterway class 3'
             WHEN river_class_code = 9 then 'undefined waterway'
             WHEN river_class_code = 99 then 'public ditch'
             ELSE 'unknown waterway' END,
    'ger', CASE WHEN river_class_code = 0 then 'schiffbarer Wasserlauf'
             WHEN river_class_code = 1 then 'Wasserlauf Klasse 1'
             WHEN river_class_code = 2 then 'Wasserlauf Klasse 2'
             WHEN river_class_code = 3 then 'Wasserlauf Klasse 3'
             WHEN river_class_code = 9 then 'nicht definierter Wasserlauf'
             WHEN river_class_code = 99 then 'öffentlicher Graben'
             ELSE 'unbekannter Wasserlauf' END)
  as legend_item,
CASE WHEN river_class_code = 0 then '",li_river_navigable,"'::uuid
             WHEN river_class_code = 1 then '",li_river_class_1,"'::uuid
             WHEN river_class_code = 2 then '",li_river_class_2,"'::uuid
             WHEN river_class_code = 3 then '",li_river_class_3,"'::uuid
             WHEN river_class_code = 9 then '",li_river_unclasified,"'::uuid
             WHEN river_class_code = 99 then '",li_river_ditch,"'::uuid
             END 
  as legend_item_id,
data_list_id,
1 as risk_level,
jsonb_strip_nulls(jsonb_build_object(
  'operator', operator,
  'operator_code', operator_code,
  'river_classification_raw', river_classification_raw,
  'river_class_code', river_class_code,
  'river_basin_planification', river_basin_planification,
  'river_basin_general', river_basin_general))
  as properties,
geometry,
CURRENT_DATE as created_at
FROM all_data
WHERE ST_IsValid(geometry) AND NOT ST_IsEmpty(geometry) AND st_length(st_transform(geometry,31370))>1;"),
  "ALTER TABLE IF EXISTS ingestion.rivers_official OWNER to pgn_group_data_team_w;",
  "GRANT ALL ON TABLE ingestion.rivers_official TO pgn_group_data_team_w;",
  "GRANT ALL ON TABLE ingestion.rivers_official TO pgn_user_airflow;")

  
  # Classification of rivers:
  # 0: navigable (Bru: null AND Kanaal)
  # 1-3: class of non-navigable rivers (in Brussels: classed is always... maybe 3?)
  # 9: not defined, not classified, not in Walloon Atlas and not part of a classified stream, Bru: missing AND not Kanaal
  ## 99: public ditch
  
  
  
  
  ### Execute the SQL commands ----
  create_ingestion_table <- function() {execute_sql_commands(ingestion_table_sql, "Ingestion table")}
  
  
  run_smart_update = function() {
    smart_update_process("rivers_official", 50, 100, 50, format(Sys.Date(), "%Y-%m-%d"), allow_update_even_if_checks_fail=overrule_checks, dry_run=do_dry_run,reuse_ingestion_data=reuse_ingestion_data)
  }
  
  
  
  # Main function -----------------------------------------------------------
  # """"""""""""""""""""----
  
  main_function = function() {
    if (!reuse_ingestion_data) {
      process_fresh_data()
      CreateImportTable(dataset = axes_hydro_wal, schema = "raw_data", table_name = "wal_river_official")
      CreateImportTable(dataset = vha, schema = "raw_data", table_name = "vla_river_official")
      CreateImportTable(dataset = bru_classification, schema = "raw_data", table_name = "bru_river_official")
      CreateImportTable(dataset = bru_segments, schema = "raw_data", table_name = "bru_waterway_segments")
      create_ingestion_table()
    }
    run_smart_update()
  }
  
  if(run_status){
    main_function()
  }
  