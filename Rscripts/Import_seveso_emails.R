## ---------------------------
##
## Script name: Read Seveso emails and prepare for import into Paragon
##
## Purpose of script: Load emails with Paragon data and load them to Postgres for further processing. Move processed emails to archive. Send e-mail warnings if there are problems
##
## Author: Joost Schouppe
##
## Date Created: 2026-06-29
##
##
## ---------------------------


# Load variables -----------------------------------------------------------
#  """""""""""""""""" ----------------------

# connection details
db_host_name <- Sys.getenv("POSTGRES_HOST_NAME")
postgres_user <- Sys.getenv("POSTGRES_USER")
postgres_password <- Sys.getenv("POSTGRES_PASSWORD")
db_name<- Sys.getenv("POSTGRES_DB_NAME_CURATED")

rscript_folder <- Sys.getenv("LOCAL_RSCRIPT_PATH")
source(paste0(rscript_folder,"/utils_updated_check_protoanchors.R"))
source(paste0(rscript_folder,"/utils.R"))

library(httr2)
library(jsonlite)
library(rvest)


# Connection details for shared mailbox
tenant_id <- Sys.getenv("AIRFLOW_AT_NCCN_TENANT_ID")
client_id <- Sys.getenv("AIRFLOW_AT_NCCN_CLIENT_ID")
client_secret <- Sys.getenv("AIRFLOW_AT_NCCN_SECRET")
target_mailbox <- Sys.getenv("AIRFLOW_AT_NCCN_EMAIL")
seveso_notifications <- Sys.getenv("SEVESO_NOTIFICATIONS")

# Set location for files received by email
offline_storage <-Sys.getenv("OFFLINE_STORAGE")
current_versions_folder <- "seveso/be"
archive_versions_folder <- "seveso/be/archive"

# Expected senders config
# `pattern`    - if set, an attachment matching this regex is expected/accepted
# `check_text` - if set, this text (loose, case-insensitive) in the body is accepted
#                 as a fallback when there's no matching attachment
sender_config <- list(
  brussels = list(
    email      = "ugeebelen@leefmilieu.brussels",
    pattern    = "^LIST_\\d{8}_GegevensBedrijven\\.xlsx$",
    check_text = "geen wijziging"
  ),
  fod = list(
    email      = "Koen.BIERMANS@werk.belgie.be",
    pattern    = "^seveso ACR \\d{8}\\.xlsx$"
  ),
  flanders = list(
    email      = "karolien.schoonjans@vlaanderen.be",
    pattern    = "^\\d{8}_VlaamsGewest\\.xlsx$"
  ),
  wallonia = list(
    email      = "frederic.gilles@spw.wallonie.be",
    pattern    = "^\\d{2} - Région Wallonne - \\w+ \\d{4}\\.xlsx$",
    check_text = "Rien de nouveau"
  )
)

# four mails expected
# warn if mail from other person arrived
# check for presence of mail with attachment
# if mail has no attachment, just archive mail
# if mail has attachment in expected format, copy to current versions, and move current file to archived versions
# when the four expected mails have been received, trigger import process
# raise an alarm when there is no fourth mail for two weeks after receiving the first mail

#readRenviron("C:/projects/pgn-data-airflow/.Renviron")

# get the current month we are working on ----
## either we are working on the month of the as of yet incomplete batch
## or on the data for the month after the last completed batch
# this should usually be the same as the current month, but it does happen that the last file is received after the end of the month

get_current_month <- function() {

con_pg <- get_con()

batch_state <- dbGetQuery(con_pg, "
  SELECT 
    month_id,
    COUNT(*) FILTER (WHERE processing_status = 'ok') AS ok_count,
    MAX(batch_status) AS batch_status
  FROM raw_data.seveso_email_intake_archive
  GROUP BY month_id
  ORDER BY month_id DESC
  LIMIT 2
")

dbDisconnect(con_pg)

current_month_id <- if (nrow(batch_state) == 0) {
  # table is empty, start fresh with current month
  as.integer(format(Sys.Date(), "%Y%m"))
} else if (batch_state$batch_status[1] %in% c("raw_data_processed")) {
  # most recent batch is done, move to next month
  latest <- as.character(batch_state$month_id[1])
  year   <- as.integer(substr(latest, 1, 4))
  month  <- as.integer(substr(latest, 5, 6))
  if (month == 12) as.integer(paste0(year + 1, "01"))
  else             as.integer(paste0(year, sprintf("%02d", month + 1)))
} else {
  # most recent batch is incomplete, keep working on it
  batch_state$month_id[1]
}
return(current_month_id)
}

current_month_id <- get_current_month()
working_month_id <- as.integer(format(Sys.Date(), "%Y%m"))
message("Working on month_id: ", current_month_id)
if (working_month_id > current_month_id) {
  message("We're still waiting for last month's data!")
}
if (working_month_id < current_month_id) {
  stop(paste0("Wanting to start on ",current_month_id," but were still in month ",working_month_id))
}


# get access token ----
token <- get_office365_email_token(tenant_id, client_id, client_secret)

# get folder ids
folders<-email_list_folders(token,target_mailbox)
postvak_in_id <- folders$id[folders$displayName == "Postvak IN"]
archive_folder_id <- folders$id[folders$displayName == "Archiveren"]



# fetch unprocessed messages from inbox ----


inbox_url <- sprintf(
  "https://graph.microsoft.com/v1.0/users/%s/mailFolders/%s/messages",
  utils::URLencode(target_mailbox, reserved = TRUE),
  postvak_in_id
)
raw_messages <- graph_get(
  token, inbox_url,
  `$top`    = 20,
  `$select` = "id,subject,from,receivedDateTime,hasAttachments,body",
  `$orderby` = "receivedDateTime asc"
)

message(length(raw_messages), " messages found in inbox ")
if (length(raw_messages)>15) {
  stop(paste0("Please clean the inbox, there are ",length(raw_messages)," or more messages there!"))
}

# Classify each message ----
# Reverse lookup: email address -> sender key
email_to_key <- setNames(
  names(sender_config),
  sapply(sender_config, `[[`, "email")
)

classify_message <- function(msg) {
  from_addr  <- msg$from$emailAddress$address
  sender_key <- email_to_key[from_addr]
  
  if (is.na(sender_key)) {
    return(list(
      message_id     = msg$id,
      sender_email   = from_addr,
      sender_key     = NA_character_,
      received       = msg$receivedDateTime,
      has_attachment = msg$hasAttachments,
      subject        = msg$subject,
      body_text      = strip_html(msg$body$content),
      status         = "unexpected_sender"
    ))
  }
  
  list(
    message_id     = msg$id,
    sender_email   = from_addr,
    sender_key     = unname(sender_key),
    received       = msg$receivedDateTime,
    has_attachment = msg$hasAttachments,
    subject        = msg$subject,
    body_text      = strip_html(msg$body$content),
    status         = "pending"
  )
}

classified <- lapply(raw_messages, classify_message)
message("Classified ", length(classified), " messages")
if (length(classified) > 0) {
message("  Unexpected senders: ", sum(sapply(classified, \(x) x$status == "unexpected_sender")))
message("  Known senders:      ", sum(sapply(classified, \(x) x$status == "pending")))
}


# Loop over all classified messages and validate ----
processed_messages <- lapply(classified, function(msg) {
  
  # skip unexpected senders, just log them
  if (msg$status == "unexpected_sender") {
    message("WARNING: unexpected sender: ", msg$sender_email)
    return(msg)
  }
  
  cfg <- sender_config[[msg$sender_key]]
  has_pattern    <- !is.null(cfg$pattern)
  has_check_text <- !is.null(cfg$check_text)
  
  check_body_text <- function() grepl(cfg$check_text, msg$body_text, ignore.case = TRUE)
  
  # --- no attachment: fall back to body text check if configured ---
  if (!msg$has_attachment) {
    if (has_check_text && check_body_text()) {
      msg$status          <- "ok"
      msg$validation_note <- NA_character_
    } else {
      msg$status          <- "no_attachment"
      msg$validation_note <- if (has_check_text) {
        sprintf("No attachment and expected text '%s' not found in body", cfg$check_text)
      } else {
        "Expected attachment but none found"
      }
    }
    msg$attachment_filename <- NA_character_
    msg$attachment_data     <- NULL
    message("  ", msg$sender_key, ": status = ", msg$status)
    return(msg)
  }
  
  # --- attachment present but no pattern configured for this sender: fall back to text check ---
  if (!has_pattern) {
    if (has_check_text && check_body_text()) {
      msg$status          <- "ok"
      msg$validation_note <- NA_character_
    } else {
      msg$status          <- "warning"
      msg$validation_note <- if (has_check_text) {
        sprintf("Unexpected attachment and expected text '%s' not found in body", cfg$check_text)
      } else {
        "Unexpected attachment received"
      }
    }
    msg$attachment_filename <- NA_character_
    msg$attachment_data     <- NULL
    message("  ", msg$sender_key, ": status = ", msg$status)
    return(msg)
  }
  
  att_url <- sprintf(
    "https://graph.microsoft.com/v1.0/users/%s/messages/%s/attachments",
    utils::URLencode(target_mailbox, reserved = TRUE),
    utils::URLencode(msg$message_id)
  )
  attachments <- graph_get(token, att_url)
  xlsx_atts   <- Filter(\(a) grepl("\\.xlsx$", a$name, ignore.case = TRUE), attachments)

  
  if (length(xlsx_atts) == 0) {
    # attachment(s) present but none are usable xlsx: fall back to text check
    if (has_check_text && check_body_text()) {
      msg$status          <- "ok"
      msg$validation_note <- NA_character_
    } else {
      msg$status          <- "no_attachment"
      msg$validation_note <- "No xlsx attachment found"
    }
    msg$attachment_filename <- NA_character_
    msg$attachment_data     <- NULL
  } else if (length(xlsx_atts) > 1) {
    msg$status              <- "warning"
    msg$validation_note     <- sprintf("%d xlsx attachments found, expected 1", length(xlsx_atts))
    msg$attachment_filename <- xlsx_atts[[1]]$name
    msg$attachment_data     <- xlsx_atts[[1]]$contentBytes
  } else {
    att      <- xlsx_atts[[1]]
    filename <- att$name
    matches  <- grepl(cfg$pattern, filename)
    msg$status              <- if (matches) "ok" else "warning"
    msg$validation_note     <- if (matches) NA_character_ else sprintf("Filename '%s' does not match expected pattern", filename)
    msg$attachment_filename <- filename
    msg$attachment_data     <- att$contentBytes
  }
  
  message("  ", msg$sender_key, ": ", msg$attachment_filename, " — ", msg$status)
  msg
})

message(length(processed_messages), " messages processed")


# Upload xlsx attachments to Postgres via CreateImportTable ----

for (msg in processed_messages) {
  
  # only upload if we have a file and it passed validation — CreateImportTable fully
  # overwrites the target table, so a "warning" (wrong filename, unexpected extra
  # attachment, ...) must never be allowed to clobber a previously good table
  if (is.null(msg$attachment_data) || is.na(msg$sender_key)) next
  if (msg$status != "ok") {
    message("  ", msg$sender_key, ": skipping upload, status = ", msg$status)
    next
  }
  
  # decode base64 to raw bytes, read into R via a temp file
  raw_bytes <- base64enc::base64decode(msg$attachment_data)
  tmp <- tempfile(fileext = ".xlsx", tmpdir = Sys.getenv("OFFLINE_STORAGE"))
  writeBin(raw_bytes, tmp)
  
  new_dataset <- tryCatch(
    readxl::read_xlsx(tmp),
    error = function(e) {
      stop(sprintf("Could not read xlsx attachment '%s' for sender '%s': %s",
                   msg$attachment_filename, msg$sender_key, e$message))
    }
  )
  
  # add month_id column for traceability
  new_dataset$month_id <- current_month_id
  
  table_name <- paste0("seveso_", msg$sender_key)
  
  tryCatch({
    CreateImportTable(
      dataset    = new_dataset,
      schema     = "raw_data",
      table_name = table_name
    )
    message("  ", msg$sender_key, ": uploaded to raw_data.", table_name)
  }, error = function(e) {
    stop("  WARNING: DB upload failed for ", msg$sender_key, ": ", e$message)
  })
  
  unlink(tmp)
}

message("Step 5 OK: uploads complete (or not needed)")

# Move all processed messages to Archiveren folder ----

for (msg in processed_messages) {
  move_url <- sprintf(
    "https://graph.microsoft.com/v1.0/users/%s/messages/%s/move",
    utils::URLencode(target_mailbox, reserved = TRUE),
    utils::URLencode(msg$message_id)
  )
  
  move_resp <- request(move_url) |>
    req_headers(
      Authorization  = paste("Bearer", token),
      `Content-Type` = "application/json"
    ) |>
    req_body_json(list(destinationId = archive_folder_id)) |>
    req_error(is_error = \(r) FALSE) |>
    req_perform()
  
  if (resp_status(move_resp) == 201) {
    message("  ", msg$sender_key %||% msg$sender_email, ": message moved to Archiveren")
  } else {
    err <- move_resp |> resp_body_json()
    message("  WARNING: could not move message for ", msg$sender_key %||% msg$sender_email, " — ", err$error$message)
  }
}



# Build intake_log from all processed messages ----
intake_log <- do.call(rbind, lapply(processed_messages, function(msg) {
  data.frame(
    sender_email        = msg$sender_email,
    attachment_filename = msg$attachment_filename %||% NA_character_,
    received_timestamp  = msg$received,
    created_at          = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    processing_status   = msg$status,
    month_id            = current_month_id,
    validation_note     = msg$validation_note %||% NA_character_,
    batch_status        = "incomplete",
    stringsAsFactors    = FALSE
  )
}))

message(nrow(intake_log), " rows in intake_log")
print(intake_log[, c("sender_email", "processing_status", "attachment_filename")])

# send to Postgres with new AppendImportTable function
if (is.data.frame(intake_log)) {
  AppendImportTable(
    dataset    = intake_log,
    schema     = "raw_data",
    table_name = "seveso_email_intake_archive"
  )
}
 
# Check batch completeness and trigger import if ready ----
con_pg <- get_con()

current_batch <- dbGetQuery(con_pg, sprintf("
  SELECT sender_email, processing_status, received_timestamp
  FROM raw_data.seveso_email_intake_archive
  WHERE month_id = %d
", current_month_id))

expected_emails <- sapply(sender_config, `[[`, "email")
ok_emails <- current_batch$sender_email[current_batch$processing_status == "ok"]
all_ok <- all(expected_emails %in% ok_emails)

if (all_ok) {
  message("All four senders OK — marking complete and triggering import")
  
  # mark complete
  dbExecute(con_pg, sprintf("
    UPDATE raw_data.seveso_email_intake_archive
    SET batch_status = 'import_triggered'
    WHERE month_id = %d
  ", current_month_id))
  
  dbDisconnect(con_pg)
  # The actual triggering is in the DAG, which will next check the status of the updated table
  
} else {
  missing_keys <- names(expected_emails[!expected_emails %in% ok_emails])
  message("Batch incomplete, missing: ", paste(missing_keys, collapse = ", "))
  
  # check if alarm should be sent
  ok_rows <- current_batch[current_batch$processing_status == "ok", ]
  
  already_alarmed <- dbGetQuery(con_pg, sprintf("
        SELECT COUNT(*) as n 
        FROM raw_data.seveso_email_intake_archive
        WHERE month_id = %d AND batch_status = 'alarm_sent'
      ", current_month_id))
  
  if (working_month_id > current_month_id && all_ok == FALSE && already_alarmed$n == 0) {
    email_sent <- send_an_email(
      token   = token,
      from    = target_mailbox,
      to      = seveso_notifications,
      subject = sprintf("SEVESO intake incomplete for month %d", current_month_id),
      body = paste0(
        sprintf(
          "Batch for month %d is incomplete and it's already a different month.\nStill missing: %s",
          current_month_id, working_month_id, paste(missing_keys, collapse = ", ")
        ),
        "\nThere will be no further warnings until this batch is complete. To reset warnings, manually edit raw_data.seveso_email_intake_archive table"
      )
    )
    if (email_sent) {
      dbExecute(con_pg, sprintf("
          UPDATE raw_data.seveso_email_intake_archive
          SET batch_status = 'alarm_sent'
          WHERE month_id = %d
        ", current_month_id))
    } else {
      message("Alarm email failed to send — will retry on next run")
    }

  }
  
  
  
  if (nrow(ok_rows) > 0) {
    oldest_ok <- min(as.POSIXct(ok_rows$received_timestamp))
    days_waiting <- as.numeric(difftime(Sys.time(), oldest_ok, units = "days"))
    
    if (days_waiting > 14) {
      # check we haven't already sent the alarm this month
      if (already_alarmed$n == 0) {
        message("Step 8: >14 days since first mail, sending alarm")
        
        email_sent <- send_an_email(
          token   = token,
          from    = target_mailbox,
          to      = seveso_notifications,
          subject = sprintf("SEVESO intake incomplete for month %d", current_month_id),
          body = paste0(
            sprintf(
              "Batch for month %d is incomplete after %.0f days.\nStill missing: %s",
              current_month_id, days_waiting, paste(missing_keys, collapse = ", ")
            ),
            "\nThere will be no further warnings until this batch is complete. To reset warnings, manually edit raw_data.seveso_email_intake_archive table"
          )
        )
        
        if (email_sent) {
          dbExecute(con_pg, sprintf("
            UPDATE raw_data.seveso_email_intake_archive
            SET batch_status = 'alarm_sent'
            WHERE month_id = %d",
            current_month_id))  
         } else {
           message("Alarm email failed to send — will retry on next run")
        }
      } else {
        message("Step 8: alarm already sent this month, skipping")
      }
    } else {
      message("Step 8: waiting, ", round(days_waiting, 1), " days since first mail")
    }
  }
  
  dbDisconnect(con_pg)
}





# Manual steps ----
# run this code manually when the above script did not result in a run, but you have some data you can manually upload to Postgres
manual_import <- FALSE
if (manual_import==TRUE) {
  VL <- readxl::read_xlsx("C:/temp/seveso/20260907_VlaamsGewest_uitgebreid.xlsx")
  BRU <-readxl::read_xlsx("C:/temp/seveso/LIST_20260708_GegevensBedrijven.xlsx")
  WAL <-readxl::read_xlsx("C:/temp/seveso/Seveso data - Région Wallonne - V1.xlsx")
  CreateImportTable(
    dataset    = VL,
    schema     = "raw_data",
    table_name = "seveso_flanders"
  )
  CreateImportTable(
    dataset    = BRU,
    schema     = "raw_data",
    table_name = "seveso_brussels"
  )
  CreateImportTable(
    dataset    = WAL,
    schema     = "raw_data",
    table_name = "seveso_wallonia"
  )
}
