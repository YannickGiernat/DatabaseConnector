# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of DatabaseConnector
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Get available Java heap space
#'
#' @description
#' For debugging purposes: get the available Java heap space.
#'
#' @return
#' The Java heap space (in bytes).
#'
#' @export
getAvailableJavaHeapSpace <- function() {
  availableSpace <- rJava::J("org.ohdsi.databaseConnector.BatchedQuery")$getAvailableHeapSpace()
  return(availableSpace)
}

validateInt64Query <- function() {
  # Validate that communication of 64-bit integers with Java is correct:
  array <- rJava::J("org.ohdsi.databaseConnector.BatchedQuery")$validateInteger64()
  oldClass(array) <- "integer64"
  if (!all.equal(array, bit64::as.integer64(c(1, -1, 8589934592, -8589934592)))) {
    abort("Error converting 64-bit integers between R and Java")
  }
}

parseJdbcColumnData <- function(batchedQuery,
                                columnTypes = NULL) {
  if (is.null(columnTypes)) {
    columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  }
  columns <- vector("list", length(columnTypes))
  for (i in seq_along(columnTypes)) {
    if (columnTypes[i] == 1) {
      column <- rJava::.jcall(batchedQuery, "[D", "getNumeric", as.integer(i))
    } else if (columnTypes[i] == 5) {
      column <- rJava::.jcall(batchedQuery, "[D", "getInteger64", as.integer(i))
      oldClass(column) <- "integer64"
    } else if (columnTypes[i] == 6) {
      column <- rJava::.jcall(batchedQuery, "[I", "getInteger", as.integer(i))
    } else if (columnTypes[i] == 3) {
      column <- rJava::.jcall(batchedQuery, "[I", "getInteger", as.integer(i))
      column <- as.Date(column, origin = "1970-01-01")
    } else if (columnTypes[i] == 4) {
      column <- rJava::.jcall(batchedQuery, "[D", "getNumeric", as.integer(i))
      column <- as.POSIXct(column, origin = "1970-01-01")
    } else if (columnTypes[i] == 7) {
      column <- rJava::.jcall(batchedQuery, "[I", "getBoolean", as.integer(i))
      column <- vapply(column, FUN = function(x) ifelse(x == -1L, NA, as.logical(x)), FUN.VALUE = logical(1))
    } else {
      column <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getString", i)
    }
    columns[[i]] <- column
  }
  names(columns) <- rJava::.jcall(batchedQuery, "[Ljava/lang/String;", "getColumnNames")

  # More efficient than as.data.frame, as it avoids converting row.names to character:
  columns <- structure(columns, class = "data.frame", row.names = seq_len(length(columns[[1]])))
  return(columns)
}

ddlExecutionTimes <- new.env()
insertExecutionTimes <- new.env()

delayIfNecessary <- function(sql, regex, executionTimes, threshold) {
  regexGroups <- stringr::str_match(sql, stringr::regex(regex, ignore_case = TRUE))
  tableName <- regexGroups[3]
  if (!is.na(tableName) && !is.null(tableName)) {
    currentTime <- Sys.time()
    lastExecutedTime <- executionTimes[[tableName]]
    if (!is.na(lastExecutedTime) && !is.null(lastExecutedTime)) {
      delta <- difftime(currentTime, lastExecutedTime, units = "secs")
      if (delta < threshold) {
        Sys.sleep(threshold - delta)
      }
    }
    executionTimes[[tableName]] <- currentTime
  }
}

delayIfNecessaryForDdl <- function(sql) {
  regexForDdl <- "(^CREATE\\s+TABLE\\s+IF\\s+EXISTS|^CREATE\\s+TABLE|^DROP\\s+TABLE\\s+IF\\s+EXISTS|^DROP\\s+TABLE)\\s+([a-zA-Z0-9_$#-]*\\.?\\s*(?:[a-zA-Z0-9_]+)*)"
  delayIfNecessary(sql, regexForDdl, ddlExecutionTimes, 5)
}

delayIfNecessaryForInsert <- function(sql) {
  regexForInsert <- "(^INSERT\\s+INTO)\\s+([a-zA-Z0-9_$#-]*\\.?\\s*(?:[a-zA-Z0-9_]+)*)"
  delayIfNecessary(sql, regexForInsert, insertExecutionTimes, 5)
}

# This helper function helps rlang handle rJava errors thrown by DatabaseConnector
# See https://github.com/tidyverse/dbplyr/issues/1186 and https://github.com/r-lib/rlang/issues/1619 for details
sanitizeJavaErrorForRlang <- function(expr) { tryCatch(expr, error = function(cnd) stop(conditionMessage(cnd))) }

# Detect CTAS and extract created table identifier
isDremioCtas <- function(sql) {
  grepl("^\\s*CREATE\\s+TABLE\\s+.*\\s+AS\\b", sql, ignore.case = TRUE, perl = TRUE)
}

extractCreatedTableFromCtas <- function(sql) {
  # Captures the identifier after CREATE TABLE up to the next whitespace/newline
  # Works with fully-qualified quoted identifiers.
  m <- regexec("(?is)^\\s*CREATE\\s+TABLE\\s+([^\\s]+)\\s+AS\\b", sql, perl = TRUE)
  mm <- regmatches(sql, m)[[1]]
  if (length(mm) >= 2) return(mm[2])
  return(NA_character_)
}

# Poll until the created table is queryable (Dremio-only)
waitForDremioTableVisible <- function(connection, tableIdent,
                                      timeout_secs = 60,
                                      initial_sleep = 0.2,
                                      max_sleep = 2.0,
                                      debug = FALSE) {
  start <- Sys.time()
  sleep <- initial_sleep
  probeSql <- paste0("SELECT 1 FROM ", tableIdent, " LIMIT 1")

  repeat {
    stmt <- NULL
    rs <- NULL
    err <- NULL
    ok <- FALSE

    # 1) Run the probe; capture any error into `err`
    tryCatch({
      stmt <- rJava::.jcall(connection@jConnection,
                            "Ljava/sql/Statement;",
                            "createStatement")

      rs <- rJava::.jcall(stmt,
                          "Ljava/sql/ResultSet;",
                          "executeQuery",
                          probeSql)

      # force evaluation
      rJava::.jcall(rs, "Z", "next")

      ok <- TRUE
    }, error = function(e) {
      err <<- e
    })

    # 2) Always close safely (errors on close must NOT escape)
    if (!is.null(rs))  tryCatch(rJava::.jcall(rs,  "V", "close"), error = function(e) NULL)
    if (!is.null(stmt)) tryCatch(rJava::.jcall(stmt, "V", "close"), error = function(e) NULL)

    # 3) Success
    if (ok) return(invisible(TRUE))

    # 4) Classify error
    msg <- if (!is.null(err)) conditionMessage(err) else ""
    msg_l <- tolower(msg)

    # VERY robust: don’t overfit; Dremio uses a few variants
    is_not_found <- grepl("object", msg_l, fixed = TRUE) && grepl("not found", msg_l, fixed = TRUE)

    cat(sprintf("[probe] ok=%s is_not_found=%s sleep=%.2fs\n", ok, is_not_found, sleep))
    if (!ok) cat("[probe] tableIdent=", tableIdent, "\n")
      cat("\n[waitForDremioTableVisible] probe failed\n")
      cat("SQL: ", probeSql, "\n", sep = "")
      cat("MSG: ", msg, "\n", sep = "")
      cat("is_not_found=", is_not_found, "\n")

    if (!is_not_found) {
      # Not our expected transient condition -> fail loudly with original error
      stop(err)
    }

    # 5) Retry until timeout
    if (as.numeric(difftime(Sys.time(), start, units = "secs")) >= timeout_secs) {
      stop(sprintf("Dremio CTAS table not visible after %ss: %s", timeout_secs, tableIdent))
    }

    Sys.sleep(sleep)
    sleep <- min(max_sleep, sleep * 1.7)
  }
}


lowLevelExecuteSql <- function(connection, sql, verbose = FALSE) {

  log_msg <- function(...) {
    if (verbose) message("[lowLevelExecuteSql] ", ...)
  }

  log_msg("Starte SQL: ", sql)

  statement <- rJava::.jcall(connection@jConnection, "Ljava/sql/Statement;", "createStatement")
  on.exit(sanitizeJavaErrorForRlang(rJava::.jcall(statement, "V", "close")))

  db <- dbms(connection)

  if ((db == "spark") || (db == "iris")) {

    log_msg("DBMS = ", db, " → execute() anstelle von executeLargeUpdate()")
    sanitizeJavaErrorForRlang(rJava::.jcall(statement, "Z", "execute", as.character(sql), check = FALSE))

    rowsAffected <- rJava::.jcall(statement, "I", "getUpdateCount", check = FALSE)
    if (rowsAffected == -1) rowsAffected <- 0

    log_msg("Rows affected: ", rowsAffected)

  } else if (db == "dremio") {

    log_msg("DBMS = dremio → starte erweiterten Sync‑Pfad …")

    # -------------------------------------------------------------------------
    # 1) Hauptausführung
    # -------------------------------------------------------------------------
    hasResult <- sanitizeJavaErrorForRlang(
      rJava::.jcall(statement, "Z", "execute", as.character(sql), check = FALSE)
    )

    log_msg("Statement ausgeführt, beginne Drainen der JDBC‑ResultChains …")

    rowsAffected <- 0
    step <- 1

    repeat {
      if (hasResult) {
        resultSet <- rJava::.jcall(statement, "Ljava/sql/ResultSet;", "getResultSet", check = FALSE)
        if (!rJava::is.jnull(resultSet)) {
          log_msg("  ResultSet #", step, " vorhanden → wird geschlossen")
          sanitizeJavaErrorForRlang(rJava::.jcall(resultSet, "V", "close", check = FALSE))
        } else {
          log_msg("  ResultSet #", step, " = <NULL>")
        }
      } else {
        updateCount <- rJava::.jcall(statement, "I", "getUpdateCount", check = FALSE)
        if (updateCount == -1L) {
          log_msg("  UpdateChain endet nach ", step - 1, " Iterationen.")
          break
        }
        rowsAffected <- rowsAffected + updateCount
        log_msg("  UpdateCount #", step, ": ", updateCount, " (Summe: ", rowsAffected, ")")
      }

      hasResult <- sanitizeJavaErrorForRlang(
        rJava::.jcall(statement, "Z", "getMoreResults", check = FALSE)
      )

      step <- step + 1
    }

    # -------------------------------------------------------------------------
    # 2) Barrier‑Query
    # -------------------------------------------------------------------------



  } else {

    log_msg("DBMS = ", db, " → Standard executeLargeUpdate()")

    rowsAffected <- sanitizeJavaErrorForRlang(
      rJava::.jcall(statement, "J", "executeLargeUpdate", as.character(sql), check = FALSE)
    )

    log_msg("Rows affected: ", rowsAffected)
  }

  if (db == "bigquery") {
    delayIfNecessaryForDdl(sql)
    delayIfNecessaryForInsert(sql)
  }
  if (isDremioCtas(sql)) {
    created <- extractCreatedTableFromCtas(sql)
    if (!is.na(created)) {
      waitForDremioTableVisible(connection, created, timeout_secs = 60)
    }
  }
  log_msg("SQL vollständig abgeschlossen.")

  invisible(rowsAffected)
}

trySettingAutoCommit <- function(connection, value) {
  tryCatch(
  {
    rJava::.jcall(connection@jConnection, "V", "setAutoCommit", value)
  },
    error = function(cond) {
      # do nothing
    }
  )
}

lowLevelDbSendQuery <- function(conn, statement) {
  if (!DBI::dbIsValid(conn)) {
    abort("Connection is closed")
  }
  dbms <- dbms(conn)

  # For Oracle, remove trailing semicolon:
  statement <- gsub(";\\s*$", "", statement)
  tryCatch(
    batchedQuery <- rJava::.jnew(
      "org.ohdsi.databaseConnector.BatchedQuery",
      conn@jConnection,
      statement,
      dbms
    ),
    error = function(error) {
      # Rethrowing error to avoid 'no field, method or inner class called 'use_cli_format''
      # error by rlang (see https://github.com/OHDSI/DatabaseConnector/issues/235)
      rlang::abort(error$message)
    }
  )

  result <- new("DatabaseConnectorJdbcResult",
                content = batchedQuery,
                type = "batchedQuery",
                statement = statement,
                dbms = dbms
  )
  return(result)
}

getBatch <- function(batchedQuery) {
  rJava::.jcall(batchedQuery, "V", "fetchBatch")
  columns <- parseJdbcColumnData(batchedQuery)
}

getAllBatches <- function(batchedQuery) {
  columnTypes <- rJava::.jcall(batchedQuery, "[I", "getColumnTypes")
  if (any(columnTypes == 5)) {
    validateInt64Query()
  }
  columns <- data.frame()
  while (!rJava::.jcall(batchedQuery, "Z", "isDone")) {
    rJava::.jcall(batchedQuery, "V", "fetchBatch")
    batch <- parseJdbcColumnData(batchedQuery,
                                 columnTypes = columnTypes)
    columns <- rbind(columns, batch)
  }
  return(columns)
}
