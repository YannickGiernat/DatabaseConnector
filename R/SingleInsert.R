# @file CtasHack.R
#
# Copyright 2023 Observational Health Data Sciences and Informatics
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

# The CTAS hack is used for platforms where INSERT statements are extremely slow, and we
# are inserting into a new table. Rather than creating the table and then inserting the data, we
# create the table using a CTAS statement. Because there are limits to the number of rows that
# can be added in a single CTAS statement, we use recursive CTAS statements, each no larger than
# some number specified per platform.
#

formatInsertRow <- function(data, castValues, sqlDataTypes) {
  if (castValues) {
    data <- castValues(data, sqlDataTypes)
  }
  return(paste0("(", paste(data, collapse = ","), ")"))
}

singleInsert <- function(connection, sqlTableName, tempTable, sqlFieldNames, sqlDataTypes, data, progressBar, tempEmulationSchema) {
  logTrace(sprintf("Inserting %d rows into table '%s' using CTAS hack", nrow(data), sqlTableName))

  assign("noLogging", TRUE, envir = globalVars)
  on.exit(
    assign("noLogging", NULL, envir = globalVars)
  )
  startTime <- Sys.time()
  batchSize <- 1000

  # Insert data in batches in temp tables using CTAS:
  if (progressBar) {
    pb <- txtProgressBar(style = 3)
  }

  for (start in seq(1, nrow(data), by = batchSize)) {
    if (progressBar) {
      setTxtProgressBar(pb, start / nrow(data))
    }
    end <- min(start + batchSize - 1, nrow(data))
    batch <- toStrings(data[start:end, , drop = FALSE], sqlDataTypes)

    varAliases <- strsplit(sqlFieldNames, ",")[[1]]
    # First line gets type information:
    valueString <- formatInsertRow(batch[1, , drop = FALSE], castValues = TRUE, sqlDataTypes = sqlDataTypes)
    if (end > start) {
      # Other lines only get type information if BigQuery:
      valueString <- paste(c(valueString, apply(batch[2:nrow(batch), , drop = FALSE],
        MARGIN = 1,
        FUN = formatInsertRow,
        castValues = TRUE,
        sqlDataTypes = sqlDataTypes
      )),
      collapse = ",\n "
      )
    }
    sql <- paste("INSERT INTO ",
      sqlTableName,
      " (",
      sqlFieldNames,
      ") VALUES ",
      valueString,
      sep = ""
    )
    sql <- SqlRender::translate(sql, targetDialect = dbms(connection), tempEmulationSchema = tempEmulationSchema)
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  if (progressBar) {
    setTxtProgressBar(pb, 1)
    close(pb)
  }
  delta <- Sys.time() - startTime
  inform(paste("Inserting data took", signif(delta, 3), attr(delta, "units")))
}