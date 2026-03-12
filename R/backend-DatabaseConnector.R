# Copyright 2026 Observational Health Data Sciences and Informatics
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

#' @export
#' @importFrom dbplyr dbplyr_edition
#' 
#' @param con Database connection 
dbplyr_edition.DatabaseConnectorConnection <- function(con) {
  2L
}

#' @importFrom dbplyr sql_query_select
#' @export
dbplyr::sql_query_select

#' @export
sql_query_select.DatabaseConnectorJdbcConnection <- function(con,
                                                             select,
                                                             from,
                                                             where = NULL,
                                                             group_by = NULL,
                                                             having = NULL,
                                                             window = NULL,
                                                             order_by = NULL,
                                                             limit = NULL,
                                                             distinct = FALSE,
                                                             ...,
                                                             subquery = FALSE,
                                                             lvl = 0) {
  switch(dbms(con),
    "sql server" = utils::getFromNamespace("sql_query_select.Microsoft SQL Server", "dbplyr")(con,
      select,
      from,
      where,
      group_by,
      having,
      window,
      order_by,
      limit,
      distinct,
      ...,
      subquery,
      lvl),
    "oracle" = utils::getFromNamespace("sql_query_select.Oracle", "dbplyr")(con,
      select,
      from,
      where,
      group_by,
      having,
      window,
      order_by,
      limit,
      distinct,
      ...,
      subquery,
      lvl),
    NextMethod()  
  )
}

# Export a sql_translation method for JDBC connections that allows dplyr code to 
# be correctly translated to SQL code. These functions are 
# imported from the dbplyr package.

#' @export
#' @importFrom dbplyr sql_translation 
sql_translation.DatabaseConnectorJdbcConnection <- function(con) {
  
  switch(dbms(con),
     "oracle" = utils::getFromNamespace("sql_translation.Oracle", "dbplyr")(con),
     "postgresql" = utils::getFromNamespace("sql_translation.PqConnection", "dbplyr")(con),
     "redshift" = utils::getFromNamespace("sql_translation.RedshiftConnection", "dbplyr")(con),
     "sql server" = utils::getFromNamespace("sql_translation.Microsoft SQL Server", "dbplyr")(con),
     "bigquery" = utils::getFromNamespace("sql_translation.BigQueryConnection", "bigrquery")(con),
     "spark" = utils::getFromNamespace("sql_translation.Spark SQL", "dbplyr")(con),
     "snowflake" = utils::getFromNamespace("sql_translation.Snowflake", "dbplyr")(con),
     "synapse" = utils::getFromNamespace("sql_translation.Microsoft SQL Server", "dbplyr")(con),
     "iris" = utils::getFromNamespace("sql_translation.PqConnection", "dbplyr")(con),
     rlang::abort("Sql dialect is not supported!")) 
}

# In addition to JDBC connections, DatabaseConnector also wraps duckdb and sqlite DBI connections
# Export a method to get correct dplyr to SQL translation environments when using these wrapped DBI connections
# See https://github.com/OHDSI/DatabaseConnector/issues/324

#' @export
#' @importFrom dbplyr sql_translation 
sql_translation.DatabaseConnectorDbiConnection <- function(con) {
  
  switch(dbms(con),
    "sqlite" = utils::getFromNamespace("sql_translation.SQLiteConnection", "dbplyr")(con),
    "duckdb" = utils::getFromNamespace("sql_translation.duckdb_connection", "duckdb")(con),
    NextMethod()) 
}


#' @importFrom dbplyr sql_escape_logical
#' @export
dbplyr::sql_escape_logical

#' @export
sql_escape_logical.DatabaseConnectorJdbcConnection <- function(con, x) {
  if (dbms(con) == "sql server") {
    dplyr::if_else(x, "1", "0", missing = "NULL")
  } else {
    NextMethod()
  }
}

