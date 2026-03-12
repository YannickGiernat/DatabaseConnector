library(DatabaseConnector)
cat("Version:", as.character(packageVersion("DatabaseConnector")), "\n")
cat("quoteDremioIdentifierParts exists:", exists("quoteDremioIdentifierParts", envir = asNamespace("DatabaseConnector")), "\n")
cat("isDremioCtas exists:", exists("isDremioCtas", envir = asNamespace("DatabaseConnector")), "\n")
cat("extractCreatedTableFromCtas exists:", exists("extractCreatedTableFromCtas", envir = asNamespace("DatabaseConnector")), "\n")

# Test quoting with exact identifier from error log
ident <- '"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet".hkj2541is_tmpach_0'
quoted <- DatabaseConnector:::quoteDremioIdentifierParts(ident)
cat("Input  :", ident, "\n")
cat("Quoted :", quoted, "\n")
expected <- '"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet"."hkj2541is_tmpach_0"'
cat("Correct:", identical(quoted, expected), "\n")

# Test isDremioCtas with multiline SQL
sql_ml <- 'CREATE TABLE "storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet".hkj2541is_tmpach_0\nAS\nSELECT 1'
cat("isDremioCtas multiline:", DatabaseConnector:::isDremioCtas(sql_ml), "\n")
cat("extract multiline     :", DatabaseConnector:::extractCreatedTableFromCtas(sql_ml), "\n")

