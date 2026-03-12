## Quick unit tests for Dremio CTAS helper functions in LowLevelFunctions.R
## Run with: Rscript extras/test_ctas_helpers.R

source(file.path(getwd(), "R", "LowLevelFunctions.R"), local = TRUE)

# ── Helpers ────────────────────────────────────────────────────────────────────
pass <- 0L; fail <- 0L

expect_eq <- function(label, got, expected) {
  if (identical(got, expected)) {
    cat(sprintf("  PASS  %s\n", label))
    pass <<- pass + 1L
  } else {
    cat(sprintf("  FAIL  %s\n         got     : %s\n         expected: %s\n",
                label, deparse(got), deparse(expected)))
    fail <<- fail + 1L
  }
}

expect_true  <- function(label, val) expect_eq(label, val, TRUE)
expect_false <- function(label, val) expect_eq(label, val, FALSE)

# ── isDremioCtas ──────────────────────────────────────────────────────────────
cat("\n── isDremioCtas ──\n")
expect_true("single-line CTAS",
  isDremioCtas('CREATE TABLE "a"."b"."c".mytbl AS SELECT 1'))
expect_true("multi-line CTAS (AS on next line)",
  isDremioCtas('CREATE TABLE "a"."b"."c".mytbl\nAS\nSELECT 1'))
expect_true("mixed-case",
  isDremioCtas('create table "a".tbl\nas select 1'))
expect_false("plain SELECT",
  isDremioCtas('SELECT 1 FROM foo'))
expect_false("CREATE TABLE without AS",
  isDremioCtas('CREATE TABLE foo (id INT)'))

# ── extractCreatedTableFromCtas ───────────────────────────────────────────────
cat("\n── extractCreatedTableFromCtas ──\n")
expect_eq("single-line fully-quoted",
  extractCreatedTableFromCtas('CREATE TABLE "a"."b"."c"."mytbl" AS SELECT 1'),
  '"a"."b"."c"."mytbl"')

expect_eq("multi-line, last part unquoted (Achilles style)",
  extractCreatedTableFromCtas(
    'CREATE TABLE "storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet".hkj2541is_tmpach_0\nAS\nSELECT 1'),
  '"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet"."hkj2541is_tmpach_0"')

expect_eq("single-line, last part unquoted",
  extractCreatedTableFromCtas('CREATE TABLE "s"."db"."schema".tbl AS SELECT 1'),
  '"s"."db"."schema"."tbl"')

expect_eq("non-CTAS returns NA",
  extractCreatedTableFromCtas('SELECT 1'),
  NA_character_)

# ── quoteDremioIdentifierParts ─────────────────────────────────────────────────
cat("\n── quoteDremioIdentifierParts ──\n")
expect_eq("all parts already quoted",
  quoteDremioIdentifierParts('"a"."b-c"."Schema"."tbl"'),
  '"a"."b-c"."Schema"."tbl"')

expect_eq("last part unquoted",
  quoteDremioIdentifierParts('"a"."b"."c".tbl'),
  '"a"."b"."c"."tbl"')

expect_eq("fully unquoted path",
  quoteDremioIdentifierParts('a.b.c'),
  '"a"."b"."c"')

expect_eq("part with embedded double-quote gets escaped",
  quoteDremioIdentifierParts('a.b"c.d'),
  '"a"."b""c.d"')

# ── parseDremioIdentifier ──────────────────────────────────────────────────────
cat("\n── parseDremioIdentifier ──\n")

p1 <- parseDremioIdentifier('"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet"."hkj2541is_tmpach_0"')
expect_eq("4-part: catalog", p1$catalog, "storagegrid")
expect_eq("4-part: schema",  p1$schema,  "dremio-ohdsi-connector.Synthea27NjParquet")
expect_eq("4-part: table",   p1$table,   "hkj2541is_tmpach_0")

p2 <- parseDremioIdentifier('"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet".hkj2541is_tmpach_0')
expect_eq("4-part unquoted last: catalog", p2$catalog, "storagegrid")
expect_eq("4-part unquoted last: schema",  p2$schema,  "dremio-ohdsi-connector.Synthea27NjParquet")
expect_eq("4-part unquoted last: table",   p2$table,   "hkj2541is_tmpach_0")

p3 <- parseDremioIdentifier('"catalog"."mytable"')
expect_eq("2-part: catalog", p3$catalog, "catalog")
expect_eq("2-part: table",   p3$table,   "mytable")
expect_false("2-part: schema is not character", is.character(p3$schema))

p4 <- parseDremioIdentifier('"a"."b"."c"')
expect_eq("3-part: catalog", p4$catalog, "a")
expect_eq("3-part: schema",  p4$schema,  "b")
expect_eq("3-part: table",   p4$table,   "c")

# ── probeSql generation (INFORMATION_SCHEMA approach) ─────────────────────────
cat("\n── probeSql generation ──\n")

# Replicate the probe SQL logic from waitForDremioTableVisible inline
make_probe_sql <- function(ident) {
  parsed <- parseDremioIdentifier(ident)
  catalog   <- parsed$catalog
  schema    <- parsed$schema
  tableName <- parsed$table
  sq <- function(s) gsub("'", "''", s, fixed = TRUE)
  if (is.character(schema)) {
    sprintf(
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_CATALOG = '%s' AND TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
      sq(catalog), sq(schema), sq(tableName)
    )
  } else {
    sprintf(
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.\"TABLES\" WHERE TABLE_CATALOG = '%s' AND TABLE_NAME = '%s'",
      sq(catalog), sq(tableName)
    )
  }
}

expect_eq("4-part probe SQL (Achilles production case)",
  make_probe_sql('"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet"."pfk8syn8s_tmpach_0"'),
  'SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_CATALOG = \'storagegrid\' AND TABLE_SCHEMA = \'dremio-ohdsi-connector.Synthea27NjParquet\' AND TABLE_NAME = \'pfk8syn8s_tmpach_0\'')

expect_eq("2-part probe SQL (no schema)",
  make_probe_sql('"catalog"."mytable"'),
  'SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_CATALOG = \'catalog\' AND TABLE_NAME = \'mytable\'')

expect_eq("single-quote in name is escaped",
  make_probe_sql('"cat"."sc"."it\'s_a_table"'),
  'SELECT TABLE_NAME FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_CATALOG = \'cat\' AND TABLE_SCHEMA = \'sc\' AND TABLE_NAME = \'it\'\'s_a_table\'')

# ── Summary ────────────────────────────────────────────────────────────────────
cat(sprintf("\n%d passed, %d failed\n", pass, fail))
if (fail > 0L) quit(status = 1L)


# ── Helpers ────────────────────────────────────────────────────────────────────
pass <- 0L; fail <- 0L

expect_eq <- function(label, got, expected) {
  if (identical(got, expected)) {
    cat(sprintf("  PASS  %s\n", label))
    pass <<- pass + 1L
  } else {
    cat(sprintf("  FAIL  %s\n         got     : %s\n         expected: %s\n",
                label, deparse(got), deparse(expected)))
    fail <<- fail + 1L
  }
}

expect_true  <- function(label, val) expect_eq(label, val, TRUE)
expect_false <- function(label, val) expect_eq(label, val, FALSE)

# ── isDremioCtas ──────────────────────────────────────────────────────────────
cat("\n── isDremioCtas ──\n")
expect_true("single-line CTAS",
  isDremioCtas('CREATE TABLE "a"."b"."c".mytbl AS SELECT 1'))
expect_true("multi-line CTAS (AS on next line)",
  isDremioCtas('CREATE TABLE "a"."b"."c".mytbl\nAS\nSELECT 1'))
expect_true("mixed-case",
  isDremioCtas('create table "a".tbl\nas select 1'))
expect_false("plain SELECT",
  isDremioCtas('SELECT 1 FROM foo'))
expect_false("CREATE TABLE without AS",
  isDremioCtas('CREATE TABLE foo (id INT)'))

# ── extractCreatedTableFromCtas ───────────────────────────────────────────────
cat("\n── extractCreatedTableFromCtas ──\n")
expect_eq("single-line fully-quoted",
  extractCreatedTableFromCtas('CREATE TABLE "a"."b"."c"."mytbl" AS SELECT 1'),
  '"a"."b"."c"."mytbl"')

expect_eq("multi-line, last part unquoted (Achilles style)",
  extractCreatedTableFromCtas(
    'CREATE TABLE "storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet".hkj2541is_tmpach_0\nAS\nSELECT 1'),
  '"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet"."hkj2541is_tmpach_0"')

expect_eq("single-line, last part unquoted",
  extractCreatedTableFromCtas('CREATE TABLE "s"."db"."schema".tbl AS SELECT 1'),
  '"s"."db"."schema"."tbl"')

expect_eq("non-CTAS returns NA",
  extractCreatedTableFromCtas('SELECT 1'),
  NA_character_)

# ── quoteDremioIdentifierParts ─────────────────────────────────────────────────
cat("\n── quoteDremioIdentifierParts ──\n")
expect_eq("all parts already quoted",
  quoteDremioIdentifierParts('"a"."b-c"."Schema"."tbl"'),
  '"a"."b-c"."Schema"."tbl"')

expect_eq("last part unquoted",
  quoteDremioIdentifierParts('"a"."b"."c".tbl'),
  '"a"."b"."c"."tbl"')

expect_eq("fully unquoted path",
  quoteDremioIdentifierParts('a.b.c'),
  '"a"."b"."c"')

expect_eq("part with embedded double-quote gets escaped",
  quoteDremioIdentifierParts('a.b"c.d'),
  '"a"."b""c.d"')

# ── parseDremioIdentifier ──────────────────────────────────────────────────────
cat("\n── parseDremioIdentifier ──\n")

p1 <- parseDremioIdentifier('"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet"."hkj2541is_tmpach_0"')
expect_eq("4-part: catalog", p1$catalog, "storagegrid")
expect_eq("4-part: schema",  p1$schema,  "dremio-ohdsi-connector.Synthea27NjParquet")
expect_eq("4-part: table",   p1$table,   "hkj2541is_tmpach_0")

p2 <- parseDremioIdentifier('"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet".hkj2541is_tmpach_0')
expect_eq("4-part unquoted last: catalog", p2$catalog, "storagegrid")
expect_eq("4-part unquoted last: schema",  p2$schema,  "dremio-ohdsi-connector.Synthea27NjParquet")
expect_eq("4-part unquoted last: table",   p2$table,   "hkj2541is_tmpach_0")

p3 <- parseDremioIdentifier('"catalog"."mytable"')
expect_eq("2-part: catalog", p3$catalog, "catalog")
expect_eq("2-part: table",   p3$table,   "mytable")
# schema should be jnull — check it is NOT a character string
expect_false("2-part: schema is not character", is.character(p3$schema))

p4 <- parseDremioIdentifier('"a"."b"."c"')
expect_eq("3-part: catalog", p4$catalog, "a")
expect_eq("3-part: schema",  p4$schema,  "b")
expect_eq("3-part: table",   p4$table,   "c")

# ── escapeJdbcPattern (via schema roundtrip check) ───────────────────────────
cat("\n── regex-escape in parseDremioIdentifier schema ──\n")
# "dremio-ohdsi-connector" has a hyphen — not special in regex, but dots in
# "dremio-ohdsi-connector.Synthea27NjParquet" ARE special and must be escaped
# when used as a JDBC getTables pattern
escapeJdbcPattern <- function(s) gsub("([.\\^$*+?{}\\[\\]|()])", "\\\\\\1", s)
schema_raw <- "dremio-ohdsi-connector.Synthea27NjParquet"
schema_esc <- escapeJdbcPattern(schema_raw)
expect_eq("dot in schema is escaped for JDBC regex",
          schema_esc,
          "dremio-ohdsi-connector\\.Synthea27NjParquet")

# ── Summary ────────────────────────────────────────────────────────────────────
cat(sprintf("\n%d passed, %d failed\n", pass, fail))
if (fail > 0L) quit(status = 1L)


# ── Helpers ────────────────────────────────────────────────────────────────────
pass <- 0L; fail <- 0L

expect_eq <- function(label, got, expected) {
  if (identical(got, expected)) {
    cat(sprintf("  PASS  %s\n", label))
    pass <<- pass + 1L
  } else {
    cat(sprintf("  FAIL  %s\n         got     : %s\n         expected: %s\n",
                label, deparse(got), deparse(expected)))
    fail <<- fail + 1L
  }
}

expect_true <- function(label, val) expect_eq(label, val, TRUE)
expect_false <- function(label, val) expect_eq(label, val, FALSE)

# ── isDremioCtas ──────────────────────────────────────────────────────────────
cat("\n── isDremioCtas ──\n")
expect_true("single-line CTAS",
  isDremioCtas('CREATE TABLE "a"."b"."c".mytbl AS SELECT 1'))
expect_true("multi-line CTAS (AS on next line)",
  isDremioCtas('CREATE TABLE "a"."b"."c".mytbl\nAS\nSELECT 1'))
expect_true("mixed-case",
  isDremioCtas('create table "a".tbl\nas select 1'))
expect_false("plain SELECT",
  isDremioCtas('SELECT 1 FROM foo'))
expect_false("CREATE TABLE without AS",
  isDremioCtas('CREATE TABLE foo (id INT)'))

# ── extractCreatedTableFromCtas ───────────────────────────────────────────────
cat("\n── extractCreatedTableFromCtas ──\n")
expect_eq("single-line fully-quoted",
  extractCreatedTableFromCtas('CREATE TABLE "a"."b"."c"."mytbl" AS SELECT 1'),
  '"a"."b"."c"."mytbl"')

expect_eq("multi-line, last part unquoted (Achilles style)",
  extractCreatedTableFromCtas(
    'CREATE TABLE "storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet".hkj2541is_tmpach_0\nAS\nSELECT 1'),
  '"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet"."hkj2541is_tmpach_0"')

expect_eq("single-line, last part unquoted",
  extractCreatedTableFromCtas('CREATE TABLE "s"."db"."schema".tbl AS SELECT 1'),
  '"s"."db"."schema"."tbl"')

expect_eq("non-CTAS returns NA",
  extractCreatedTableFromCtas('SELECT 1'),
  NA_character_)

# ── quoteDremioIdentifierParts ─────────────────────────────────────────────────
cat("\n── quoteDremioIdentifierParts ──\n")
expect_eq("all parts already quoted",
  quoteDremioIdentifierParts('"a"."b-c"."Schema"."tbl"'),
  '"a"."b-c"."Schema"."tbl"')

expect_eq("last part unquoted",
  quoteDremioIdentifierParts('"a"."b"."c".tbl'),
  '"a"."b"."c"."tbl"')

expect_eq("fully unquoted path",
  quoteDremioIdentifierParts('a.b.c'),
  '"a"."b"."c"')

expect_eq("part with embedded double-quote gets escaped",
  quoteDremioIdentifierParts('a.b"c.d'),
  '"a"."b""c.d"')

# ── Summary ────────────────────────────────────────────────────────────────────
cat(sprintf("\n%d passed, %d failed\n", pass, fail))
if (fail > 0L) quit(status = 1L)



