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



