source("R/LowLevelFunctions.R", local = TRUE)

ident <- '"storagegrid"."dremio-ohdsi-connector"."Synthea27NjParquet".hkj2541is_tmpach_0'
cat("Input  :", ident, "\n")
cat("Quoted :", quoteDremioIdentifierParts(ident), "\n")

# Tokenizer step-by-step
parts <- character(0)
current <- ""
in_quote <- FALSE
for (ch in strsplit(ident, "")[[1]]) {
  if (ch == '"') {
    in_quote <- !in_quote
    current <- paste0(current, ch)
  } else if (ch == '.' && !in_quote) {
    parts <- c(parts, current)
    current <- ""
  } else {
    current <- paste0(current, ch)
  }
}
parts <- c(parts, current)

cat("Parts:\n")
for (i in seq_along(parts)) {
  cat(sprintf("  [%d] >%s<  starts_with_quote=%s\n",
              i, parts[i], substr(parts[i], 1, 1) == '"'))
}

