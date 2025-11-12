# Test script for DuckDB config functionality
library(DatabaseConnector)
library(duckdb)

# Create a temporary DuckDB file
temp_db_file <- tempfile(fileext = ".duckdb")

# Test 1: Connection without config (existing behavior)
cat("Test 1: Connection without config\n")
connectionDetails1 <- createConnectionDetails(
  dbms = "duckdb",
  server = temp_db_file
)

tryCatch({
  connection1 <- connect(connectionDetails1)
  cat("✓ Connection without config successful\n")
  disconnect(connection1)
}, error = function(e) {
  cat("✗ Connection without config failed:", e$message, "\n")
})

# Test 2: Connection with config (new functionality)
cat("\nTest 2: Connection with config\n")
connectionDetails2 <- createConnectionDetails(
  dbms = "duckdb",
  server = temp_db_file,
  extraSettings = list(
    config = list(
      memory_limit = "8GB",
      preserve_insertion_order = "false"
    )
  )
)

tryCatch({
  connection2 <- connect(connectionDetails2)
  cat("✓ Connection with config successful\n")
  
  # Verify that the config was applied by checking memory_limit setting
  result <- querySql(connection2, "SELECT current_setting('memory_limit') as memory_limit")
  cat("Memory limit setting:", result$MEMORY_LIMIT, "\n")
  
  disconnect(connection2)
}, error = function(e) {
  cat("✗ Connection with config failed:", e$message, "\n")
})

# Test 3: Connection with invalid config (error handling)
cat("\nTest 3: Connection with invalid config\n")
connectionDetails3 <- createConnectionDetails(
  dbms = "duckdb",
  server = temp_db_file,
  extraSettings = list(
    config = list(
      invalid_setting = "invalid_value"
    )
  )
)

tryCatch({
  connection3 <- connect(connectionDetails3)
  cat("✗ Connection with invalid config should have failed\n")
  disconnect(connection3)
}, error = function(e) {
  cat("✓ Connection with invalid config correctly failed:", e$message, "\n")
})

# Cleanup
unlink(temp_db_file)
cat("\nTest completed\n")