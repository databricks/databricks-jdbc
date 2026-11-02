package com.databricks.jdbc.model.core;

/** SQL Execution API result formats, including the APC-only native Arrow-batch shape. */
public enum Format {
  ARROW_STREAM,
  ARROW_BATCH,
  CSV,
  JSON_ARRAY
}
