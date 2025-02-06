package com.databricks.jdbc.model.core;

/**
 * Enum to represent the disposition of the result data from SQL Execution API.
 *
 * <p>TODO: Replace this class with the corresponding SDK implementation once it becomes available
 */
public enum Disposition {
  /** Result data is returned as external DBFS links in ARROW format. */
  EXTERNAL_LINKS,
  /** Result data is returned inline in JSON format. */
  INLINE,
  /** Result data is returned as a mix of inline and external links in ARROW format. */
  INLINE_OR_EXTERNAL_LINKS
}
