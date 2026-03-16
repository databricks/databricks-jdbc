package com.databricks.jdbc.common;

/**
 * Enum representing metadata operation types for SEA metadata logging. These values are sent as
 * HTTP headers to track which metadata operation is being performed.
 */
public enum MetadataOperationType {
  GET_CATALOGS("GetCatalogs"),
  GET_SCHEMAS("GetSchemas"),
  GET_TABLES("GetTables"),
  GET_COLUMNS("GetColumns"),
  GET_FUNCTIONS("GetFunctions"),
  GET_PRIMARY_KEYS("GetPrimaryKeys"),
  GET_CROSS_REFERENCE("GetCrossReference");

  private final String headerValue;

  MetadataOperationType(String headerValue) {
    this.headerValue = headerValue;
  }

  /** Returns the header value to be sent in the HTTP request. */
  public String getHeaderValue() {
    return headerValue;
  }
}
