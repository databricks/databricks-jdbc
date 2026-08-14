package com.databricks.jdbc.common;

/**
 * Enum representing metadata operation types for SEA metadata logging. These values are sent as
 * HTTP headers to track which metadata operation is being performed.
 */
public enum MetadataOperationType {
  GET_CATALOGS("GetCatalogs", true),
  GET_SCHEMAS("GetSchemas", true),
  GET_TABLES("GetTables", true),
  GET_COLUMNS("GetColumns", true),
  GET_FUNCTIONS("GetFunctions", true),
  GET_PRIMARY_KEYS("GetPrimaryKeys", true),
  // Both JDBC operations use native GetCrossReference; cross-reference filters parent rows
  // client-side.
  GET_IMPORTED_KEYS("GetCrossReference", true),
  GET_CROSS_REFERENCE("GetCrossReference", true),
  GET_PROCEDURES("GetProcedures", false),
  GET_PROCEDURE_COLUMNS("GetProcedureColumns", false);

  private final String headerValue;
  private final boolean thriftNativeSupported;

  MetadataOperationType(String headerValue, boolean thriftNativeSupported) {
    this.headerValue = headerValue;
    this.thriftNativeSupported = thriftNativeSupported;
  }

  /** Returns the header value to be sent in the HTTP request. */
  public String getHeaderValue() {
    return headerValue;
  }

  public boolean isThriftNativeSupported() {
    return thriftNativeSupported;
  }
}
