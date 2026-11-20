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

  /**
   * Whether this operation is issued as a SHOW command rather than an information_schema SELECT.
   */
  public boolean isShowOperation() {
    switch (this) {
      case GET_CATALOGS:
      case GET_SCHEMAS:
      case GET_TABLES:
      case GET_COLUMNS:
      case GET_FUNCTIONS:
      case GET_PRIMARY_KEYS:
      case GET_CROSS_REFERENCE:
        return true;
      default:
        return false;
    }
  }
}
