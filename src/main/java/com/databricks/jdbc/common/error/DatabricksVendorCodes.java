package com.databricks.jdbc.common.error;

/**
 * Centralized registry for Databricks JDBC driver vendor error codes. These codes are included in
 * SQLException.getErrorCode() and formatted as: [Databricks][JDBCDriver](vendor_code)
 */
public enum DatabricksVendorCodes {

  // ========== AUTHENTICATION/AUTHORIZATION ERRORS (500000-599999) ==========

  /** Incorrect or invalid UID parameter provided */
  INCORRECT_UID(500174, "Invalid UID parameter: Expected 'token' or omit UID parameter entirely"),

  /** Incorrect or invalid access token provided */
  INCORRECT_ACCESS_TOKEN(500593, "Incorrect or invalid access token provided"),

  // ========== CONFIGURATION/PARAMETER ERRORS (700000-799999) ==========

  /** Incorrect host URL provided */
  INCORRECT_HOST(700120, "Incorrect host URL provided");

  private final int code;
  private final String message;

  /**
   * Constructor for vendor error codes.
   *
   * @param code The numeric vendor code
   * @param message The error message associated with this vendor code
   */
  DatabricksVendorCodes(int code, String message) {
    this.code = code;
    this.message = message;
  }

  /**
   * Gets the numeric vendor code.
   *
   * @return The vendor error code
   */
  public int getCode() {
    return code;
  }

  /**
   * Gets the error message for this vendor code.
   *
   * @return The error message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Formats this vendor code with the standard Databricks JDBC driver prefix.
   *
   * @return Formatted string like "[Databricks][JDBCDriver](500174)"
   */
  public String formatVendorCode() {
    return String.format("[Databricks][JDBCDriver](%d)", code);
  }

  /**
   * Gets a complete formatted error message with vendor code prefix.
   *
   * @return Complete formatted error message like "[Databricks][JDBCDriver](500174) Invalid UID
   *     parameter..."
   */
  public String getFormattedErrorMessage() {
    return formatVendorCode() + " " + message;
  }

  /**
   * Static utility method to format any vendor code.
   *
   * @param vendorCode The numeric vendor code
   * @return Formatted string like "[Databricks][JDBCDriver](500174)"
   */
  public static String formatVendorCode(int vendorCode) {
    return String.format("[Databricks][JDBCDriver](%d)", vendorCode);
  }

  /**
   * Finds a vendor code enum by its numeric code.
   *
   * @param code The numeric vendor code to search for
   * @return The matching DatabricksVendorCodes enum, or null if not found
   */
  public static DatabricksVendorCodes fromCode(int code) {
    for (DatabricksVendorCodes vendorCode : values()) {
      if (vendorCode.getCode() == code) {
        return vendorCode;
      }
    }
    return null;
  }
}
