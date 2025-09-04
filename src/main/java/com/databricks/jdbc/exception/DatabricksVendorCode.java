package com.databricks.jdbc.exception;

/**
 * Centralized registry for Databricks JDBC driver vendor error codes. These codes are included in
 * SQLException.getErrorCode() and formatted as: [Databricks][JDBCDriver](vendor_code)
 */
public enum DatabricksVendorCode {

  // ========== AUTHENTICATION/AUTHORIZATION ERRORS (500000-599999) ==========

  /** Incorrect or invalid UID parameter provided */
  INCORRECT_UID(
      500174,
      "Invalid UID parameter: Expected 'token' or omit UID parameter entirely",
      "Invalid UID parameter: Expected 'token' or omit UID parameter entirely"),

  /** Incorrect or invalid access token provided */
  INCORRECT_ACCESS_TOKEN(
      500593, "Incorrect or invalid access token provided", "Invalid access token"),

  // ========== CONFIGURATION/PARAMETER ERRORS (700000-799999) ==========

  /** Incorrect host URL provided */
  INCORRECT_HOST(700120, "Incorrect host URL provided", "java.net.UnknownHostException");

  private final int code;
  private final String message;
  private final String upstreamErrorMessage;

  /**
   * Constructor for vendor error codes with upstream error message.
   *
   * @param code The numeric vendor code
   * @param message The error message associated with this vendor code
   * @param upstreamErrorMessage The upstream error message (optional)
   */
  DatabricksVendorCode(int code, String message, String upstreamErrorMessage) {
    this.code = code;
    this.message = message;
    this.upstreamErrorMessage = upstreamErrorMessage.toLowerCase();
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
   * Gets the upstream error message for this vendor code.
   *
   * @return The upstream error message, or null if not provided
   */
  public String getUpstreamErrorMessage() {
    return upstreamErrorMessage;
  }

  /**
   * Finds a vendor code enum by its numeric code.
   *
   * @param code The numeric vendor code to search for
   * @return The matching DatabricksVendorCodes enum, or null if not found
   */
  public static DatabricksVendorCode fromCode(int code) {
    for (DatabricksVendorCode vendorCode : values()) {
      if (vendorCode.getCode() == code) {
        return vendorCode;
      }
    }
    return null;
  }

  /**
   * Extracts a vendor error code from a throwable by examining the exception chain.
   *
   * @param throwable the throwable to analyze, may be null
   * @return the vendor error code if found, or 0 if none detected
   */
  public static int getVendorCode(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      String errorMessage = current.getClass() + ": " + current.getMessage();
      int vendorCode = getVendorCode(errorMessage);
      if (vendorCode != 0) {
        return vendorCode;
      }

      // Move to the next cause in the chain
      current = current.getCause();
    }

    return 0;
  }

  /**
   * Extracts a vendor error code from an error message string using case-insensitive matching.
   *
   * @param exceptionMessage the error message to analyze, may be null
   * @return the vendor error code if found, or 0 if none detected
   */
  public static int getVendorCode(String exceptionMessage) {
    if (exceptionMessage == null) {
      return 0;
    }
    exceptionMessage = exceptionMessage.toLowerCase();
    for (DatabricksVendorCode vendorCode : values()) {
      if (exceptionMessage.contains(vendorCode.getUpstreamErrorMessage())) {
        return vendorCode.getCode();
      }
    }
    return 0;
  }
}
