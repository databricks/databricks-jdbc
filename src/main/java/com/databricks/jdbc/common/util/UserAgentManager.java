package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.UserAgent;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class UserAgentManager {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(UserAgentManager.class);
  private static final String SDK_USER_AGENT = "databricks-sdk-java";
  private static final String JDBC_HTTP_USER_AGENT = "databricks-jdbc-http";
  private static final String DEFAULT_USER_AGENT = "DatabricksJDBCDriverOSS";
  private static final String CLIENT_USER_AGENT_PREFIX = "Java";
  public static final String USER_AGENT_SEA_CLIENT = "SQLExecHttpClient";
  public static final String USER_AGENT_THRIFT_CLIENT = "THttpClient";
  private static final String VERSION_FILLER = "version";

  /**
   * Parse custom user agent string into name and version components.
   *
   * @param customerUserAgent The custom user agent string (may be URL encoded)
   * @return String array [name, version] or null if parsing fails
   */
  private static String[] parseCustomerUserAgent(String customerUserAgent) {
    try {
      String decodedUA = URLDecoder.decode(customerUserAgent, StandardCharsets.UTF_8);
      int i = decodedUA.indexOf('/');
      String customerName = (i < 0) ? decodedUA : decodedUA.substring(0, i);
      String customerVersion = (i < 0) ? VERSION_FILLER : decodedUA.substring(i + 1);
      return new String[] {customerName, customerVersion};
    } catch (Exception e) {
      LOGGER.debug("Failed to parse customer userAgent entry {}, Error {}", customerUserAgent, e);
      return null;
    }
  }

  /**
   * Set the user agent for the Databricks JDBC driver.
   *
   * @param connectionContext The connection context.
   */
  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    // Set the base product
    UserAgent.withProduct(DEFAULT_USER_AGENT, DriverUtil.getDriverVersion());

    // Set client info (this may trigger getClientType which fetches feature flags)
    UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, connectionContext.getClientUserAgent());

    // Set custom user agent (maintains proper order: base -> client type -> custom)
    if (connectionContext.getCustomerUserAgent() != null) {
      String[] parsed = parseCustomerUserAgent(connectionContext.getCustomerUserAgent());
      if (parsed != null) {
        try {
          UserAgent.withOtherInfo(parsed[0], UserAgent.sanitize(parsed[1]));
        } catch (IllegalArgumentException e) {
          LOGGER.debug(
              "Failed to set user agent for customer userAgent entry {}, Error {}",
              connectionContext.getCustomerUserAgent(),
              e);
        }
      }
    }
  }

  /**
   * Build user agent string for connector service requests (without client type to avoid circular
   * dependency). This is used specifically for feature flags requests since client type is not yet
   * determined.
   *
   * @param connectionContext The connection context.
   * @return User agent string with format: "DatabricksJDBCDriverOSS/version databricks-jdbc-http
   *     jvm/version os/name [CustomApp/version]"
   */
  public static String buildUserAgentForConnectorService(
      IDatabricksConnectionContext connectionContext) {
    StringBuilder userAgent = new StringBuilder();

    // Base product: DatabricksJDBCDriverOSS/version
    userAgent.append(DEFAULT_USER_AGENT).append("/").append(DriverUtil.getDriverVersion());

    // JDBC HTTP identifier
    userAgent.append(" ").append(JDBC_HTTP_USER_AGENT);

    // JVM version
    userAgent
        .append(" jvm/")
        .append(System.getProperty("java.version", "unknown").replace(" ", "_"));

    // OS name
    userAgent.append(" os/").append(System.getProperty("os.name", "unknown").replace(" ", "_"));

    // Custom user agent (if provided)
    if (connectionContext.getCustomerUserAgent() != null) {
      String[] parsed = parseCustomerUserAgent(connectionContext.getCustomerUserAgent());
      if (parsed != null) {
        try {
          userAgent.append(" ").append(parsed[0]).append("/").append(UserAgent.sanitize(parsed[1]));
        } catch (IllegalArgumentException e) {
          LOGGER.debug(
              "Failed to include customer userAgent entry {} in connector service UA, Error {}",
              connectionContext.getCustomerUserAgent(),
              e);
        }
      }
    }

    return userAgent.toString();
  }

  /** Gets the user agent string for Databricks Driver HTTP Client. */
  public static String getUserAgentString() {
    String sdkUserAgent = UserAgent.asString();
    // Split the string into parts
    String[] parts = sdkUserAgent.split("\\s+");
    // User Agent is in format:
    // product/product-version databricks-sdk-java/sdk-version jvm/jvm-version other-info
    // Remove the SDK part from user agent
    StringBuilder mergedString = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      if (parts[i].startsWith(SDK_USER_AGENT)) {
        mergedString.append(JDBC_HTTP_USER_AGENT);
      } else {
        mergedString.append(parts[i]);
      }
      if (i != parts.length - 1) {
        mergedString.append(" "); // Add space between parts
      }
    }
    return mergedString.toString();
  }
}
