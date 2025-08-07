package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.core.UserAgent;

public class UserAgentManager {
  private static final String SDK_USER_AGENT = "databricks-sdk-java";
  private static final String JDBC_HTTP_USER_AGENT = "databricks-jdbc-http";
  private static final String DEFAULT_USER_AGENT = "DatabricksJDBCDriverOSS";
  private static final String CLIENT_USER_AGENT_PREFIX = "Java";
  public static final String USER_AGENT_SEA_CLIENT = "SQLExecHttpClient";
  public static final String USER_AGENT_THRIFT_CLIENT = "THttpClient";
  private static final String APP_NAME_SYSTEM_PROPERTY = "app.name";
  private static final String VERSION_FILLER = "version";

  /**
   * Set the user agent for the Databricks JDBC driver.
   *
   * @param connectionContext The connection context.
   */
  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    // Set the base product and client info
    UserAgent.withProduct(DEFAULT_USER_AGENT, DriverUtil.getDriverVersion());
    UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, connectionContext.getClientUserAgent());

    // Update application name for both telemetry and user agent
    updateUserAgentAndTelemetry(connectionContext, null);
  }

  /**
   * Determines the application name using a fallback mechanism: 1. useragententry url param 2.
   * applicationname url param 3. client info property "applicationname" 4. System property app.name
   *
   * @param connectionContext The connection context
   * @param clientInfoAppName The application name from client info properties, can be null
   * @return The determined application name or null if none is found
   */
  static String determineApplicationName(
      IDatabricksConnectionContext connectionContext, String clientInfoAppName) {
    // First check URL params
    String appName = connectionContext.getCustomerUserAgent();
    if (!isNullOrEmpty(appName)) {
      return appName;
    }

    // Then check application name URL param
    appName = connectionContext.getApplicationName();
    if (!isNullOrEmpty(appName)) {
      return appName;
    }

    // Then check client info property
    if (!isNullOrEmpty(clientInfoAppName)) {
      return clientInfoAppName;
    }

    // Finally check system property
    return System.getProperty(APP_NAME_SYSTEM_PROPERTY);
  }

  /**
   * Updates both the telemetry client app name and HTTP user agent headers. To be called during
   * connection initialization and when app name changes.
   *
   * @param connectionContext The connection context
   * @param clientInfoAppName Optional client info app name, can be null
   */
  public static void updateUserAgentAndTelemetry(
      IDatabricksConnectionContext connectionContext, String clientInfoAppName) {
    String appName = determineApplicationName(connectionContext, clientInfoAppName);
    if (!isNullOrEmpty(appName)) {
      // Update telemetry
      TelemetryHelper.updateClientAppName(appName);

      // Update HTTP user agent
      int i = appName.indexOf('/');
      String customerName = (i < 0) ? appName : appName.substring(0, i);
      String customerVersion = (i < 0) ? VERSION_FILLER : appName.substring(i + 1);
      UserAgent.withOtherInfo(customerName, UserAgent.sanitize(customerVersion));
    }
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
