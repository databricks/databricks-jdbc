package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.core.UserAgent;

/** Helper class for determining and setting user agent and client app name. */
public class UserAgentHelper {
  private static final String APP_NAME_SYSTEM_PROPERTY = "app.name";
  private static final String VERSION_FILLER = "version";

  /**
   * Determines the application name using a fallback mechanism: 1. useragententry url param 2.
   * applicationname url param 3. client info property "applicationname" 4. System property app.name
   *
   * @param connectionContext The connection context
   * @param clientInfoAppName The application name from client info properties, can be null
   * @return The determined application name or null if none is found
   */
  public static String determineApplicationName(
      IDatabricksConnectionContext connectionContext, String clientInfoAppName) {
    // First check URL params
    String appName = connectionContext.getCustomerUserAgent();
    if (!isNullOrEmpty(appName)) {
      return appName;
    }

    // Then check applicationname URL param
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
}
