package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.sdk.core.UserAgent;

public class UserAgentManager {
  private static final String SDK_USER_AGENT = "databricks-sdk-java";
  private static final String JDBC_HTTP_USER_AGENT = "databricks-jdbc-http";
  private static final String VERSION_FILLER_FOR_CUSTOMER_USER_AGENT = "version";
  private static final String DEFAULT_USER_AGENT = "DatabricksJDBCDriverOSS";
  private static final String CLIENT_USER_AGENT_PREFIX = "Java";
  public static final String USER_AGENT_SEA_CLIENT = "SQLExecHttpClient";
  public static final String USER_AGENT_THRIFT_CLIENT = "THttpClient";

  /**
   * Set the user agent for the Databricks JDBC driver.
   *
   * @param connectionContext The connection context.
   */
  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    UserAgent.withProduct(DEFAULT_USER_AGENT, DriverUtil.getVersion());
    UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, connectionContext.getClientUserAgent());
    String customerUA = connectionContext.getCustomerUserAgent();
    if (!isNullOrEmpty(customerUA)) {
      int i = customerUA.indexOf('/');
      String customerName = (i < 0) ? customerUA : customerUA.substring(0, i);
      String customerVersion =
          (i < 0) ? VERSION_FILLER_FOR_CUSTOMER_USER_AGENT : customerUA.substring(i + 1);
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
