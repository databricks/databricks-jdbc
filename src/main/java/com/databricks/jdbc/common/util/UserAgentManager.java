package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.CLIENT_USER_AGENT_PREFIX;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.USER_AGENT_DELIMITER;
import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.sdk.core.UserAgent;

public class UserAgentManager {
  private static final String SDK_USER_AGENT = "databricks-sdk-java";
  private static final String JDBC_HTTP_USER_AGENT = "databricks-jdbc-http";

  /**
   * Set the user agent for the Databricks JDBC driver.
   *
   * @param connectionContext The connection context.
   */
  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    UserAgent.withProduct(DatabricksJdbcConstants.DEFAULT_USER_AGENT, DriverUtil.getVersion());
    String customerUA = connectionContext.getCustomerUserAgent();
    String userAgentInfo = connectionContext.getClientUserAgent();
    if (!isNullOrEmpty(customerUA)) {
      userAgentInfo += USER_AGENT_DELIMITER + customerUA.trim();
    }
    String sanitisedUserAgentInfo = UserAgent.sanitize(userAgentInfo);
    UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, sanitisedUserAgentInfo);
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
