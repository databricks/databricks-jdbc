package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.CLIENT_USER_AGENT_PREFIX;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.USER_AGENT_DELIMITER;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.UserAgent;

public class UserAgentManager {
  /**
   * Set the user agent for the Databricks JDBC driver.
   *
   * @param connectionContext The connection context.
   */
  public static void setUserAgent(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    UserAgent.withProduct(DatabricksJdbcConstants.DEFAULT_USER_AGENT, DriverUtil.getVersion());
    String customerUA = connectionContext.getCustomerUserAgent(),
        clientUA = connectionContext.getClientUserAgent();

    if (customerUA == null || customerUA.isEmpty()) {
      UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, clientUA);
    } else {
      String[] split = customerUA.split("/", -1);

      if (split.length == 1) { // No "/" found in customerUserAgent
        UserAgent.withOtherInfo(
            CLIENT_USER_AGENT_PREFIX, clientUA + USER_AGENT_DELIMITER + customerUA.trim());
      } else if (split.length == 2) { // Exactly one "/" found
        UserAgent.withOtherInfo(split[0].trim(), split[1].trim());
      } else { // More than one "/" found
        throw new DatabricksParsingException("UserAgent is allowed to have a maximum of one /");
      }
    }
    System.out.println("Constructed UserAgent: " + UserAgent.asString());
  }
}
