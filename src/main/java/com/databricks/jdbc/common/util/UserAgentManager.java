package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.CLIENT_USER_AGENT_PREFIX;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.sdk.core.UserAgent;

public class UserAgentManager {
  /**
   * Set the user agent for the Databricks JDBC driver.
   *
   * @param connectionContext The connection context.
   */
  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    UserAgent.withProduct(DatabricksJdbcConstants.DEFAULT_USER_AGENT, DriverUtil.getVersion());
    UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, connectionContext.getClientUserAgent());
  }
}
