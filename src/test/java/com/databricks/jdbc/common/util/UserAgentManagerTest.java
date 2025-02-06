package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.util.UserAgentManager.getUserAgentString;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class UserAgentManagerTest {
  @Test
  void testUserAgentSetsClientCorrectly() throws DatabricksSQLException {
    // Thrift
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(CLUSTER_JDBC_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    System.out.println(getUserAgentString());
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/0.9.9-oss"));
    assertTrue(userAgent.contains(" Java/THttpClient-HC-MyApp"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));

    // SEA
    connectionContext =
        DatabricksConnectionContextFactory.create(WAREHOUSE_JDBC_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    userAgent = getUserAgentString();
    assertTrue(userAgent.contains("DatabricksJDBCDriverOSS/0.9.9-oss"));
    assertTrue(userAgent.contains(" Java/SQLExecHttpClient-HC-MyApp"));
    assertTrue(userAgent.contains(" databricks-jdbc-http "));
    assertFalse(userAgent.contains("databricks-sdk-java"));
  }

  @Test
  void testUserAgentSetsCustomerInput() throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(USER_AGENT_URL, new Properties());
    UserAgentManager.setUserAgent(connectionContext);
    String userAgent = getUserAgentString();
    assertTrue(userAgent.contains("TEST-24.2.0.2712019"));
  }
}
