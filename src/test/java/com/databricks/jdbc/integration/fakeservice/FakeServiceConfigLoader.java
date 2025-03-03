package com.databricks.jdbc.integration.fakeservice;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FakeServiceConfigLoader {

  public static final String DATABRICKS_HOST_PROP = "host.databricks";

  public static final String CLOUD_FETCH_HOST_PROP = "host.cloudfetch";

  public static final String TEST_CATALOG = "testcatalog";

  public static final String TEST_SCHEMA = "testschema";

  public static final ConnectionInfoType connectionInfo =
      System.getenv("CONNECTION_INFO") != null
          ? ConnectionInfoType.valueOf(System.getenv("CONNECTION_INFO").toUpperCase())
          : ConnectionInfoType.SQL_EXEC;

  private static final String SQL_EXEC_FAKE_SERVICE_TEST_PROPS =
      "sqlexecfakeservicetest.properties";

  private static final String SQL_GATEWAY_FAKE_SERVICE_TEST_PROPS =
      "sqlgatewayfakeservicetest.properties";

  private static final String FAKE_SERVICE_USER_AGENT = "DatabricksJdbcDriverOss-FakeService";

  private static final String THRIFT_SERVER_FAKE_SERVICE_TEST_PROPS =
      "thriftserverfakeservicetest.properties";

  public enum ConnectionInfoType {
    THRIFT_SERVER,
    SQL_GATEWAY,
    SQL_EXEC
  }

  private static final Properties properties = new Properties();

  static {
    final String propsFileName = getPropsFileName();

    try (InputStream input =
        FakeServiceConfigLoader.class.getClassLoader().getResourceAsStream(propsFileName)) {
      properties.load(input);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load properties file: " + propsFileName, e);
    }
  }

  private static String getPropsFileName() {
    switch (connectionInfo) {
      case THRIFT_SERVER:
        return THRIFT_SERVER_FAKE_SERVICE_TEST_PROPS;
      case SQL_GATEWAY:
        return SQL_GATEWAY_FAKE_SERVICE_TEST_PROPS;
      default:
        return SQL_EXEC_FAKE_SERVICE_TEST_PROPS;
    }
  }

  public static String getProperty(String key) {
    return properties.getProperty(key);
  }

  public static Properties getProperties() {
    return properties;
  }

  public static String getFakeServiceUserAgent() {
    return FAKE_SERVICE_USER_AGENT;
  }
}
