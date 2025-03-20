package com.databricks.jdbc.integration.fakeservice;

import com.databricks.jdbc.common.DatabricksJdbcConstants;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FakeServiceConfigLoader {

  public static final String DATABRICKS_HOST_PROP = "host.databricks";

  public static final String CLOUD_FETCH_HOST_PROP = "host.cloudfetch";

  public static final String TEST_CATALOG = "testcatalog";

  public static final String TEST_SCHEMA = "testschema";

  private static final DatabricksJdbcConstants.FakeServiceType fakeServiceType =
      System.getenv("FAKE_SERVICE_TYPE") != null
          ? DatabricksJdbcConstants.FakeServiceType.valueOf(
              System.getenv("FAKE_SERVICE_TYPE").toUpperCase())
          : DatabricksJdbcConstants.FakeServiceType.SQL_EXEC;

  private static final String SQL_EXEC_FAKE_SERVICE_TEST_PROPS =
      "sqlexecfakeservicetest.properties";

  private static final String SQL_GATEWAY_FAKE_SERVICE_TEST_PROPS =
      "sqlgatewayfakeservicetest.properties";

  private static final String FAKE_SERVICE_USER_AGENT = "DatabricksJdbcDriverOss-FakeService";

  private static final String THRIFT_SERVER_FAKE_SERVICE_TEST_PROPS =
      "thriftserverfakeservicetest.properties";

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

  public static DatabricksJdbcConstants.FakeServiceType getCloudFetchFakeServiceType() {
    switch (fakeServiceType) {
      case THRIFT_SERVER:
        return DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH_THRIFT_SERVER;
      case SQL_GATEWAY:
        return DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH_SQL_GATEWAY;
      default:
        return DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH;
    }
  }

  public static DatabricksJdbcConstants.FakeServiceType getFakeServiceType() {
    return fakeServiceType;
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

  public static boolean shouldUseThriftClient() {
    return !fakeServiceType.equals(DatabricksJdbcConstants.FakeServiceType.SQL_EXEC);
  }

  private static String getPropsFileName() {
    switch (fakeServiceType) {
      case THRIFT_SERVER:
        return THRIFT_SERVER_FAKE_SERVICE_TEST_PROPS;
      case SQL_GATEWAY:
        return SQL_GATEWAY_FAKE_SERVICE_TEST_PROPS;
      default:
        return SQL_EXEC_FAKE_SERVICE_TEST_PROPS;
    }
  }
}
