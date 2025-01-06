package com.databricks.jdbc.integration.fakeservice;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class FakeServiceConfigLoader {

  public static final String DATABRICKS_HOST_PROP = "host.databricks";

  public static final String CLOUD_FETCH_HOST_PROP = "host.cloudfetch";

  public static final String TEST_CATALOG = "testcatalog";

  public static final String TEST_SCHEMA = "testschema";

  public static final boolean shouldUseThriftClient =
      Boolean.parseBoolean(System.getenv("USE_THRIFT_CLIENT"));

  public static final boolean isM2MTest = Boolean.parseBoolean(System.getenv("IS_M2M_TEST"));

  private static final String SQL_EXEC_FAKE_SERVICE_TEST_PROPS =
      "sqlexecfakeservicetest.properties";

  private static final String SQL_GATEWAY_FAKE_SERVICE_TEST_PROPS =
      "sqlgatewayfakeservicetest.properties";

  private static final String SQL_EXEC_M2M_FAKE_SERVICE_TEST_PROPS =
      "sqlexecm2mfakeservicetest.properties";

  private static final Properties properties = new Properties();

  static {
    final String propsFileName =
        shouldUseThriftClient
            ? SQL_GATEWAY_FAKE_SERVICE_TEST_PROPS
            : isM2MTest ? SQL_EXEC_M2M_FAKE_SERVICE_TEST_PROPS : SQL_EXEC_FAKE_SERVICE_TEST_PROPS;

    try (InputStream input =
        FakeServiceConfigLoader.class.getClassLoader().getResourceAsStream(propsFileName)) {
      properties.load(input);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load properties file: " + propsFileName, e);
    }
  }

  public static String getProperty(String key) {
    return properties.getProperty(key);
  }

  public static Properties getProperties() {
    return properties;
  }
}
