package com.databricks.jdbc.integration.fakeservice;

import static com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader.CLOUD_FETCH_HOST_PROP;
import static com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader.DATABRICKS_HOST_PROP;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.github.tomakehurst.wiremock.extension.Extension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Base class for integration tests that use {@link FakeServiceExtension} for simulating {@link
 * com.databricks.jdbc.driver.DatabricksJdbcConstants.FakeServiceType#SQL_EXEC} and {@link
 * com.databricks.jdbc.driver.DatabricksJdbcConstants.FakeServiceType#CLOUD_FETCH}.
 */
public abstract class AbstractFakeServiceIntegrationTests {

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#SQL_EXEC}.
   * Intercepts all requests to SQL Execution API.
   */
  @RegisterExtension
  private static final FakeServiceExtension databricksApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          FakeServiceConfigLoader.shouldUseThriftClient
              ? DatabricksJdbcConstants.FakeServiceType.SQL_GATEWAY
              : DatabricksJdbcConstants.FakeServiceType.SQL_EXEC,
          FakeServiceConfigLoader.getProperty(DATABRICKS_HOST_PROP));

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#CLOUD_FETCH}.
   * Intercepts all requests to Cloud Fetch API.
   */
  @RegisterExtension
  private static final FakeServiceExtension cloudFetchApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          FakeServiceConfigLoader.shouldUseThriftClient
              ? DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH_SQL_GATEWAY
              : DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH,
          FakeServiceConfigLoader.getProperty(CLOUD_FETCH_HOST_PROP));

  /**
   * Resets the potential mutations (e.g., URLs set by {@link #setDatabricksApiTargetUrl}, {@link
   * #setCloudFetchApiTargetUrl}) to meaningful defaults, after all tests have completed.
   */
  @AfterAll
  static void resetPossibleMutations() {
    databricksApiExtension.setTargetBaseUrl(
        FakeServiceConfigLoader.getProperty(DATABRICKS_HOST_PROP));
    cloudFetchApiExtension.setTargetBaseUrl(
        FakeServiceConfigLoader.getProperty(CLOUD_FETCH_HOST_PROP));
  }

  protected static void setDatabricksApiTargetUrl(final String sqlExecApiTargetUrl) {
    databricksApiExtension.setTargetBaseUrl(sqlExecApiTargetUrl);
  }

  protected static void setCloudFetchApiTargetUrl(final String cloudFetchApiTargetUrl) {
    cloudFetchApiExtension.setTargetBaseUrl(cloudFetchApiTargetUrl);
  }

  protected FakeServiceExtension getDatabricksApiExtension() {
    return databricksApiExtension;
  }

  protected FakeServiceExtension getCloudFetchApiExtension() {
    return cloudFetchApiExtension;
  }

  /**
   * Returns true if the test uses {@link com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient}.
   */
  protected boolean isSqlExecSdkClient() {
    return !FakeServiceConfigLoader.shouldUseThriftClient;
  }

  /** Returns the extensions to be used for stubbing. */
  protected static Extension[] getExtensions() {
    return new Extension[] {new StubMappingCredentialsCleaner()};
  }

  protected static FakeServiceExtension.FakeServiceMode getFakeServiceMode() {
    return databricksApiExtension.getFakeServiceMode();
  }
}
