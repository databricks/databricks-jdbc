package com.databricks.jdbc.integration.fakeservice;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getJWTTokenEndpointHost;
import static com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader.CLOUD_FETCH_HOST_PROP;
import static com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader.DATABRICKS_HOST_PROP;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.github.tomakehurst.wiremock.extension.Extension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Base class for integration tests that use {@link FakeServiceExtension} for simulating {@link
 * DatabricksJdbcConstants.FakeServiceType#SQL_EXEC} and {@link
 * DatabricksJdbcConstants.FakeServiceType#CLOUD_FETCH}.
 */
public abstract class AbstractFakeServiceIntegrationTests {

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#SQL_EXEC}.
   * Intercepts all requests to SQL Execution API and uses a custom user agent.
   */
  @RegisterExtension
  private static final FakeServiceExtension databricksApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig()
                      .dynamicPort()
                      .dynamicHttpsPort()
                      .extensions(getExtensions())
                      .httpClientFactory(
                          new FakeServiceHttpClientFactory(
                              FakeServiceConfigLoader.getFakeServiceUserAgent()))),
          FakeServiceConfigLoader.getFakeServiceType(),
          FakeServiceConfigLoader.getProperty(DATABRICKS_HOST_PROP));

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#CLOUD_FETCH}.
   * Intercepts all requests to Cloud Fetch API and uses a custom user agent.
   */
  @RegisterExtension
  private static final FakeServiceExtension cloudFetchApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig()
                      .dynamicPort()
                      .dynamicHttpsPort()
                      .extensions(getExtensions())
                      .httpClientFactory(
                          new FakeServiceHttpClientFactory(
                              FakeServiceConfigLoader.getFakeServiceUserAgent()))),
          FakeServiceConfigLoader.getCloudFetchFakeServiceType(),
          FakeServiceConfigLoader.getProperty(CLOUD_FETCH_HOST_PROP));

  /**
   * {@link FakeServiceExtension} for {@link
   * DatabricksJdbcConstants.FakeServiceType#JWT_TOKEN_ENDPOINT}. Iy uses a custom user agent for
   * all its interactions with the endpoint in the record mode.
   */
  @RegisterExtension
  private static final FakeServiceExtension tokenEndpointApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig()
                      .dynamicPort()
                      .dynamicHttpsPort()
                      .extensions(getExtensions())
                      .httpClientFactory(
                          new FakeServiceHttpClientFactory(
                              FakeServiceConfigLoader.getFakeServiceUserAgent()))),
          DatabricksJdbcConstants.FakeServiceType.JWT_TOKEN_ENDPOINT,
          getJWTTokenEndpointHost());

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
   * Returns true if the test uses {@link
   * com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient}.
   */
  protected boolean isSqlExecSdkClient() {
    return FakeServiceConfigLoader.getFakeServiceType()
        .equals(DatabricksJdbcConstants.FakeServiceType.SQL_EXEC);
  }

  /** Returns the extensions to be used for stubbing. */
  protected static Extension[] getExtensions() {
    return new Extension[] {new StubMappingRedactor()};
  }

  protected static FakeServiceExtension.FakeServiceMode getFakeServiceMode() {
    return databricksApiExtension.getFakeServiceMode();
  }
}
