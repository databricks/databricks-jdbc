package com.databricks.jdbc.integration.fakeservice;

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
public abstract class BaseFakeServiceIntegrationTests {

  /**
   * {@link FakeServiceExtension} for {@link DatabricksJdbcConstants.FakeServiceType#SQL_EXEC}.
   * Intercepts all requests to SQL Execution API.
   */
  @RegisterExtension
  private static final FakeServiceExtension sqlExecApiExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          DatabricksJdbcConstants.FakeServiceType.SQL_EXEC,
          "https://" + System.getenv("DATABRICKS_HOST"));

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
          DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH,
          "https://dbstoragepzjc6kojqibtg.blob.core.windows.net");

  /** Resets the possible mutations for the extensions after all tests have run. */
  @AfterAll
  protected static void resetPossibleMutations() {
    sqlExecApiExtension.setTargetBaseUrl("https://" + System.getenv("DATABRICKS_HOST"));
    cloudFetchApiExtension.setTargetBaseUrl("https://dbstoragepzjc6kojqibtg.blob.core.windows.net");
  }

  protected FakeServiceExtension getSqlExecApiExtension() {
    return sqlExecApiExtension;
  }

  protected FakeServiceExtension getCloudFetchApiExtension() {
    return cloudFetchApiExtension;
  }

  /** Returns the extensions to be used for stubbing. */
  private static Extension[] getExtensions() {
    return new Extension[] {new StubMappingCredentialsCleaner()};
  }
}
