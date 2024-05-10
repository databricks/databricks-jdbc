package com.databricks.jdbc.integration.fakeservice;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.FAKE_SERVICE_PORT_PROP_PREFIX;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;

import com.databricks.jdbc.driver.DatabricksJdbcConstants.FakeService;
import com.databricks.jdbc.integration.IntegrationTestUtil;
import com.github.tomakehurst.wiremock.common.SingleRootFileSource;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import com.github.tomakehurst.wiremock.standalone.JsonFileMappingsSource;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A JUnit 5 extension that facilitates the management of fake service behavior for testing
 * purposes. This extension integrates WireMockServer for stubbing HTTP responses and controlling
 * the behavior of HTTP requests during testing. It supports both recording real service responses
 * and replaying previously recorded responses based on a specified mode.
 *
 * <p>Sample Usage:
 *
 * <pre>{@code
 * public class IntegrationTests {
 *
 *   // FakeServiceExtension for SERVICE_A API.
 *   @RegisterExtension
 *   static FakeServiceExtension serviceAExtension =
 *       new FakeServiceExtension(
 *           new DBWireMockExtension.Builder()
 *               .options(wireMockConfig().dynamicPort().dynamicHttpsPort()),
 *           FakeService.SERVICE_A,
 *           "https://service-a.com");
 *
 *   // FakeServiceExtension for SERVICE_B API.
 *   @RegisterExtension
 *   static FakeServiceExtension serviceBExtension =
 *       new FakeServiceExtension(
 *           new DBWireMockExtension.Builder()
 *               .options(wireMockConfig().dynamicPort().dynamicHttpsPort()),
 *           FakeService.SERVICE_B,
 *           "https://service-b.com");
 *
 *   // Test methods interacting with SERVICE_A and SERVICE_B...
 * }
 * }</pre>
 */
public class FakeServiceExtension extends DBWireMockExtension {

  /**
   * Maximum size in bytes of text body size in stubbing beyond which it is extracted in a separate
   * file.
   */
  private static final long MAX_STUBBING_TEXT_SIZE = 102400;

  /**
   * Maximum size in bytes of binary body size in stubbing beyond which it is extracted in a
   * separate file.
   */
  private static final long MAX_STUBBING_BINARY_SIZE = 102400;

  /**
   * Environment variable holding the fake service mode.
   *
   * <ul>
   *   <li>{@link FakeServiceMode#RECORD}: Requests are sent to production service and responses are
   *       persisted.
   *   <li>{@link FakeServiceMode#REPLAY}: Saved responses are replayed instead of sending requests
   *       to production service.
   * </ul>
   */
  public static final String FAKE_SERVICE_TEST_MODE_ENV = "FAKE_SERVICE_TEST_MODE";

  /** Path to the stubbing directory for SQL Execution API. */
  public static final String SQL_EXEC_API_STUBBING_FILE_PATH = "src/test/resources/sqlexecapi";

  /** Path to the stubbing directory for Cloud Fetch API. */
  public static final String CLOUD_FETCH_API_STUBBING_FILE_PATH =
      "src/test/resources/cloudfetchapi";

  /** Fake service to manage. */
  private final FakeService fakeService;

  /** Base URL of the target production service. */
  private final String targetBaseUrl;

  /** Mode of the fake service. */
  private FakeServiceMode fakeServiceMode;

  public enum FakeServiceMode {
    RECORD,
    REPLAY
  }

  public FakeServiceExtension(Builder builder, FakeService fakeService, String targetBaseUrl) {
    super(builder);
    this.fakeService = fakeService;
    this.targetBaseUrl = targetBaseUrl;
  }

  public FakeService getFakeService() {
    return fakeService;
  }

  public String getTargetBaseUrl() {
    return targetBaseUrl;
  }

  public FakeServiceMode getFakeServiceMode() {
    return fakeServiceMode;
  }

  /** {@inheritDoc} */
  @Override
  protected void onBeforeAll(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws Exception {
    super.onBeforeAll(wireMockRuntimeInfo, context);

    String fakeServiceModeValue = System.getenv(FAKE_SERVICE_TEST_MODE_ENV);
    fakeServiceMode =
        fakeServiceModeValue != null
            ? FakeServiceMode.valueOf(fakeServiceModeValue.toUpperCase())
            : FakeServiceMode.REPLAY;

    System.setProperty(
        FAKE_SERVICE_PORT_PROP_PREFIX + fakeService.name().toLowerCase(),
        String.valueOf(wireMockRuntimeInfo.getHttpPort()));

    System.setProperty(IS_FAKE_SERVICE_TEST_PROP, "true");
  }

  /** {@inheritDoc} */
  @Override
  protected void onBeforeEach(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws Exception {
    super.onBeforeEach(wireMockRuntimeInfo, context);

    if (fakeServiceMode == FakeServiceMode.REPLAY) {
      loadStubMappings(wireMockRuntimeInfo, context);
    } else {
      startRecording(wireMockRuntimeInfo);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void onAfterEach(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws Exception {
    IntegrationTestUtil.resetJDBCConnection();

    if (fakeServiceMode == FakeServiceMode.RECORD) {
      saveStubMappings(wireMockRuntimeInfo, context);
    }

    super.onAfterEach(wireMockRuntimeInfo, context);
  }

  /** {@inheritDoc} */
  @Override
  protected void onAfterAll(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws Exception {
    System.clearProperty(FAKE_SERVICE_PORT_PROP_PREFIX + fakeService.name().toLowerCase());
    System.clearProperty(IS_FAKE_SERVICE_TEST_PROP);

    super.onAfterAll(wireMockRuntimeInfo, context);
  }

  /** Gets the stubbing directory for the current test class and method. */
  @NotNull
  private String getStubbingDir(ExtensionContext context) {
    String testClassName = context.getTestClass().orElseThrow().getSimpleName().toLowerCase();
    String testMethodName = context.getTestMethod().orElseThrow().getName().toLowerCase();

    return (fakeService == FakeService.SQL_EXEC
            ? SQL_EXEC_API_STUBBING_FILE_PATH
            : CLOUD_FETCH_API_STUBBING_FILE_PATH)
        + "/"
        + testClassName
        + "/"
        + testMethodName;
  }

  /** Loads stub mappings from the stubbing directory. */
  private void loadStubMappings(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context) {
    String stubbingDir = getStubbingDir(context);
    wireMockRuntimeInfo.getWireMock().loadMappingsFrom(stubbingDir);
  }

  /** Starts recording stub mappings. */
  private void startRecording(WireMockRuntimeInfo wireMockRuntimeInfo) {
    wireMockRuntimeInfo
        .getWireMock()
        .startStubRecording(
            new RecordSpecBuilder()
                .forTarget(targetBaseUrl)
                .makeStubsPersistent(false) // manually save stub mappings
                .extractTextBodiesOver(MAX_STUBBING_TEXT_SIZE)
                .extractBinaryBodiesOver(MAX_STUBBING_BINARY_SIZE)
                .transformers(StubMappingCredentialsCleaner.NAME));
  }

  /**
   * Saves recorded stub mappings to the stubbing directory. Before saving, it clears the existing
   * stubbings.
   */
  private void saveStubMappings(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws IOException {
    List<StubMapping> stubMappingList =
        wireMockRuntimeInfo.getWireMock().stopStubRecording().getStubMappings();
    String stubbingDir = getStubbingDir(context) + "/mappings";
    deleteFilesInDir(stubbingDir);
    new JsonFileMappingsSource(new SingleRootFileSource(stubbingDir), null).save(stubMappingList);
  }

  /** Deletes files in the given directory. */
  private static void deleteFilesInDir(String dirPath) throws IOException {
    Path dir = Paths.get(dirPath);
    if (!Files.exists(dir)) {
      throw new IOException("Directory does not exist: " + dir.toAbsolutePath());
    }

    try (Stream<Path> paths = Files.walk(dir)) {
      paths
          .filter(Files::isRegularFile)
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  throw new RuntimeException("Failed to delete file: " + path.toAbsolutePath(), e);
                }
              });
    }
  }
}
