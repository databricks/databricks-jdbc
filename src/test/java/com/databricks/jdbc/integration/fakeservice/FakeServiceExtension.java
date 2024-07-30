package com.databricks.jdbc.integration.fakeservice;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.FAKE_SERVICE_URI_PROP_SUFFIX;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.IS_FAKE_SERVICE_TEST_PROP;
import static com.github.tomakehurst.wiremock.common.AbstractFileSource.byFileExtension;

import com.databricks.jdbc.driver.DatabricksJdbcConstants.FakeServiceType;
import com.databricks.jdbc.integration.IntegrationTestUtil;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.common.BinaryFile;
import com.github.tomakehurst.wiremock.common.ContentTypes;
import com.github.tomakehurst.wiremock.common.SingleRootFileSource;
import com.github.tomakehurst.wiremock.common.TextFile;
import com.github.tomakehurst.wiremock.http.ContentTypeHeader;
import com.github.tomakehurst.wiremock.http.HttpHeaders;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import com.github.tomakehurst.wiremock.standalone.JsonFileMappingsSource;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import com.github.tomakehurst.wiremock.stubbing.StubMappingCollection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
public class FakeServiceExtension extends DatabricksWireMockExtension {

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

  /** Root directory for extracted body files. */
  private static final String EXTRACTED_BODY_FILE_ROOT = "src/test/resources/__files";

  /**
   * Environment variable holding the fake service mode.
   *
   * <ul>
   *   <li>{@link FakeServiceMode#RECORD}: Requests are sent to production service and responses are
   *       persisted.
   *   <li>{@link FakeServiceMode#REPLAY}: Saved responses are replayed instead of sending requests
   *       to production service.
   *   <li>{@link FakeServiceMode#DRY}: Requests are sent to production service but not persisted.
   * </ul>
   */
  public static final String FAKE_SERVICE_TEST_MODE_ENV = "FAKE_SERVICE_TEST_MODE";

  public static final String TARGET_URI_PROP_SUFFIX = ".targetURI";

  /** Path to the stubbing directory for SQL Execution API. */
  public static final String SQL_EXEC_API_STUBBING_FILE_PATH = "src/test/resources/sqlexecapi";

  /** Path to the stubbing directory for Cloud Fetch API. */
  public static final String CLOUD_FETCH_API_STUBBING_FILE_PATH =
      "src/test/resources/cloudfetchapi";

  /** Path to the stubbing directory for SQL Gateway API. */
  public static final String SQL_GATEWAY_API_STUBBING_FILE_PATH =
      "src/test/resources/sqlgatewayapi";

  /** Path to the stubbing directory for Cloud Fetch when using SQL Gateway API. */
  public static final String CLOUD_FETCH_SQL_GATEWAY_API_STUBBING_FILE_PATH =
      "src/test/resources/cloudfetchsqlgatewayapi";

  /** Fake service to manage. */
  private final FakeServiceType fakeServiceType;

  /** HTTP port of the WireMock server. */
  private int wireMockServerHttpPort;

  /** Base URL of the target production service. */
  private String targetBaseUrl;

  /** Mode of the fake service. */
  private FakeServiceMode fakeServiceMode;

  /** Index of parameterised test invocation. */
  private final String TEST_INVOCATION_INDEX_KEY = "invocation";

  public enum FakeServiceMode {
    RECORD,
    REPLAY,
    DRY
  }

  public FakeServiceExtension(
      Builder builder, FakeServiceType fakeServiceType, String targetBaseUrl) {
    super(builder);
    this.fakeServiceType = fakeServiceType;
    this.targetBaseUrl = targetBaseUrl;
  }

  public FakeServiceType getFakeServiceType() {
    return fakeServiceType;
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
    wireMockServerHttpPort = wireMockRuntimeInfo.getHttpPort();

    setFakeServiceProperties(wireMockServerHttpPort);
    IntegrationTestUtil.resetJDBCConnection();
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
    clearFakeServiceProperties();

    super.onAfterAll(wireMockRuntimeInfo, context);
  }

  /** Sets the base URL of the target production service to be tracked. */
  protected void setTargetBaseUrl(String targetBaseUrl) {
    this.targetBaseUrl = targetBaseUrl;

    // Refresh the fake service properties
    setFakeServiceProperties(wireMockServerHttpPort);
  }

  /** Gets the stubbing directory for the current test class and method. */
  private String getStubbingDir(ExtensionContext context) {
    String testClassName = context.getTestClass().orElseThrow().getSimpleName().toLowerCase();
    String testMethodName = context.getTestMethod().orElseThrow().getName().toLowerCase();

    String basePath;
    switch (fakeServiceType) {
      case SQL_EXEC:
        basePath = SQL_EXEC_API_STUBBING_FILE_PATH;
        break;
      case CLOUD_FETCH:
        basePath = CLOUD_FETCH_API_STUBBING_FILE_PATH;
        break;
      case SQL_GATEWAY:
        basePath = SQL_GATEWAY_API_STUBBING_FILE_PATH;
        break;
      case CLOUD_FETCH_SQL_GATEWAY:
        basePath = CLOUD_FETCH_SQL_GATEWAY_API_STUBBING_FILE_PATH;
        break;
      case CLOUD_FETCH_UC_VOLUME:
        basePath = "src/test/resources/cloudfetchucvolume";
        break;
      default:
        throw new IllegalStateException("Unsupported fake service type: " + fakeServiceType);
    }

    String uniqueID = context.getUniqueId();
    int invocationIndex = uniqueID.indexOf(TEST_INVOCATION_INDEX_KEY);
    if (invocationIndex != -1) {
      uniqueID =
          uniqueID.substring(
              uniqueID.lastIndexOf(TEST_INVOCATION_INDEX_KEY), uniqueID.length() - 1);
      return String.format("%s/%s/%s.%s", basePath, testClassName, testMethodName, uniqueID);
    } else {
      return String.format("%s/%s/%s", basePath, testClassName, testMethodName);
    }
  }

  /** Loads stub mappings from the stubbing directory. */
  private void loadStubMappings(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws IOException {
    final String stubbingDir = getStubbingDir(context);
    final SingleRootFileSource fileSource = new SingleRootFileSource(stubbingDir + "/mappings");

    if (!fileSource.exists()) {
      // No stub mappings to load
      return;
    }

    final List<TextFile> mappingFiles =
        fileSource.listFilesRecursively().stream()
            .filter(byFileExtension("json"))
            .collect(Collectors.toList());

    for (TextFile mappingFile : mappingFiles) {
      final StubMappingCollection stubCollection =
          JsonUtils.read(mappingFile.readContents(), StubMappingCollection.class);
      for (StubMapping mapping : stubCollection.getMappingOrMappings()) {
        embedExtractedBodyFile(mapping);
        wireMockRuntimeInfo.getWireMock().register(mapping);
      }
    }
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

  private void setFakeServiceProperties(int wireMockServerHttpPort) {
    System.setProperty(IS_FAKE_SERVICE_TEST_PROP, "true");
    System.setProperty(
        fakeServiceType.name().toLowerCase() + TARGET_URI_PROP_SUFFIX, targetBaseUrl);
    System.setProperty(
        targetBaseUrl + FAKE_SERVICE_URI_PROP_SUFFIX, "http://localhost:" + wireMockServerHttpPort);
  }

  private void clearFakeServiceProperties() {
    System.clearProperty(IS_FAKE_SERVICE_TEST_PROP);
    System.clearProperty(fakeServiceType.name().toLowerCase() + TARGET_URI_PROP_SUFFIX);
    System.clearProperty(targetBaseUrl + FAKE_SERVICE_URI_PROP_SUFFIX);
  }

  /** Embeds the extracted body file content into the stub mapping. */
  private static void embedExtractedBodyFile(final StubMapping mapping) {
    final SingleRootFileSource fileSource = new SingleRootFileSource(EXTRACTED_BODY_FILE_ROOT);
    final String bodyFileName = mapping.getResponse().getBodyFileName();

    if (bodyFileName != null) {
      final ResponseDefinitionBuilder responseDefinitionBuilder =
          ResponseDefinitionBuilder.like(mapping.getResponse()).withBodyFile(null);
      if (ContentTypes.determineIsTextFromMimeType(getMimeType(mapping))) {
        final TextFile bodyFile = fileSource.getTextFileNamed(bodyFileName);
        responseDefinitionBuilder.withBody(bodyFile.readContentsAsString());
      } else {
        BinaryFile bodyFile = fileSource.getBinaryFileNamed(bodyFileName);
        responseDefinitionBuilder.withBody(bodyFile.readContents());
      }

      mapping.setResponse(responseDefinitionBuilder.build());
    }
  }

  /** Gets the MIME type of the response body. */
  private static String getMimeType(StubMapping mapping) {
    return Optional.ofNullable(mapping.getResponse().getHeaders())
        .map(HttpHeaders::getContentTypeHeader)
        .map(ContentTypeHeader::mimeTypePart)
        .orElse(null);
  }

  /** Deletes files in the given directory. */
  private static void deleteFilesInDir(String dirPath) throws IOException {
    Path dir = Paths.get(dirPath);
    if (!Files.exists(dir)) {
      Files.createDirectories(dir);
      return;
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
