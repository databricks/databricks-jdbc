package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.volume.DatabricksUCVolumeClient;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.DatabricksWireMockExtension;
import com.databricks.jdbc.integration.fakeservice.FakeServiceConfigLoader;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class UCVolumeDataExchangeIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @RegisterExtension
  private static final FakeServiceExtension cloudFetchUCVolumeExtension =
      new FakeServiceExtension(
          new DatabricksWireMockExtension.Builder()
              .options(
                  wireMockConfig().dynamicPort().dynamicHttpsPort().extensions(getExtensions())),
          DatabricksJdbcConstants.FakeServiceType.CLOUD_FETCH_UC_VOLUME,
          "https://us-west-2-extstaging-managed-catalog-test-bucket-1.s3-fips.us-west-2.amazonaws.com");

  private DatabricksUCVolumeClient client;
  private Connection con;
  private static final String jdbcUrlTemplate =
      "jdbc:databricks://%s/default;transportMode=http;ssl=0;AuthMech=3;httpPath=%s";
  private static final String HTTP_PATH = "/sql/1.0/warehouses/791ba2a31c7fd70a";
  private static final String LOCAL_TEST_DIRECTORY = "/tmp";

  @BeforeAll
  static void beforeAll() {
    setDatabricksApiTargetUrl("https://e2-dogfood.staging.cloud.databricks.com");
    setCloudFetchApiTargetUrl("https://e2-dogfood-core.s3.us-west-2.amazonaws.com");
  }

  @BeforeEach
  void setUp() throws SQLException {
    con = getConnection();
    client = new DatabricksUCVolumeClient(con);
    con.setClientInfo("allowlistedVolumeOperationLocalFilePaths", LOCAL_TEST_DIRECTORY);
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (con != null) {
      con.close();
    }
  }

  @ParameterizedTest
  @MethodSource("provideParametersForGetObject")
  void testGetObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean expected)
      throws Exception {
    File file = new File(localPath);
    if (file.exists()) {
      file.delete();
    }
    assertEquals(expected, client.getObject(catalog, schema, volume, objectPath, localPath));
  }

  private static Stream<Arguments> provideParametersForGetObject() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "abc_file1.csv",
            "/tmp/download1.csv",
            true),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/efg_file1.csv",
            "/tmp/download2.csv",
            true));
  }

  @Test
  public void testGetObject_FileRead() throws Exception {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(con);

    String volume = "test_volume1";
    String objectPath = "hello_world.txt";
    String localPath = "/tmp/download_hello_world.txt";
    String expectedContent = "helloworld";

    File file = new File(localPath);
    if (file.exists()) {
      file.delete();
    }

    assertTrue(
        client.getObject(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, volume, objectPath, localPath));
    byte[] LocalFileContent = Files.readAllBytes(Paths.get(localPath));
    String actualContent = new String(LocalFileContent, StandardCharsets.UTF_8);

    assertEquals(expectedContent, actualContent);
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutObject")
  void testPutObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      String localContent,
      boolean toOverwrite,
      boolean expected)
      throws Exception {

    if (client.objectExists(catalog, schema, volume, objectPath, false)) {
      assertTrue(client.deleteObject(catalog, schema, volume, objectPath));
    }
    Files.write(Paths.get(localPath), localContent.getBytes(StandardCharsets.UTF_8));

    assertEquals(
        expected, client.putObject(catalog, schema, volume, objectPath, localPath, toOverwrite));
  }

  private static Stream<Arguments> provideParametersForPutObject() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "upload1.txt",
            "/tmp/download1.txt",
            "helloworld",
            false,
            true),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/upload2.txt",
            "/tmp/download1.txt",
            "helloworld",
            false,
            true));
  }

  @Test
  public void testPutAndGet() throws Exception {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(con);

    String catalog = UC_VOLUME_CATALOG;
    String schema = UC_VOLUME_SCHEMA;
    String volume = "test_volume1";
    String objectPath = "hello_world.txt";
    boolean toOverwrite = false;
    String localPathForUpload = "/tmp/upload_hello_world.txt";
    String localPathForDownload = "/tmp/download_hello_world.txt";
    String expectedContent = "helloworld";

    Files.write(Paths.get(localPathForUpload), expectedContent.getBytes(StandardCharsets.UTF_8));

    if (client.objectExists(catalog, schema, volume, objectPath, false)) {
      assertTrue(client.deleteObject(catalog, schema, volume, objectPath));
    }
    assertTrue(
        client.putObject(catalog, schema, volume, objectPath, localPathForUpload, toOverwrite));

    File file = new File(localPathForDownload);
    if (file.exists()) {
      file.delete();
    }
    assertTrue(client.getObject(catalog, schema, volume, objectPath, localPathForDownload));

    byte[] fileContent = Files.readAllBytes(Paths.get(localPathForDownload));
    String actualContent = new String(fileContent, StandardCharsets.UTF_8);
    assertEquals(expectedContent, actualContent);
  }

  @Test
  public void testPutAndDelete() throws Exception {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(con);

    String catalog = UC_VOLUME_CATALOG;
    String schema = UC_VOLUME_SCHEMA;
    String volume = "test_volume1";
    String objectPath = "test_hello_world.txt";
    String localPathForUpload = "/tmp/upload_hello_world.txt";
    String fileContent = "helloworld";

    Files.write(Paths.get(localPathForUpload), fileContent.getBytes(StandardCharsets.UTF_8));
    assertTrue(client.putObject(catalog, schema, volume, objectPath, localPathForUpload, false));
    assertTrue(client.objectExists(catalog, schema, volume, objectPath, false));
    assertTrue(client.deleteObject(catalog, schema, volume, objectPath));
    assertFalse(client.objectExists(catalog, schema, volume, objectPath, false));
  }

  @Test
  public void testPutAndGetOverwrite() throws Exception {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(con);

    String catalog = UC_VOLUME_CATALOG;
    String schema = UC_VOLUME_SCHEMA;
    String volume = "test_volume1";
    String objectPath = "overwrite.txt";
    String initialContent = "initialContent";
    String overwriteContent = "overwriteContent";

    String localPathForInitialUpload = "/tmp/upload_overwrite_test_1.txt";
    String localPathForInitialDownload = "/tmp/download_overwrite_test_1.txt";

    File file = new File(localPathForInitialDownload);
    if (file.exists()) {
      file.delete();
    }
    if (client.objectExists(catalog, schema, volume, objectPath, false)) {
      assertTrue(client.deleteObject(catalog, schema, volume, objectPath));
    }

    Files.write(
        Paths.get(localPathForInitialUpload), initialContent.getBytes(StandardCharsets.UTF_8));
    assertTrue(
        client.putObject(catalog, schema, volume, objectPath, localPathForInitialUpload, false));
    assertTrue(client.getObject(catalog, schema, volume, objectPath, localPathForInitialDownload));
    byte[] fileContent = Files.readAllBytes(Paths.get(localPathForInitialDownload));
    String actualContent = new String(fileContent, StandardCharsets.UTF_8);
    assertEquals(initialContent, actualContent);

    String localPathForOverwriteUpload = "/tmp/upload_overwrite_test_2.txt";
    String localPathForOverwriteDownload = "/tmp/download_overwrite_test_2.txt";
    file = new File(localPathForOverwriteDownload);
    if (file.exists()) {
      file.delete();
    }

    Files.write(
        Paths.get(localPathForOverwriteUpload), overwriteContent.getBytes(StandardCharsets.UTF_8));
    assertTrue(
        client.putObject(catalog, schema, volume, objectPath, localPathForOverwriteUpload, true));
    assertTrue(
        client.getObject(catalog, schema, volume, objectPath, localPathForOverwriteDownload));
    fileContent = Files.readAllBytes(Paths.get(localPathForOverwriteDownload));
    actualContent = new String(fileContent, StandardCharsets.UTF_8);
    assertEquals(overwriteContent, actualContent);
  }

  private Connection getConnection() throws SQLException {
    String jdbcUrl = String.format(jdbcUrlTemplate, getFakeServiceHost(), HTTP_PATH);

    Properties connProps = new Properties();
    connProps.put(DatabricksJdbcUrlParams.USER, getDatabricksUser());
    connProps.put(DatabricksJdbcUrlParams.PASSWORD, getDatabricksToken());
    connProps.put(CATALOG, FakeServiceConfigLoader.getProperty(CATALOG));
    connProps.put(
        DatabricksJdbcUrlParams.CONN_SCHEMA,
        FakeServiceConfigLoader.getProperty(DatabricksJdbcUrlParams.CONN_SCHEMA));

    return DriverManager.getConnection(jdbcUrl, connProps);
  }
}
