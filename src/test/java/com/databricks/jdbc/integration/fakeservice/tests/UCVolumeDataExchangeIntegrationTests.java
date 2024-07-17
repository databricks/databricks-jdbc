package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.HTTP_PATH;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.client.impl.sdk.DatabricksUCVolumeClient;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import com.databricks.jdbc.integration.fakeservice.DatabricksWireMockExtension;
import com.databricks.jdbc.integration.fakeservice.FakeServiceExtension;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
      "jdbc:databricks://%s/default;transportMode=http;ssl=0;AuthMech=3;httpPath=%s;catalog=SPARK";

  private static final String HTTP_PATH = "/sql/1.0/warehouses/791ba2a31c7fd70a";
  private static final String LOCAL_TEST_DIRECTORY = "/tmp";

  @BeforeAll
  static void beforeAll() {
    setDatabricksApiTargetUrl("https://e2-dogfood.staging.cloud.databricks.com");
    setCloudFetchApiTargetUrl("https://e2-dogfood-core.s3.us-west-2.amazonaws.com");
  }

  @BeforeEach
  void setUp() throws SQLException {
    // TODO: Testing is done here using the E2-Dogfood environment. Need to update this to use a
    // test warehouse.
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

  @ParameterizedTest
  @MethodSource("provideParametersForGetObject_FileRead")
  void testGetObject_FileRead(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      String expectedContentPath,
      String expectedContent)
      throws Exception {
    byte[] fileContent = Files.readAllBytes(Paths.get(expectedContentPath));
    String actualContent = new String(fileContent, StandardCharsets.UTF_8);
    File file = new File(localPath);
    if (file.exists()) {
      file.delete();
    }

    assertTrue(client.getObject(catalog, schema, volume, objectPath, localPath));
    assertEquals(expectedContent, actualContent);
  }

  private static Stream<Arguments> provideParametersForGetObject_FileRead() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "hello_world.txt",
            "/tmp/download_hello_world.txt",
            "/tmp/expected_hello_world.txt",
            "helloworld"));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutObject")
  void testPutObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean toOverwrite,
      boolean expected)
      throws Exception {

    if (client.objectExists(catalog, schema, volume, objectPath, false)) {
      assertTrue(client.deleteObject(catalog, schema, volume, objectPath));
    }

    assertEquals(
        expected, client.putObject(catalog, schema, volume, objectPath, localPath, toOverwrite));
  }

  private static Stream<Arguments> provideParametersForPutObject() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "upload1.csv",
            "/tmp/download1.csv",
            false,
            true),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/upload2.csv",
            "/tmp/download2.csv",
            false,
            true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutAndGetTest")
  void testPutAndGet(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      boolean toOverwrite,
      String localPathForUpload,
      String localPathForDownload,
      String expectedContent)
      throws Exception {

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

  private static Stream<Arguments> provideParametersForPutAndGetTest() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "hello_world.txt",
            false,
            "/tmp/upload_hello_world.txt",
            "/tmp/download_hello_world.txt",
            "helloworld"));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutAndDeleteTest")
  void testPutAndDelete(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPathForUpload,
      String fileContent)
      throws Exception {

    Files.write(Paths.get(localPathForUpload), fileContent.getBytes(StandardCharsets.UTF_8));
    assertTrue(client.putObject(catalog, schema, volume, objectPath, localPathForUpload, false));
    assertTrue(client.objectExists(catalog, schema, volume, objectPath, false));
    assertTrue(client.deleteObject(catalog, schema, volume, objectPath));
    assertFalse(client.objectExists(catalog, schema, volume, objectPath, false));
  }

  private static Stream<Arguments> provideParametersForPutAndDeleteTest() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "test_hello_world.txt",
            "/tmp/upload_hello_world.txt",
            "helloworld"));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutAndGetOverwriteTest")
  void testPutAndGetOverwrite(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String initialContent,
      String overwriteContent)
      throws Exception {

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

  private static Stream<Arguments> provideParametersForPutAndGetOverwriteTest() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "overwrite.txt",
            "initialContent",
            "overwriteContent"));
  }

  private Connection getConnection() throws SQLException {
    String jdbcUrl = String.format(jdbcUrlTemplate, getFakeServiceHost(), HTTP_PATH);

    return DriverManager.getConnection(jdbcUrl, getDatabricksUser(), getDatabricksToken());
  }
}
