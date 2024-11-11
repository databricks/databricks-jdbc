package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksVolumeClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class UCVolumeTests {

  private IDatabricksVolumeClient client;
  private Connection con;

  private static final String LOCAL_TEST_DIRECTORY = "/tmp";

  @BeforeEach
  void setUp() throws SQLException {
    con = getDogfoodJDBCConnection();
    System.out.println("Connection established......");
    client = ((IDatabricksConnection) con).getVolumeClient();
    con.setClientInfo("allowlistedVolumeOperationLocalFilePaths", LOCAL_TEST_DIRECTORY);
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (con != null) {
      con.close();
    }
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPrefixExists")
  void testPrefixExists(
      String catalog,
      String schema,
      String volume,
      String prefix,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.prefixExists(catalog, schema, volume, prefix, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForPrefixExists() {
    return Stream.of(
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc", true, true),
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "xyz", false, false),
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "dEf", false, true),
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "#!", true, true),
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "aBc", true, true),
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "folder1/ab", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "folder1/folder2/e", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/xyz",
            true,
            false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExistsCaseSensitivity")
  void testObjectExistsCaseSensitivity(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectPath, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForObjectExistsCaseSensitivity() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file1.csv", true, false),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "aBc_file1.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file1.csv", false, true),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/ABC_file1.csv",
            false,
            true),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/efg_file1.csv",
            true,
            true),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/xyz_file.csv",
            true,
            false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExistsVolumeReferencing")
  void testObjectExistsVolumeReferencing(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectPath, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForObjectExistsVolumeReferencing() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file3.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume2", "abc_file4.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file2.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume2", "abc_file2.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file4.csv", true, false),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume2", "abc_file3.csv", true, false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExistsSpecialCharacters")
  void testObjectExistsSpecialCharacters(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectPath, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForObjectExistsSpecialCharacters() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "@!aBc_file1.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "@aBc_file1.csv", true, false),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "#!#_file3.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "#_file3.csv", true, false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForVolumeExists")
  void testVolumeExists(
      String catalog, String schema, String volumeName, boolean caseSensitive, boolean expected)
      throws Exception {
    assertEquals(expected, client.volumeExists(catalog, schema, volumeName, caseSensitive));
  }

  private static Stream<Arguments> provideParametersForVolumeExists() {
    return Stream.of(
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", true, true),
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "###", true, true),
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume5", true, false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForListObjectsInSubFolders")
  void testListObjects_SubFolders(
      String catalog,
      String schema,
      String volume,
      String prefix,
      boolean caseSensitive,
      List<String> expected)
      throws Exception {
    assertEquals(expected, client.listObjects(catalog, schema, volume, prefix, caseSensitive));
  }

  private static Stream<Arguments> provideParametersForListObjectsInSubFolders() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "#",
            true,
            Arrays.asList("#!#_file1.csv", "#!#_file3.csv", "#!_file3.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/a",
            true,
            Arrays.asList("aBc_file1.csv", "abc_file2.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/folder2/efg",
            true,
            Arrays.asList("efg_file1.csv")));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForListObjectsVolumeReferencing")
  void testListObjects_VolumeReferencing(
      String catalog,
      String schema,
      String volume,
      String prefix,
      boolean caseSensitive,
      List<String> expected)
      throws Exception {
    assertEquals(expected, client.listObjects(catalog, schema, volume, prefix, caseSensitive));
  }

  private static Stream<Arguments> provideParametersForListObjectsVolumeReferencing() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "#",
            true,
            Arrays.asList("#!#_file1.csv", "#!#_file3.csv", "#!_file3.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume2",
            "a",
            true,
            Arrays.asList("aBC_file3.csv", "abc_file2.csv", "abc_file4.csv")));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForListObjectsCaseSensitivity_SpecialCharacters")
  void testListObjects_CaseSensitivity_SpecialCharacters(
      String catalog,
      String schema,
      String volume,
      String prefix,
      boolean caseSensitive,
      List<String> expected)
      throws Exception {
    assertEquals(expected, client.listObjects(catalog, schema, volume, prefix, caseSensitive));
  }

  private static Stream<Arguments>
      provideParametersForListObjectsCaseSensitivity_SpecialCharacters() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "#",
            true,
            Arrays.asList("#!#_file1.csv", "#!#_file3.csv", "#!_file3.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume2",
            "ab",
            true,
            Arrays.asList("abc_file2.csv", "abc_file4.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume2",
            "aB",
            true,
            Arrays.asList("aBC_file3.csv")),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume2",
            "ab",
            false,
            Arrays.asList("aBC_file3.csv", "abc_file2.csv", "abc_file4.csv")));
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
    assertEquals(expected, client.getObject(catalog, schema, volume, objectPath, localPath));
  }

  private static Stream<Arguments> provideParametersForGetObject() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "abc_file2.csv",
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
      String expectedContent)
      throws Exception {
    byte[] fileContent = Files.readAllBytes(Paths.get(localPath));
    String actualContent = new String(fileContent, StandardCharsets.UTF_8);

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
            "/tmp/downloadtest.csv",
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

    assertTrue(
        client.putObject(catalog, schema, volume, objectPath, localPathForUpload, toOverwrite));
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
            "hello_world.txt",
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

    String uniqueId = UUID.randomUUID().toString();
    String localPathForUpload = "/tmp/upload_overwrite_test_" + uniqueId + ".txt";
    String localPathForDownload = "/tmp/download_overwrite_test_" + uniqueId + ".txt";

    Files.write(Paths.get(localPathForUpload), initialContent.getBytes(StandardCharsets.UTF_8));
    assertTrue(client.putObject(catalog, schema, volume, objectPath, localPathForUpload, false));
    assertTrue(client.getObject(catalog, schema, volume, objectPath, localPathForDownload));
    byte[] fileContent = Files.readAllBytes(Paths.get(localPathForDownload));
    String actualContent = new String(fileContent, StandardCharsets.UTF_8);
    assertEquals(initialContent, actualContent);

    // re-initialise paths to avoid collision
    uniqueId = UUID.randomUUID().toString();
    localPathForUpload = "/tmp/upload_overwrite_test_" + uniqueId + ".txt";
    localPathForDownload = "/tmp/download_overwrite_test_" + uniqueId + ".txt";

    Files.write(Paths.get(localPathForUpload), overwriteContent.getBytes(StandardCharsets.UTF_8));
    assertTrue(client.putObject(catalog, schema, volume, objectPath, localPathForUpload, true));
    assertTrue(client.getObject(catalog, schema, volume, objectPath, localPathForDownload));
    fileContent = Files.readAllBytes(Paths.get(localPathForDownload));
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
}
