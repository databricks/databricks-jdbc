package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.TestConstants.UC_VOLUME_CATALOG;
import static com.databricks.jdbc.TestConstants.UC_VOLUME_SCHEMA;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.client.impl.sdk.DatabricksUCVolumeClient;
import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Integration tests for UC Volume Client. */
public class UCVolumeIntegrationTests extends AbstractFakeServiceIntegrationTests {

  private DatabricksUCVolumeClient client;
  private Connection con;

  @BeforeAll
  static void beforeAll() {
    setDatabricksApiTargetUrl("https://e2-dogfood.staging.cloud.databricks.com");
    setCloudFetchApiTargetUrl("https://e2-dogfood-core.s3.us-west-2.amazonaws.com");
  }

  @BeforeEach
  void setUp() throws SQLException {
    if (!isAllpurposeCluster()) {
      // TODO: Testing is done here using the E2-Dogfood environment. Need to update this to use a
      // test warehouse.
      con = getValidDogfoodConnection();
      System.out.println("Connection established......");
      client = new DatabricksUCVolumeClient(con);
    }
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
    if (!isAllpurposeCluster()) {
      boolean result = client.prefixExists(catalog, schema, volume, prefix, caseSensitive);
      assertEquals(expected, result);
    }
  }

  private static Stream<Arguments> provideParametersForPrefixExists() {
    return Stream.of(
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "folder1/ab", true, true));
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
    if (!isAllpurposeCluster()) {
      boolean result = client.objectExists(catalog, schema, volume, objectPath, caseSensitive);
      assertEquals(expected, result);
    }
  }

  private static Stream<Arguments> provideParametersForObjectExistsCaseSensitivity() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file1.csv", true, false),
        Arguments.of(
            UC_VOLUME_CATALOG,
            UC_VOLUME_SCHEMA,
            "test_volume1",
            "folder1/ABC_file1.csv",
            false,
            true));
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
    if (!isAllpurposeCluster()) {
      boolean result = client.objectExists(catalog, schema, volume, objectPath, caseSensitive);
      assertEquals(expected, result);
    }
  }

  private static Stream<Arguments> provideParametersForObjectExistsVolumeReferencing() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file3.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume2", "abc_file4.csv", true, true));
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
    if (!isAllpurposeCluster()) {
      boolean result = client.objectExists(catalog, schema, volume, objectPath, caseSensitive);
      assertEquals(expected, result);
    }
  }

  private static Stream<Arguments> provideParametersForObjectExistsSpecialCharacters() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "@!aBc_file1.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "#_file3.csv", true, false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForVolumeExists")
  void testVolumeExists(
      String catalog, String schema, String volumeName, boolean caseSensitive, boolean expected)
      throws Exception {

    if (!isAllpurposeCluster()) {
      assertEquals(expected, client.volumeExists(catalog, schema, volumeName, caseSensitive));
    }
  }

  private static Stream<Arguments> provideParametersForVolumeExists() {
    return Stream.of(
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", true, true),
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
    if (!isAllpurposeCluster()) {
      assertEquals(expected, client.listObjects(catalog, schema, volume, prefix, caseSensitive));
    }
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
            Arrays.asList("aBc_file1.csv", "abc_file2.csv")));
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
    if (!isAllpurposeCluster()) {
      assertEquals(expected, client.listObjects(catalog, schema, volume, prefix, caseSensitive));
    }
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

    if (!isAllpurposeCluster()) {
      assertEquals(expected, client.listObjects(catalog, schema, volume, prefix, caseSensitive));
    }
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
            false,
            Arrays.asList("aBC_file3.csv", "abc_file2.csv", "abc_file4.csv")));
  }
}
