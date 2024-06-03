package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.client.impl.sdk.DatabricksUCVolumeClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class UCVolumeTests {

  private DatabricksUCVolumeClient client;
  private Connection con;

  @BeforeEach
  void setUp() throws SQLException {
    // TO DO: Testing is done here using the E2-Dogfood environment. Need to update this to use a
    // test warehouse.
    con = getDogfoodJDBCConnection();
    System.out.println("Connection established......");
    client = new DatabricksUCVolumeClient(con);
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
        Arguments.of(UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "aBc", true, true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExistsCaseSensitivity")
  void testObjectExistsCaseSensitivity(
      String catalog,
      String schema,
      String volume,
      String objectName,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectName, caseSensitive);
    assertEquals(expected, result);
  }

  private static Stream<Arguments> provideParametersForObjectExistsCaseSensitivity() {
    return Stream.of(
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file1.csv", true, false),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "aBc_file1.csv", true, true),
        Arguments.of(
            UC_VOLUME_CATALOG, UC_VOLUME_SCHEMA, "test_volume1", "abc_file1.csv", false, true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExistsVolumeReferencing")
  void testObjectExistsVolumeReferencing(
      String catalog,
      String schema,
      String volume,
      String objectName,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectName, caseSensitive);
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
      String objectName,
      boolean caseSensitive,
      boolean expected)
      throws Exception {
    boolean result = client.objectExists(catalog, schema, volume, objectName, caseSensitive);
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
}
