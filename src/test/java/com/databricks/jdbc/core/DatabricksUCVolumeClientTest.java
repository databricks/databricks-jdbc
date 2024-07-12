package com.databricks.jdbc.core;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_SUCCEEDED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.client.impl.sdk.DatabricksUCVolumeClient;
import java.sql.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksUCVolumeClientTest {

  @Mock Connection connection;

  @Mock Statement statement;

  @Mock ResultSet resultSet;
  @Mock ResultSet resultSet_abc_volume1;
  @Mock ResultSet resultSet_abc_volume2;

  @ParameterizedTest
  @MethodSource("provideParametersForPrefixExists")
  public void testPrefixExists(String volume, String prefix, boolean expected) throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String listFilesSQL =
        String.format("LIST '/Volumes/%s/%s/%s/'", TEST_CATALOG, TEST_SCHEMA, volume);
    when(statement.executeQuery(listFilesSQL)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, true, true, true, false);
    when(resultSet.getString("name"))
        .thenReturn("aBc_file1", "abC_file2", "def_file1", "efg_file2", "#!#_file3");

    assertEquals(expected, client.objectExists(TEST_CATALOG, TEST_SCHEMA, volume, prefix));
    verify(statement).executeQuery(listFilesSQL);
  }

  private static Stream<Arguments> provideParametersForPrefixExists() {
    return Stream.of(
        Arguments.of("abc_volume1", "abc", false),
        Arguments.of("abc_volume2", "xyz", false),
        Arguments.of("abc_volume2", "def", true),
        Arguments.of("abc_volume2", "#!", true),
        Arguments.of("abc_volume2", "aBc", true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExists_CaseSensitivity")
  public void testObjectExistsCaseSensitivity(
      String volume, String objectPath, boolean caseSensitive, boolean expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String listFilesSQL =
        String.format("LIST '/Volumes/%s/%s/%s/'", TEST_CATALOG, TEST_SCHEMA, volume);

    when(statement.executeQuery(
            String.format("LIST '/Volumes/%s/%s/%s/'", TEST_CATALOG, TEST_SCHEMA, "abc_volume1")))
        .thenReturn(resultSet_abc_volume1);
    when(resultSet_abc_volume1.next()).thenReturn(true, false);
    when(resultSet_abc_volume1.getString("name")).thenReturn("aBc_file1");

    assertEquals(
        expected,
        client.objectExists(TEST_CATALOG, TEST_SCHEMA, volume, objectPath, caseSensitive));
    verify(statement).executeQuery(listFilesSQL);
  }

  private static Stream<Arguments> provideParametersForObjectExists_CaseSensitivity() {
    return Stream.of(
        Arguments.of("abc_volume1", "abc_file1", true, false),
        Arguments.of("abc_volume1", "aBc_file1", true, true),
        Arguments.of("abc_volume1", "abc_file1", false, true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExists_VolumeReferencing")
  public void testObjectExistsVolumeReferencing(
      String volume, String objectPath, boolean caseSensitive, boolean expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String listFilesSQL =
        String.format("LIST '/Volumes/%s/%s/%s/'", TEST_CATALOG, TEST_SCHEMA, volume);

    if (volume.equals("abc_volume1")) {
      when(statement.executeQuery(
              String.format("LIST '/Volumes/%s/%s/%s/'", TEST_CATALOG, TEST_SCHEMA, "abc_volume1")))
          .thenReturn(resultSet_abc_volume1);
      when(resultSet_abc_volume1.next()).thenReturn(true, true, false);
      when(resultSet_abc_volume1.getString("name")).thenReturn("abc_file3", "abc_file1");
    } else if (volume.equals("abc_volume2")) {
      when(statement.executeQuery(
              String.format("LIST '/Volumes/%s/%s/%s/'", TEST_CATALOG, TEST_SCHEMA, "abc_volume2")))
          .thenReturn(resultSet_abc_volume2);
      when(resultSet_abc_volume2.next()).thenReturn(true, true, false);
      when(resultSet_abc_volume2.getString("name")).thenReturn("abc_file4", "abc_file1");
    }

    assertEquals(
        expected,
        client.objectExists(TEST_CATALOG, TEST_SCHEMA, volume, objectPath, caseSensitive));
    verify(statement).executeQuery(listFilesSQL);
  }

  private static Stream<Arguments> provideParametersForObjectExists_VolumeReferencing() {
    return Stream.of(
        Arguments.of("abc_volume1", "abc_file3", true, true),
        Arguments.of("abc_volume2", "abc_file4", true, true),
        Arguments.of("abc_volume1", "abc_file1", true, true),
        Arguments.of("abc_volume2", "abc_file1", true, true),
        Arguments.of("abc_volume1", "abc_file4", true, false),
        Arguments.of("abc_volume2", "abc_file3", true, false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForObjectExists_SpecialCharacters")
  public void testObjectExistsSpecialCharacters(
      String volume, String objectPath, boolean caseSensitive, boolean expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String listFilesSQL =
        String.format("LIST '/Volumes/%s/%s/%s/'", TEST_CATALOG, TEST_SCHEMA, volume);

    when(statement.executeQuery(
            String.format("LIST '/Volumes/%s/%s/%s/'", TEST_CATALOG, TEST_SCHEMA, "abc_volume1")))
        .thenReturn(resultSet_abc_volume1);
    when(resultSet_abc_volume1.next()).thenReturn(true, true, false);
    when(resultSet_abc_volume1.getString("name")).thenReturn("@!aBc_file1", "#!#_file3");

    assertEquals(
        expected,
        client.objectExists(TEST_CATALOG, TEST_SCHEMA, volume, objectPath, caseSensitive));
    verify(statement).executeQuery(listFilesSQL);
  }

  private static Stream<Arguments> provideParametersForObjectExists_SpecialCharacters() {
    return Stream.of(
        Arguments.of("abc_volume1", "@!aBc_file1", true, true),
        Arguments.of("abc_volume1", "@aBc_file1", true, false),
        Arguments.of("abc_volume1", "#!#_file3", true, true),
        Arguments.of("abc_volume1", "#_file3", true, false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForVolumeExists")
  public void testVolumeExists(String volumeName, boolean caseSensitive, boolean expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String showVolumesSQL = String.format("SHOW VOLUMES IN %s.%s", TEST_CATALOG, TEST_SCHEMA);
    when(statement.executeQuery(showVolumesSQL)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, true, true, true, false);
    when(resultSet.getString("volume_name"))
        .thenReturn("aBc_volume1", "abC_volume2", "def_volume1", "efg_volume2", "#!#_volume3");

    assertEquals(
        expected, client.volumeExists(TEST_CATALOG, TEST_SCHEMA, volumeName, caseSensitive));
    verify(statement).executeQuery(showVolumesSQL);
  }

  private static Stream<Arguments> provideParametersForVolumeExists() {
    return Stream.of(
        Arguments.of("abc_volume1", true, false),
        Arguments.of("abc_volume1", false, true),
        Arguments.of("def_volume1", true, true),
        Arguments.of("#!#_volume3", true, true),
        Arguments.of("aBC_volume1", true, false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForListObjects")
  public void testListObjects(String volume, String prefix, List<String> expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String listFilesSQL =
        String.format("LIST '/Volumes/%s/%s/%s/'", TEST_CATALOG, TEST_SCHEMA, volume);
    when(statement.executeQuery(listFilesSQL)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, true, true, true, true, true, false);
    when(resultSet.getString("name"))
        .thenReturn(
            "aBc_file1",
            "abC_file2",
            "def_file1",
            "efg_file2",
            "#!#_file3",
            "xyz_file4",
            "###file1");

    List<String> filenames = client.listObjects(TEST_CATALOG, TEST_SCHEMA, volume, prefix, true);

    assertEquals(expected.size(), filenames.size());
    assertTrue(filenames.containsAll(expected));
    verify(statement).executeQuery(listFilesSQL);
  }

  private static Stream<Arguments> provideParametersForListObjects() {
    return Stream.of(
        Arguments.of("abc_volume1", "a", Arrays.asList("aBc_file1", "abC_file2")),
        Arguments.of("abc_volume1", "aBC", Collections.emptyList()),
        Arguments.of("abc_volume1", "xyz", Collections.singletonList("xyz_file4")),
        Arguments.of("abc_volume1", "aB", Collections.singletonList("aBc_file1")),
        Arguments.of("abc_volume1", "#", Arrays.asList("#!#_file3", "###file1")),
        Arguments.of("abc_volume1", "aBc", Collections.singletonList("aBc_file1")),
        Arguments.of(
            "abc_volume2",
            "",
            Arrays.asList(
                "aBc_file1",
                "abC_file2",
                "def_file1",
                "efg_file2",
                "#!#_file3",
                "xyz_file4",
                "###file1")));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForGetObject")
  public void testGetObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String getObjectQuery =
        String.format(
            "GET '/Volumes/%s/%s/%s/%s' TO '%s'", catalog, schema, volume, objectPath, localPath);
    when(statement.executeQuery(getObjectQuery)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME))
        .thenReturn(VOLUME_OPERATION_STATUS_SUCCEEDED);
    boolean result = client.getObject(catalog, schema, volume, objectPath, localPath);

    assertEquals(expected, result);
    verify(statement).executeQuery(getObjectQuery);
  }

  private static Stream<Arguments> provideParametersForGetObject() {
    return Stream.of(
        Arguments.of(
            "test_catalog",
            "test_schema",
            "test_volume",
            "test_objectPath",
            "test_localPath",
            true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForGetObject_FileNotFound")
  public void testGetObject_FileNotFound(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String getObjectQuery =
        String.format(
            "GET '/Volumes/%s/%s/%s/%s' TO '%s'", catalog, schema, volume, objectPath, localPath);
    when(statement.executeQuery(getObjectQuery))
        .thenThrow(new SQLException("Volume operation failed : Failed to download file"));

    assertThrows(
        SQLException.class,
        () -> {
          client.getObject(catalog, schema, volume, objectPath, localPath);
        });
    verify(statement).executeQuery(getObjectQuery);
  }

  private static Stream<Arguments> provideParametersForGetObject_FileNotFound() {
    return Stream.of(
        Arguments.of(
            "test_catalog",
            "test_schema",
            "test_volume",
            "non_existent_file",
            "test_localPath",
            false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutObject")
  public void testPutObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean toOverwrite,
      boolean expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String putObjectQuery =
        String.format(
            "PUT '%s' INTO '/Volumes/%s/%s/%s/%s'%s",
            localPath, catalog, schema, volume, objectPath, toOverwrite ? " OVERWRITE" : "");
    when(statement.executeQuery(putObjectQuery)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME))
        .thenReturn(VOLUME_OPERATION_STATUS_SUCCEEDED);
    boolean result = client.putObject(catalog, schema, volume, objectPath, localPath, toOverwrite);

    assertEquals(expected, result);
    verify(statement).executeQuery(putObjectQuery);
  }

  private static Stream<Arguments> provideParametersForPutObject() {
    return Stream.of(
        Arguments.of(
            "test_catalog",
            "test_schema",
            "test_volume",
            "test_objectpath",
            "test_localpath",
            false,
            true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutObject_InvalidLocalPath")
  public void testPutObject_InvalidLocalPath(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean toOverwrite)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String putObjectQuery =
        String.format(
            "PUT '%s' INTO '/Volumes/%s/%s/%s/%s'%s",
            localPath, catalog, schema, volume, objectPath, toOverwrite ? " OVERWRITE" : "");
    when(statement.executeQuery(putObjectQuery))
        .thenThrow(new SQLException("Invalid local path: File not found or is a directory"));

    assertThrows(
        SQLException.class,
        () -> {
          client.putObject(catalog, schema, volume, objectPath, localPath, toOverwrite);
        });
    verify(statement).executeQuery(putObjectQuery);
  }

  private static Stream<Arguments> provideParametersForPutObject_InvalidLocalPath() {
    return Stream.of(
        Arguments.of(
            "test_catalog",
            "test_schema",
            "test_volume",
            "test_objectpath",
            "invalid_localpath",
            false));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPutObject_OverwriteExistingFile")
  public void testPutObject_OverwriteExistingFile(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean toOverwrite,
      boolean expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String putObjectQuery =
        String.format(
            "PUT '%s' INTO '/Volumes/%s/%s/%s/%s'%s",
            localPath, catalog, schema, volume, objectPath, toOverwrite ? " OVERWRITE" : "");
    when(statement.executeQuery(putObjectQuery)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME))
        .thenReturn(VOLUME_OPERATION_STATUS_SUCCEEDED);

    boolean result = client.putObject(catalog, schema, volume, objectPath, localPath, toOverwrite);

    assertEquals(expected, result);
    verify(statement).executeQuery(putObjectQuery);
  }

  private static Stream<Arguments> provideParametersForPutObject_OverwriteExistingFile() {
    return Stream.of(
        Arguments.of(
            "test_catalog",
            "test_schema",
            "test_volume",
            "existing_objectpath",
            "valid_localpath",
            true,
            true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForDeleteObject")
  public void testDeleteObject(
      String catalog, String schema, String volume, String objectPath, boolean expected)
      throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String deleteObjectQuery =
        String.format("REMOVE '/Volumes/%s/%s/%s/%s'", catalog, schema, volume, objectPath);
    when(statement.executeQuery(deleteObjectQuery)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME))
        .thenReturn(VOLUME_OPERATION_STATUS_SUCCEEDED);
    boolean result = client.deleteObject(catalog, schema, volume, objectPath);

    assertEquals(expected, result);
    verify(statement).executeQuery(deleteObjectQuery);
  }

  private static Stream<Arguments> provideParametersForDeleteObject() {
    return Stream.of(Arguments.of("test_catalog", "test_schema", "test_volume", "test_path", true));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForDeleteObject_InvalidObjectPath")
  public void testDeleteObject_InvalidObjectPath(
      String catalog, String schema, String volume, String objectPath) throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String deleteObjectQuery =
        String.format("REMOVE '/Volumes/%s/%s/%s/%s'", catalog, schema, volume, objectPath);
    when(statement.executeQuery(deleteObjectQuery))
        .thenThrow(new SQLException("Invalid object path: Object not found"));

    assertThrows(
        SQLException.class,
        () -> {
          client.deleteObject(catalog, schema, volume, objectPath);
        });
    verify(statement).executeQuery(deleteObjectQuery);
  }

  private static Stream<Arguments> provideParametersForDeleteObject_InvalidObjectPath() {
    return Stream.of(
        Arguments.of("test_catalog", "test_schema", "test_volume", "invalid_objectpath"));
  }
}
