package com.databricks.jdbc.core;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.client.impl.sdk.DatabricksUCVolumeClient;
import java.sql.*;
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

  private String createListQuery(String catalog, String schema, String volume) {
    return String.format("LIST '/Volumes/%s/%s/%s/'", catalog, schema, volume);
  }

  @ParameterizedTest
  @MethodSource("provideParametersForPrefixExists")
  public void testPrefixExists(String volume, String prefix, boolean expected) throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String listFilesSQL = createListQuery(TEST_CATALOG, TEST_SCHEMA, volume);
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
    String listFilesSQL = createListQuery(TEST_CATALOG, TEST_SCHEMA, volume);

    when(statement.executeQuery(createListQuery(TEST_CATALOG, TEST_SCHEMA, "abc_volume1")))
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
    String listFilesSQL = createListQuery(TEST_CATALOG, TEST_SCHEMA, volume);

    if (volume.equals("abc_volume1")) {
      when(statement.executeQuery(createListQuery(TEST_CATALOG, TEST_SCHEMA, "abc_volume1")))
          .thenReturn(resultSet_abc_volume1);
      when(resultSet_abc_volume1.next()).thenReturn(true, true, false);
      when(resultSet_abc_volume1.getString("name")).thenReturn("abc_file3", "abc_file1");
    } else if (volume.equals("abc_volume2")) {
      when(statement.executeQuery(createListQuery(TEST_CATALOG, TEST_SCHEMA, "abc_volume2")))
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
    String listFilesSQL = createListQuery(TEST_CATALOG, TEST_SCHEMA, volume);

    when(statement.executeQuery(createListQuery(TEST_CATALOG, TEST_SCHEMA, "abc_volume1")))
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
}
