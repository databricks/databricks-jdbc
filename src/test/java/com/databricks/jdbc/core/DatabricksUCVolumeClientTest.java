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

  @ParameterizedTest
  @MethodSource("provideParametersForPrefixExists")
  public void testPrefixExists(String volume, String prefix, boolean expected) throws SQLException {
    DatabricksUCVolumeClient client = new DatabricksUCVolumeClient(connection);

    when(connection.createStatement()).thenReturn(statement);
    String listFilesSQL =
        "LIST '/Volumes/" + TEST_CATALOG + "/" + TEST_SCHEMA + "/" + volume + "/'";
    when(statement.executeQuery(listFilesSQL)).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true, true, true, true, true, false);
    when(resultSet.getString("name"))
        .thenReturn("aBc_file1", "abC_file2", "def_file1", "efg_file2", "#!#_file3");

    boolean exists = client.prefixExists(TEST_CATALOG, TEST_SCHEMA, volume, prefix, true);

    assertEquals(expected, exists);
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
}
