package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.util.DriverUtil.DBSQL_VERSION_SQL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DriverUtilTest {
  private static final String TEST_HTTP_PATH = "/sql/1.0/warehouses/warehouse_id";
  private static final String TEST_HTTP_PATH_2 = "/sql/1.0/warehouses/warehouse_id_2";
  @Mock IDatabricksConnection connection;
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock IDatabricksStatement statement;
  @Mock IDatabricksResultSet resultSet;

  @BeforeEach
  void setUp() {
    DriverUtil.clearDBSQLVersionCache(); // Clear the cache before each test case
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "2023.99, true",
        "2024.30, false",
        "2024.29, true",
        "2024.31, false",
        "2025.0, false",
        "'', false", // empty string
        "' ', false" // string with one space
      })
  void testDriverSupportInSEA(String dbsqlVersion, boolean throwsError) throws SQLException {
    when(connection.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SQL_EXEC);
    when(connectionContext.getHttpPath()).thenReturn(TEST_HTTP_PATH);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SELECT current_version().dbsql_version")).thenReturn(resultSet);
    when(resultSet.getString(1)).thenReturn(dbsqlVersion);

    if (throwsError) {
      assertThrows(
          DatabricksValidationException.class,
          () -> DriverUtil.ensureUpdatedDBSQLVersionInUse(connection));
    } else {
      assertDoesNotThrow(() -> DriverUtil.ensureUpdatedDBSQLVersionInUse(connection));
    }
  }

  @Test
  void testDriverSupportInThrift() {
    when(connection.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.THRIFT);

    assertDoesNotThrow(() -> DriverUtil.ensureUpdatedDBSQLVersionInUse(connection));
  }

  @Test
  void testCacheIsSeparateForDifferentHttpPaths() throws SQLException {
    when(connection.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SQL_EXEC);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(DBSQL_VERSION_SQL)).thenReturn(resultSet);

    // First connection with TEST_HTTP_PATH
    when(connectionContext.getHttpPath()).thenReturn(TEST_HTTP_PATH);
    when(resultSet.getString(1)).thenReturn("2024.30");
    DriverUtil.ensureUpdatedDBSQLVersionInUse(connection);

    // Second connection with TEST_HTTP_PATH_2
    when(connectionContext.getHttpPath()).thenReturn(TEST_HTTP_PATH_2);
    when(resultSet.getString(1)).thenReturn("2024.31");
    DriverUtil.ensureUpdatedDBSQLVersionInUse(connection);

    // Verify that the statement was executed twice (once for each path)
    verify(statement, times(2)).executeQuery(DBSQL_VERSION_SQL);
  }

  @Test
  void testCacheIsReusedForSameHttpPath() throws SQLException {
    when(connection.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SQL_EXEC);
    when(connectionContext.getHttpPath()).thenReturn(TEST_HTTP_PATH);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(DBSQL_VERSION_SQL)).thenReturn(resultSet);
    when(resultSet.getString(1)).thenReturn("2024.30");

    // Call twice with the same HTTP path
    DriverUtil.ensureUpdatedDBSQLVersionInUse(connection);
    DriverUtil.ensureUpdatedDBSQLVersionInUse(connection);

    // Verify that the statement was executed only once
    verify(statement, times(1)).executeQuery(DBSQL_VERSION_SQL);
  }
}
