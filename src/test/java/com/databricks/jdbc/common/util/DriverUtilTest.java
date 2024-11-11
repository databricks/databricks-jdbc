package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DriverUtilTest {
  @Mock IDatabricksConnection connection;
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock IDatabricksStatement statement;
  @Mock IDatabricksResultSet resultSet;

  @ParameterizedTest
  @CsvSource({
    "2023.99, true",
    "2024.30, false",
    "2024.29, true",
    "2024.31, false",
    "2025.0, false",
  })
  void testDriverSupportInSEA(String dbsqlVersion, boolean throwsError) throws SQLException {
    when(connection.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SQL_EXEC);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SELECT current_version().dbsql_version")).thenReturn(resultSet);
    when(resultSet.getString(1)).thenReturn(dbsqlVersion);
    if (throwsError) {
      assertThrows(
          DatabricksValidationException.class,
          () -> DriverUtil.ensureUpdatedDBRVersionInUse(connection));
    } else {
      assertDoesNotThrow(() -> DriverUtil.ensureUpdatedDBRVersionInUse(connection));
    }
  }

  @Test
  void testDriverSupportInThrift() {
    when(connection.getConnectionContext()).thenReturn(connectionContext);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.THRIFT);
    assertDoesNotThrow(() -> DriverUtil.ensureUpdatedDBRVersionInUse(connection));
  }
}
