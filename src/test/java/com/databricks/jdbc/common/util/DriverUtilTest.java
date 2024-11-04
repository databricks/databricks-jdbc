package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DriverUtilTest {
  @Mock IDatabricksConnection connection;
  @Mock IDatabricksStatement statement;
  @Mock IDatabricksResultSet resultSet;

  @ParameterizedTest
  @CsvSource({"2024.30, true", "2024.29, false", "2024.31, true", "2025.0, true", "2023.99, false"})
  void testDoesDriverSupportSEA(String dbsqlVersion, boolean expectedResult) throws SQLException {
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery("SELECT current_version().dbsql_version")).thenReturn(resultSet);
    when(resultSet.getString(1)).thenReturn(dbsqlVersion);
    boolean result = DriverUtil.isUpdatedDBRVersionInUse(connection);
    assertEquals(
        expectedResult,
        result,
        String.format("Expected doesDriverSupportSEA(%s) to be %s", dbsqlVersion, expectedResult));
  }
}
