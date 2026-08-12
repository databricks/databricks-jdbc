package com.databricks.jdbc.comparator.suite;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

class ApcCoreProviderTest {

  @Test
  void positiveCoreCaseDoesNotTreatMatchingFailuresAsParity() throws Exception {
    Connection leftConnection = mock(Connection.class);
    Connection rightConnection = mock(Connection.class);
    Statement leftStatement = mock(Statement.class);
    Statement rightStatement = mock(Statement.class);
    when(leftConnection.createStatement()).thenReturn(leftStatement);
    when(rightConnection.createStatement()).thenReturn(rightStatement);
    when(leftStatement.executeQuery(anyString())).thenThrow(new SQLException("query failed"));

    ApcCoreProvider provider = new ApcCoreProvider();

    assertThrows(
        SQLException.class,
        () ->
            provider.execute(
                leftConnection, rightConnection, provider.getTestCases().get(0), "APC core"));
  }
}
