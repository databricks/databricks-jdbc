package com.databricks.jdbc.client.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.sql.ServiceError;
import com.databricks.sdk.service.sql.StatementExecutionService;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksSdkClientTest {
  @Mock ExecuteStatementResponse response;
  @Mock StatementStatus status;
  @Mock ServiceError errorInfo;
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock StatementExecutionService statementExecutionService;
  @Mock ApiClient apiClient;

  @Test
  void testHandleFailedExecution() throws SQLException {
    String statementId = "statementId";
    String statement = "statement";
    when(response.getStatus()).thenReturn(status);
    when(status.getState()).thenReturn(StatementState.CANCELED);
    when(status.getError()).thenReturn(errorInfo);
    when(errorInfo.getMessage()).thenReturn("Error message");
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    SQLException thrown =
        assertThrows(
            SQLException.class,
            () -> databricksSdkClient.handleFailedExecution(response, statementId, statement));
    assertEquals(
        "Statement execution failed statementId -> statement\n" + "CANCELED: Error message",
        thrown.getMessage());
  }

  @Test
  void testHandleFailedExecutionWithInvalidState() throws DatabricksParsingException {
    when(response.getStatus()).thenReturn(status);
    when(status.getState()).thenReturn(StatementState.PENDING); // Assuming PENDING is not handled
    when(status.getError()).thenReturn(errorInfo);
    when(errorInfo.getMessage()).thenReturn("This error should not occur");

    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                databricksSdkClient.handleFailedExecution(
                    response, "statementId", "SELECT * FROM table"));
    assertEquals("Invalid state for error", thrown.getMessage());
  }
}
