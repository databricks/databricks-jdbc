package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.DEFAULT_HTTP_EXCEPTION_SQLSTATE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.sql.*;
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
  @Mock IDatabricksStatementInternal statementInternal;

  @Test
  void testHandleFailedExecution() throws SQLException {
    String statementId = "statementId";
    String statement = "statement";
    when(connectionContext.getAuthMech()).thenReturn(AuthMech.PAT);
    when(connectionContext.getHostUrl()).thenReturn("https://pat.databricks.com");
    when(connectionContext.getToken()).thenReturn("pat-token");
    when(response.getStatus()).thenReturn(status);
    when(status.getState()).thenReturn(StatementState.CANCELED);
    when(status.getError()).thenReturn(errorInfo);
    when(status.getSqlState()).thenReturn(DEFAULT_HTTP_EXCEPTION_SQLSTATE);
    when(errorInfo.getMessage()).thenReturn("Error message");
    when(errorInfo.getErrorCode()).thenReturn(ServiceErrorCode.BAD_REQUEST);
    DatabricksSdkClient databricksSdkClient =
        new DatabricksSdkClient(connectionContext, statementExecutionService, apiClient);
    assertThrows(SQLException.class, () -> databricksSdkClient.getMoreResults(statementInternal));
    SQLException thrown =
        assertThrows(
            SQLException.class,
            () -> databricksSdkClient.handleFailedExecution(response, statementId, statement));
    assertEquals(
        "Statement execution failed statementId -> statement\nCANCELED. Error Message: Error message, Error code: BAD_REQUEST",
        thrown.getMessage());
    assertEquals(DEFAULT_HTTP_EXCEPTION_SQLSTATE, thrown.getSQLState());
  }
}
