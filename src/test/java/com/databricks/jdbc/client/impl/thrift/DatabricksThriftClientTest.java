package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.getNamespace;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.CATALOG;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.SCHEMA;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.*;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.SQLException;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksThriftClientTest {
  @Mock DatabricksThriftAccessor thriftAccessor;
  @Mock IDatabricksSession session;
  @Mock TRowSet resultData;
  @Mock TGetResultSetMetadataResp resultMetadataData;

  @Test
  void testCreateSession() throws DatabricksSQLException {
    DatabricksThriftClient client = new DatabricksThriftClient(thriftAccessor);
    TOpenSessionReq openSessionReq =
        new TOpenSessionReq()
            .setInitialNamespace(getNamespace(CATALOG, SCHEMA))
            .setConfiguration(EMPTY_MAP)
            .setCanUseMultipleCatalogs(true)
            .setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);
    TOpenSessionResp openSessionResp =
        new TOpenSessionResp()
            .setSessionHandle(SESSION_HANDLE)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftAccessor.getThriftResponse(openSessionReq, CommandName.OPEN_SESSION, null))
        .thenReturn(openSessionResp);

    ImmutableSessionInfo actualResponse =
        client.createSession(CLUSTER_COMPUTE, CATALOG, SCHEMA, EMPTY_MAP);
    assertEquals(actualResponse.sessionHandle(), SESSION_HANDLE);
  }

  @Test
  void testCloseSession() throws DatabricksSQLException {
    DatabricksThriftClient client = new DatabricksThriftClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TCloseSessionReq closeSessionReq = new TCloseSessionReq().setSessionHandle(SESSION_HANDLE);
    TCloseSessionResp closeSessionResp =
        new TCloseSessionResp().setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    when(thriftAccessor.getThriftResponse(closeSessionReq, CommandName.CLOSE_SESSION, null))
        .thenReturn(closeSessionResp);
    assertDoesNotThrow(() -> client.deleteSession(session, CLUSTER_COMPUTE));
  }

  @Test
  void testExecute() throws SQLException {
    DatabricksThriftClient client = new DatabricksThriftClient(thriftAccessor);
    when(session.getSessionInfo()).thenReturn(SESSION_INFO);
    TExecuteStatementReq executeStatementReq =
        new TExecuteStatementReq().setStatement(TEST_STRING).setSessionHandle(SESSION_HANDLE);
    TFetchResultsResp fetchResultsResp =
        new TFetchResultsResp()
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .setResults(resultData)
            .setResultSetMetadata(resultMetadataData);
    when(thriftAccessor.getThriftResponse(executeStatementReq, CommandName.EXECUTE_STATEMENT, null))
        .thenReturn(fetchResultsResp);
    DatabricksResultSet resultSet =
        client.executeStatement(
            TEST_STRING, CLUSTER_COMPUTE, Collections.emptyMap(), StatementType.SQL, session, null);
    assertEquals(resultSet.getStatementStatus().getState(), StatementState.SUCCEEDED);
  }

  @Test
  void testUnimplementedFunctions() {
    DatabricksThriftClient client = new DatabricksThriftClient(thriftAccessor);
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> client.listFunctions(session, TEST_CATALOG, TEST_SCHEMA, TEST_FUNCTION_PATTERN));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class, () -> client.listTypeInfo(session));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class, () -> client.listCatalogs(session));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> client.listSchemas(session, TEST_CATALOG, TEST_SCHEMA));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> client.listTables(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> client.listColumns(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE, TEST_COLUMN));
    assertThrows(
        DatabricksSQLFeatureNotImplementedException.class,
        () -> client.listPrimaryKeys(session, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE));
  }
}
