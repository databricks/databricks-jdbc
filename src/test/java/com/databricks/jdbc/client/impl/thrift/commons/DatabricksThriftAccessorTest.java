package com.databricks.jdbc.client.impl.thrift.commons;

import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_BYTE_LIMIT;
import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_ROW_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.DatabricksSQLException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksThriftAccessorTest {
  @Mock TCLIService.Client thriftClient;
  DatabricksThriftAccessor accessor;

  @Test
  void testOpenSession() throws TException, DatabricksSQLException {
    accessor = new DatabricksThriftAccessor(thriftClient);
    TOpenSessionReq request = new TOpenSessionReq();
    TOpenSessionResp response = new TOpenSessionResp();
    when(thriftClient.OpenSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request, CommandName.OPEN_SESSION, null), response);
  }

  @Test
  void testCloseSession() throws TException, DatabricksSQLException {
    accessor = new DatabricksThriftAccessor(thriftClient);
    TCloseSessionReq request = new TCloseSessionReq();
    TCloseSessionResp response = new TCloseSessionResp();
    when(thriftClient.CloseSession(request)).thenReturn(response);
    assertEquals(accessor.getThriftResponse(request, CommandName.CLOSE_SESSION, null), response);
  }

  @Test
  void testExecute() throws TException, DatabricksSQLException {
    accessor = new DatabricksThriftAccessor(thriftClient);
    TExecuteStatementReq request = new TExecuteStatementReq();
    TOperationHandle tOperationHandle = new TOperationHandle();
    TFetchResultsResp response =
        new TFetchResultsResp().setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    TGetResultSetMetadataResp metadataResp = new TGetResultSetMetadataResp();
    TExecuteStatementResp tExecuteStatementResp =
        new TExecuteStatementResp()
            .setOperationHandle(tOperationHandle)
            .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS));
    TFetchResultsReq fetchResultsReq =
        new TFetchResultsReq()
            .setOperationHandle(tOperationHandle)
            .setIncludeResultSetMetadata(true)
            .setFetchType((short) 0)
            .setMaxRows(DEFAULT_ROW_LIMIT)
            .setMaxBytes(DEFAULT_BYTE_LIMIT);
    TGetResultSetMetadataReq resultSetMetadataReq =
        new TGetResultSetMetadataReq().setOperationHandle(tOperationHandle);
    when(thriftClient.GetResultSetMetadata(resultSetMetadataReq)).thenReturn(metadataResp);
    when(thriftClient.FetchResults(fetchResultsReq)).thenReturn(response);
    when(thriftClient.ExecuteStatement(request)).thenReturn(tExecuteStatementResp);
    TFetchResultsResp actualResponse =
        (TFetchResultsResp)
            accessor.getThriftResponse(request, CommandName.EXECUTE_STATEMENT, null);
    assertEquals(actualResponse, response.setResultSetMetadata(metadataResp));
  }
}
