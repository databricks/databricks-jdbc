package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.log.LogCapture;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RemoteChunkProviderTest {

  private static final StatementId STATEMENT_ID = new StatementId("statement_id");
  @Mock private IDatabricksSession mockSession;

  @Test
  public void testInitEmptyChunkDownloader() {
    ResultManifest resultManifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setTotalRowCount(0L)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()));
    ResultData resultData = new ResultData().setExternalLinks(new ArrayList<>());
    when(mockSession.getConnectionContext()).thenReturn(mock(IDatabricksConnectionContext.class));
    when(mockSession.getConnectionContext().getClientType()).thenReturn(DatabricksClientType.SEA);
    assertDoesNotThrow(
        () ->
            new RemoteChunkProvider(
                STATEMENT_ID, resultManifest, resultData, mockSession, null, 4));
  }

  @Test
  public void apcMetadataExternalChunkDownloadRedactsStatementIdInLogs() throws Exception {
    String rawId = "private-apc-metadata-external-id";
    StatementId statementId =
        StatementId.forCurrentSession(
                rawId,
                ImmutableSessionInfo.builder()
                    .clientType(DatabricksClientType.SEA)
                    .sessionId("session")
                    .computeResource(new AllPurposeCluster("org", "cluster"))
                    .build())
            .withRedactedDiagnostics();
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    assertFalse(context.isBoundedSeaApiEnabled());
    when(mockSession.getConnectionContext()).thenReturn(context);
    when(context.getClientType()).thenReturn(DatabricksClientType.SEA);
    when(context.getChunkReadyTimeoutSeconds()).thenReturn(30);

    byte[] arrowData = TemporalArrowTestData.create();
    IDatabricksHttpClient httpClient = mock(IDatabricksHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    HttpEntity entity = mock(HttpEntity.class);
    when(httpClient.execute(
            org.mockito.ArgumentMatchers.any(HttpGet.class), org.mockito.ArgumentMatchers.eq(true)))
        .thenReturn(response);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContent()).thenAnswer(ignored -> new ByteArrayInputStream(arrowData));
    when(entity.getContentLength()).thenReturn((long) arrowData.length);

    ResultManifest manifest =
        new ResultManifest()
            .setTotalChunkCount(1L)
            .setTotalRowCount(2L)
            .setResultCompression(CompressionCodec.NONE)
            .setChunks(
                List.of(new BaseChunkInfo().setChunkIndex(0L).setRowOffset(0L).setRowCount(2L)))
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()).setColumnCount(0L));
    ResultData data =
        new ResultData()
            .setExternalLinks(
                List.of(
                    new ExternalLink()
                        .setChunkIndex(0L)
                        .setRowOffset(0L)
                        .setRowCount(2L)
                        .setExpiration("2099-01-01T00:00:00Z")
                        .setExternalLink("https://example.com/chunk")));

    try (LogCapture logs = new LogCapture()) {
      RemoteChunkProvider provider =
          new RemoteChunkProvider(statementId, manifest, data, mockSession, httpClient, 1);
      try {
        provider.downloadNextChunks();
        provider.getChunkByIndex(0L).getChunkReadyFuture().get(10, TimeUnit.SECONDS);
        assertTrue(logs.getOutput().contains("ChunkDownloadTask complete: statementId=[REDACTED]"));
        assertFalse(logs.getOutput().contains(rawId));
      } finally {
        provider.close();
      }
    }
    assertEquals(rawId, statementId.toSQLExecStatementId());
  }
}
