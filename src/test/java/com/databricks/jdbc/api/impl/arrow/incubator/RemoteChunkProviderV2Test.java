package com.databricks.jdbc.api.impl.arrow.incubator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RemoteChunkProviderV2Test {

  private static final String STATEMENT_ID = "test-statement-123";
  private static final int MAX_PARALLEL_DOWNLOADS = 3;
  private static final String EXPIRY_TIME = "2025-01-08T00:00:00Z";
  @Mock private IDatabricksSession mockSession;
  @Mock private IDatabricksHttpClient mockHttpClient;
  @Mock private IDatabricksStatementInternal mockStatement;
  @Mock private IDatabricksClient mockDatabricksClient;

  @Test
  void shouldConstructWithManifest() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockDatabricksClient);
    when(mockDatabricksClient.getResultChunks(any(), anyLong()))
        .thenReturn(Collections.emptyList());

    // Prepare test data
    ResultManifest manifest = createTestManifest(2);
    ResultData resultData = createTestResultData(2);

    // Create provider
    RemoteChunkProviderV2 provider =
        new RemoteChunkProviderV2(
            new StatementId(STATEMENT_ID),
            manifest,
            resultData,
            mockSession,
            mockHttpClient,
            MAX_PARALLEL_DOWNLOADS);

    // Verify initialization
    assertEquals(2, provider.getChunkCount());
    assertTrue(provider.hasNextChunk());
    assertFalse(provider.isClosed());
  }

  @Test
  void shouldConstructWithThriftResponse() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockDatabricksClient);
    when(mockDatabricksClient.getResultChunks(any(), anyLong()))
        .thenReturn(Collections.emptyList());
    when(mockStatement.getStatementId()).thenReturn(new StatementId(STATEMENT_ID));

    // Prepare test data
    TFetchResultsResp resultsResp = createTestThriftResponse(2);

    // Create provider
    RemoteChunkProviderV2 provider =
        new RemoteChunkProviderV2(
            mockStatement,
            resultsResp,
            mockSession,
            mockHttpClient,
            MAX_PARALLEL_DOWNLOADS,
            CompressionCodec.NONE);

    // Verify initialization
    assertEquals(2, provider.getChunkCount());
    assertTrue(provider.hasNextChunk());
    assertFalse(provider.isClosed());
  }

  @Test
  void shouldDownloadChunksSequentially() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockDatabricksClient);

    // Prepare test data
    ResultManifest manifest = createTestManifest(3);
    ResultData resultData = createTestResultData(3);
    List<ExternalLink> mockLinks = Collections.singletonList(createTestExternalLink(0));

    when(mockDatabricksClient.getResultChunks(any(), eq(0L))).thenReturn(mockLinks);

    // Create provider
    new RemoteChunkProviderV2(
        new StatementId(STATEMENT_ID),
        manifest,
        resultData,
        mockSession,
        mockHttpClient,
        MAX_PARALLEL_DOWNLOADS);

    // Verify downloads were initiated
    verify(mockSession.getDatabricksClient(), times(1)).getResultChunks(any(), eq(0L));
  }

  @Test
  void shouldManageChunkLifecycle() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockDatabricksClient);
    when(mockDatabricksClient.getResultChunks(any(), anyLong()))
        .thenReturn(Collections.emptyList());

    // Prepare test data
    ResultManifest manifest = createTestManifest(1);
    ResultData resultData = createTestResultData(1);

    // Mock HTTP client behavior - throw error during download
    doAnswer(
            invocation -> {
              FutureCallback<byte[]> callback = invocation.getArgument(2);
              callback.failed(new IOException("Test error"));
              return null;
            })
        .when(mockHttpClient)
        .executeAsync(any(), any(), any());

    // Create provider
    RemoteChunkProviderV2 provider =
        new RemoteChunkProviderV2(
            new StatementId(STATEMENT_ID),
            manifest,
            resultData,
            mockSession,
            mockHttpClient,
            MAX_PARALLEL_DOWNLOADS);

    // Move to first chunk
    assertTrue(provider.next());

    // Verify error when retrieving chunk
    assertThrows(DatabricksSQLException.class, provider::getChunk, "Failed to ready chunk");
  }

  @Test
  void shouldHandleDownloadErrors() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockDatabricksClient);

    // Prepare test data with simulated error
    ResultManifest manifest = createTestManifest(1);
    ResultData resultData = createTestResultData(1);
    DatabricksSQLException testException = new DatabricksSQLException("Test error");

    when(mockDatabricksClient.getResultChunks(any(), anyLong())).thenThrow(testException);

    // Create provider and verify error
    assertThrows(
        DatabricksSQLException.class,
        () -> {
          RemoteChunkProviderV2 provider =
              new RemoteChunkProviderV2(
                  new StatementId(STATEMENT_ID),
                  manifest,
                  resultData,
                  mockSession,
                  mockHttpClient,
                  MAX_PARALLEL_DOWNLOADS);
          provider.downloadNextChunks();
        });
  }

  @Test
  void shouldCleanupOnClose() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockDatabricksClient);
    when(mockDatabricksClient.getResultChunks(any(), anyLong()))
        .thenReturn(Collections.emptyList());

    // Prepare test data
    ResultManifest manifest = createTestManifest(2);
    ResultData resultData = createTestResultData(2);

    // Create provider
    RemoteChunkProviderV2 provider =
        new RemoteChunkProviderV2(
            new StatementId(STATEMENT_ID),
            manifest,
            resultData,
            mockSession,
            mockHttpClient,
            MAX_PARALLEL_DOWNLOADS);

    // Close provider
    provider.close();

    // Verify state
    assertTrue(provider.isClosed());
  }

  @Test
  void shouldRespectMaxParallelDownloads() throws DatabricksSQLException {
    when(mockSession.getDatabricksClient()).thenReturn(mockDatabricksClient);
    when(mockDatabricksClient.getResultChunks(any(), anyLong()))
        .thenReturn(Collections.emptyList());

    // Prepare test data
    int maxParallel = 2;
    ResultManifest manifest = createTestManifest(4); // More chunks than max parallel
    ResultData resultData = createTestResultData(4);

    // Create provider
    RemoteChunkProviderV2 provider =
        new RemoteChunkProviderV2(
            new StatementId(STATEMENT_ID),
            manifest,
            resultData,
            mockSession,
            mockHttpClient,
            maxParallel);

    // Verify initial state
    assertEquals(maxParallel, provider.getAllowedChunksInMemory());
  }

  @Test
  void shouldHandleEmptyResults() throws DatabricksSQLException {
    // Prepare test data with no chunks
    ResultManifest manifest = createTestManifest(0);
    ResultData resultData = createTestResultData(0);

    // Create provider
    RemoteChunkProviderV2 provider =
        new RemoteChunkProviderV2(
            new StatementId(STATEMENT_ID),
            manifest,
            resultData,
            mockSession,
            mockHttpClient,
            MAX_PARALLEL_DOWNLOADS);

    // Verify state
    assertEquals(0, provider.getChunkCount());
    assertFalse(provider.hasNextChunk());
    assertEquals(0, provider.getRowCount());
  }

  // Test utility methods
  private ResultManifest createTestManifest(int chunkCount) {
    List<BaseChunkInfo> chunks = new ArrayList<>();
    for (int i = 0; i < chunkCount; i++) {
      chunks.add(createTestChunkInfo(i));
    }

    ResultManifest manifest = new ResultManifest();
    manifest.setChunks(chunks);
    manifest.setTotalChunkCount((long) chunkCount);
    manifest.setTotalRowCount(chunkCount * 100L);
    manifest.setResultCompression(CompressionCodec.NONE);
    return manifest;
  }

  private BaseChunkInfo createTestChunkInfo(long index) {
    BaseChunkInfo info = new BaseChunkInfo();
    info.setChunkIndex(index);
    info.setRowCount(100L);
    info.setRowOffset(index * 100L);
    return info;
  }

  private ResultData createTestResultData(int chunkCount) {
    List<ExternalLink> links = new ArrayList<>();
    for (int i = 0; i < chunkCount; i++) {
      links.add(createTestExternalLink(i));
    }

    ResultData data = new ResultData();
    data.setExternalLinks(links);
    return data;
  }

  private ExternalLink createTestExternalLink(long index) {
    ExternalLink link = new ExternalLink();
    link.setChunkIndex(index);
    link.setExpiration(EXPIRY_TIME);
    link.setExternalLink("https://test.databricks.com/chunks/" + index);
    return link;
  }

  private TFetchResultsResp createTestThriftResponse(int linkCount) {
    TFetchResultsResp resp = new TFetchResultsResp();
    TRowSet rowSet = new TRowSet();

    List<TSparkArrowResultLink> links = new ArrayList<>();
    for (int i = 0; i < linkCount; i++) {
      TSparkArrowResultLink link = new TSparkArrowResultLink();
      link.setStartRowOffset(i * 100L);
      link.setRowCount(100);
      link.setExpiryTime(Instant.parse(EXPIRY_TIME).toEpochMilli());
      links.add(link);
    }

    rowSet.setResultLinks(links);
    resp.setResults(rowSet);
    resp.setHasMoreRows(false);
    return resp;
  }
}
