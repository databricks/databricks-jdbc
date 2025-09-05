package com.databricks.jdbc.api.impl.arrow.incubator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.arrow.ChunkLinkDownloadService;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TRowSet;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowResultLink;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RemoteChunkProviderV2Test {

  private static final String STATEMENT_ID = "test-statement-123";
  private static final int MAX_PARALLEL_DOWNLOADS = 3;
  private static final String EXPIRY_TIME = "2025-01-08T00:00:00Z";
  private static final int CHUNK_READY_TIMEOUT_SECONDS = 60;
  @Mock private IDatabricksSession mockSession;
  @Mock private IDatabricksHttpClient mockHttpClient;
  @Mock private IDatabricksStatementInternal mockStatement;
  @Mock private IDatabricksConnectionContext mockConnectionContext;

  /** Creates a minimal valid Arrow stream as byte array for testing */
  public static byte[] createValidArrowData() {
    try (BufferAllocator allocator = new RootAllocator();
        ByteArrayOutputStream out = new ByteArrayOutputStream()) {

      // Create a simple schema with one string column
      Field field = new Field("test_column", FieldType.nullable(new ArrowType.Utf8()), null);
      Schema schema = new Schema(java.util.Arrays.asList(field));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {

        writer.start();

        // Add one batch with some test data
        VarCharVector vector = (VarCharVector) root.getVector("test_column");
        vector.allocateNew();
        vector.setSafe(0, "test_value".getBytes());
        vector.setValueCount(1);
        root.setRowCount(1);

        writer.writeBatch();
        writer.end();
      }

      return out.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test Arrow data", e);
    }
  }

  @BeforeEach
  void setUp() {
    when(mockSession.getConnectionContext()).thenReturn(mockConnectionContext);
    when(mockConnectionContext.getChunkReadyTimeoutSeconds())
        .thenReturn(CHUNK_READY_TIMEOUT_SECONDS);
  }

  @Test
  void shouldConstructWithManifest() throws Exception {
    // Prepare test data
    ResultManifest manifest = createTestManifest(2);
    ResultData resultData = createTestResultData(2);

    // Mock ChunkLinkDownloadService construction and behavior
    try (MockedConstruction<ChunkLinkDownloadService> mockedConstruction =
        mockConstruction(
            ChunkLinkDownloadService.class,
            (mock, context) -> {
              // Mock the getLinkForChunk method that gets called during downloadNextChunks
              when(mock.getLinkForChunk(anyLong()))
                  .thenReturn(CompletableFuture.completedFuture(createTestExternalLink(0)));
            })) {

      // Mock HTTP client to simulate successful downloads during initialization
      doAnswer(
              invocation -> {
                FutureCallback<byte[]> callback = invocation.getArgument(2);
                callback.completed(createValidArrowData()); // Simulate successful download
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

      // Verify initialization
      assertEquals(2, provider.getChunkCount());
      assertEquals(200L, provider.getRowCount());
      assertTrue(provider.hasNextChunk());
      assertFalse(provider.isClosed());
      assertEquals(CompressionCodec.NONE, provider.getCompressionCodec());
      assertEquals(Math.min(MAX_PARALLEL_DOWNLOADS, 2), provider.getAllowedChunksInMemory());

      // Verify ChunkLinkDownloadService was created
      assertEquals(1, mockedConstruction.constructed().size());
    }
  }

  @Test
  void shouldConstructWithThriftResponse() throws Exception {
    // Prepare test data
    TFetchResultsResp resultsResp = createTestThriftResponse(2);

    // Mock ChunkLinkDownloadService construction and behavior
    try (MockedConstruction<ChunkLinkDownloadService> mockedConstruction =
        mockConstruction(
            ChunkLinkDownloadService.class,
            (mock, context) -> {
              // Mock the getLinkForChunk method for any chunks that might need links
              when(mock.getLinkForChunk(anyLong()))
                  .thenReturn(CompletableFuture.completedFuture(createTestExternalLink(0)));
            })) {

      // Mock HTTP client to simulate successful downloads during initialization
      doAnswer(
              invocation -> {
                FutureCallback<byte[]> callback = invocation.getArgument(2);
                callback.completed(createValidArrowData()); // Simulate successful download
                return null;
              })
          .when(mockHttpClient)
          .executeAsync(any(), any(), any());

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
      assertEquals(200L, provider.getRowCount());
      assertTrue(provider.hasNextChunk());
      assertFalse(provider.isClosed());
      assertEquals(CompressionCodec.NONE, provider.getCompressionCodec());

      // Verify ChunkLinkDownloadService was created
      assertEquals(1, mockedConstruction.constructed().size());
    }
  }

  @Test
  void shouldHandleChunkLinkDownloadFailure() {
    // Prepare test data
    ResultManifest manifest = createTestManifest(1);

    // Mock ChunkLinkDownloadService to throw exception
    try (MockedConstruction<ChunkLinkDownloadService> mockedConstruction =
        mockConstruction(
            ChunkLinkDownloadService.class,
            (mock, context) -> {
              CompletableFuture<ExternalLink> failedFuture = new CompletableFuture<>();
              failedFuture.completeExceptionally(
                  new DatabricksSQLException(
                      "Link download failed", DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR));
              when(mock.getLinkForChunk(anyLong())).thenReturn(failedFuture);
            })) {

      // Create provider with invalid links to trigger link download
      ResultData emptyResultData = new ResultData();
      emptyResultData.setExternalLinks(Collections.emptyList());

      // Should throw exception during download
      DatabricksSQLException exception =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  new RemoteChunkProviderV2(
                      new StatementId(STATEMENT_ID),
                      manifest,
                      emptyResultData,
                      mockSession,
                      mockHttpClient,
                      MAX_PARALLEL_DOWNLOADS));

      assertEquals(
          DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.toString(), exception.getSQLState());
      assertTrue(exception.getMessage().contains("Chunk link download failed"));

      // Verify ChunkLinkDownloadService was created
      assertEquals(1, mockedConstruction.constructed().size());
    }
  }

  @Test
  void shouldHandleInterruptedLinkDownload() {
    // Prepare test data
    ResultManifest manifest = createTestManifest(1);

    // Mock ChunkLinkDownloadService to simulate interruption
    try (MockedConstruction<ChunkLinkDownloadService> mockedConstruction =
        mockConstruction(
            ChunkLinkDownloadService.class,
            (mock, context) ->
                when(mock.getLinkForChunk(anyLong()))
                    .thenThrow(new InterruptedException("Thread interrupted")))) {

      // Create provider with invalid links to trigger link download
      ResultData emptyResultData = new ResultData();
      emptyResultData.setExternalLinks(Collections.emptyList());

      // Should throw exception during download
      DatabricksSQLException exception =
          assertThrows(
              DatabricksSQLException.class,
              () ->
                  new RemoteChunkProviderV2(
                      new StatementId(STATEMENT_ID),
                      manifest,
                      emptyResultData,
                      mockSession,
                      mockHttpClient,
                      MAX_PARALLEL_DOWNLOADS));

      assertEquals(
          DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR.toString(), exception.getSQLState());
      assertTrue(exception.getMessage().contains("Chunk link download interrupted"));

      // Verify ChunkLinkDownloadService was created
      assertEquals(1, mockedConstruction.constructed().size());
    }
  }

  @Test
  void shouldStopDownloadWhenClosed() throws Exception {
    // Prepare test data
    ResultManifest manifest = createTestManifest(5);
    ResultData resultData = createTestResultData(5);

    // Mock ChunkLinkDownloadService construction and behavior
    try (MockedConstruction<ChunkLinkDownloadService> mockedConstruction =
        mockConstruction(
            ChunkLinkDownloadService.class,
            (mock, context) -> {
              // Mock the getLinkForChunk method that gets called during downloadNextChunks
              when(mock.getLinkForChunk(anyLong()))
                  .thenReturn(CompletableFuture.completedFuture(createTestExternalLink(0)));
            })) {

      // Mock HTTP client to simulate successful downloads during initialization
      doAnswer(
              invocation -> {
                FutureCallback<byte[]> callback = invocation.getArgument(2);
                callback.completed(createValidArrowData()); // Simulate successful download
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

      // Close the provider
      provider.close();

      // Try to download - should exit early due to closed state
      provider.downloadNextChunks();

      assertTrue(provider.isClosed());

      // Verify shutdown was called on link download service
      ChunkLinkDownloadService<?> linkService = mockedConstruction.constructed().get(0);
      verify(linkService).shutdown();
    }
  }

  @Test
  void shouldRespectMaxChunksInMemoryLimit() throws Exception {
    // Prepare test data with more chunks than allowed in memory
    int maxParallel = 2;
    ResultManifest manifest = createTestManifest(5);
    ResultData resultData = createTestResultData(5);

    try (MockedConstruction<ChunkLinkDownloadService> mockedConstruction =
        mockConstruction(
            ChunkLinkDownloadService.class,
            (mock, context) -> {
              // Mock successful link retrieval for initialization
              when(mock.getLinkForChunk(anyLong()))
                  .thenReturn(CompletableFuture.completedFuture(createTestExternalLink(0)));
            })) {

      // Mock HTTP client for initialization downloads
      doAnswer(
              invocation -> {
                FutureCallback<byte[]> callback = invocation.getArgument(2);
                callback.completed(createValidArrowData());
                return null;
              })
          .when(mockHttpClient)
          .executeAsync(any(), any(), any());

      RemoteChunkProviderV2 provider =
          new RemoteChunkProviderV2(
              new StatementId(STATEMENT_ID),
              manifest,
              resultData,
              mockSession,
              mockHttpClient,
              maxParallel);

      // Verify that allowed chunks in memory respects the limit
      assertEquals(maxParallel, provider.getAllowedChunksInMemory());
    }
  }

  @Test
  void shouldHandleChunkNavigation() throws Exception {
    // Prepare test data
    ResultManifest manifest = createTestManifest(3);
    ResultData resultData = createTestResultData(3);

    try (MockedConstruction<ChunkLinkDownloadService> mockedConstruction =
        mockConstruction(
            ChunkLinkDownloadService.class,
            (mock, context) ->
                when(mock.getLinkForChunk(anyLong()))
                    .thenReturn(CompletableFuture.completedFuture(createTestExternalLink(0))))) {

      // Mock HTTP client to simulate successful downloads during initialization
      doAnswer(
              invocation -> {
                FutureCallback<byte[]> callback = invocation.getArgument(2);
                callback.completed(createValidArrowData()); // Simulate successful download
                return null;
              })
          .when(mockHttpClient)
          .executeAsync(any(), any(), any());

      RemoteChunkProviderV2 provider =
          new RemoteChunkProviderV2(
              new StatementId(STATEMENT_ID),
              manifest,
              resultData,
              mockSession,
              mockHttpClient,
              MAX_PARALLEL_DOWNLOADS);

      // Test navigation
      assertTrue(provider.hasNextChunk());
      assertTrue(provider.next()); // Move to chunk 0
      assertTrue(provider.hasNextChunk());
      assertTrue(provider.next()); // Move to chunk 1
      assertTrue(provider.hasNextChunk());
      assertTrue(provider.next()); // Move to chunk 2
      assertFalse(provider.hasNextChunk()); // No more chunks
    }
  }

  @Test
  void shouldHandleEmptyResults() throws Exception {
    // Prepare test data with no chunks
    ResultManifest manifest = createTestManifest(0);
    ResultData resultData = createTestResultData(0);

    try (MockedConstruction<ChunkLinkDownloadService> mockedConstruction =
        mockConstruction(
            ChunkLinkDownloadService.class,
            (mock, context) -> {
              // For empty results, no link downloads should be needed
              when(mock.getLinkForChunk(anyLong()))
                  .thenReturn(CompletableFuture.completedFuture(createTestExternalLink(0)));
            })) {

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
      assertEquals(0, provider.getRowCount());
      assertFalse(provider.hasNextChunk());
      assertEquals(0, provider.getAllowedChunksInMemory());
    }
  }

  @Test
  void shouldCreateChunksCorrectly() throws Exception {
    // Test the chunk creation methods using reflection
    ResultManifest manifest = createTestManifest(2);
    ResultData resultData = createTestResultData(2);

    try (MockedConstruction<ChunkLinkDownloadService> mockedConstruction =
        mockConstruction(
            ChunkLinkDownloadService.class,
            (mock, context) -> {
              // Mock successful link retrieval for initialization
              when(mock.getLinkForChunk(anyLong()))
                  .thenReturn(CompletableFuture.completedFuture(createTestExternalLink(0)));
            })) {

      // Mock HTTP client for initialization
      doAnswer(
              invocation -> {
                FutureCallback<byte[]> callback = invocation.getArgument(2);
                callback.completed(createValidArrowData());
                return null;
              })
          .when(mockHttpClient)
          .executeAsync(any(), any(), any());

      RemoteChunkProviderV2 provider =
          new RemoteChunkProviderV2(
              new StatementId(STATEMENT_ID),
              manifest,
              resultData,
              mockSession,
              mockHttpClient,
              MAX_PARALLEL_DOWNLOADS);

      // Use reflection to test chunk creation methods
      java.lang.reflect.Method createChunkMethod1 =
          RemoteChunkProviderV2.class.getDeclaredMethod(
              "createChunk", StatementId.class, long.class, BaseChunkInfo.class);
      createChunkMethod1.setAccessible(true);

      ArrowResultChunkV2 chunk1 =
          (ArrowResultChunkV2)
              createChunkMethod1.invoke(
                  provider, new StatementId(STATEMENT_ID), 0L, createTestChunkInfo(0));

      assertNotNull(chunk1);
      assertEquals(0L, chunk1.getChunkIndex());
    }
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
