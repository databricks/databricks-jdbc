package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.core.SqlExecResultFormat;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ApcArrowBatchChunkProviderTest {
  private static final StatementId ID = new StatementId("statement");

  private static ResultManifest manifest(CompressionCodec codec) {
    return new ResultManifest()
        .setFormat(SqlExecResultFormat.ARROW_BATCH)
        .setResultCompression(codec)
        .setTotalChunkCount(0L)
        .setTotalRowCount(0L)
        .setSchema(
            new ResultSchema()
                .setColumnCount(1L)
                .setColumns(
                    List.of(
                        new ColumnInfo()
                            .setName("n")
                            .setPosition(0L)
                            .setTypeName(ColumnInfoTypeName.INT)
                            .setTypeText("INT")
                            .setTypeJson("\"integer\""))));
  }

  private static byte[] stream(int value) throws Exception {
    Schema schema = ArrowBatchSchemaBuilder.build(manifest(CompressionCodec.NONE).getSchema());
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      IntVector vector = (IntVector) root.getVector("n");
      vector.allocateNew(1);
      vector.set(0, value);
      vector.setValueCount(1);
      root.setRowCount(1);
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, bytes)) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
      return bytes.toByteArray();
    }
  }

  private static byte[] bareBatch(int value) throws Exception {
    byte[] stream = stream(value);
    int schemaLength =
        ArrowBatchSchemaBuilder.serialize(manifest(CompressionCodec.NONE).getSchema()).length;
    return Arrays.copyOfRange(stream, schemaLength, stream.length - 8);
  }

  private static ArrowBatch batch(int value, boolean compressed) throws Exception {
    byte[] bytes = bareBatch(value);
    if (compressed) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try (LZ4FrameOutputStream lz4 = new LZ4FrameOutputStream(out)) {
        lz4.write(bytes);
      }
      bytes = out.toByteArray();
    }
    return new ArrowBatch().setBatch(bytes).setRowCount(1L);
  }

  private static IDatabricksSession session(IDatabricksClient client, boolean cloudFetch) {
    IDatabricksSession session = mock(IDatabricksSession.class);
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    lenient().when(session.getConnectionContext()).thenReturn(context);
    lenient().when(session.getDatabricksClient()).thenReturn(client);
    lenient().when(context.isCloudFetchEnabled()).thenReturn(cloudFetch);
    return session;
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void compressedBatchesAcrossPagesUseSumOfRowsAndIgnoreTotals(boolean bounded) throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    IDatabricksSession session = session(client, false);
    lenient().when(session.getConnectionContext().isBoundedSeaApiEnabled()).thenReturn(bounded);
    ResultData first = new ResultData().setArrowBatches(List.of(batch(10, true), batch(20, true)));
    first.setNextChunkIndex(3L);
    first =
        new ObjectMapper()
            .readValue(new ObjectMapper().writeValueAsString(first), ResultData.class);
    ResultData second = new ResultData().setArrowBatches(List.of(batch(30, true)));
    when(client.getResultChunksData(ID, 3, 2)).thenReturn(second);
    try (ResultCloser result =
        new ResultCloser(
            new ArrowStreamResult(
                manifest(CompressionCodec.LZ4_FRAME),
                first,
                ID,
                session,
                mock(IDatabricksHttpClient.class)))) {
      assertEquals(List.of(10, 20, 30), readInts(result.result));
      assertEquals(3, result.result.getRowCount());
    }
    verify(client).getResultChunksData(ID, 3, 2);
  }

  @Test
  void absentInitialResultFetchesFromZeroAndPopulatedSchemaDoesNotChangeDecoding()
      throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    IDatabricksSession session = session(client, false);
    when(client.getResultChunksData(ID, 0, 0))
        .thenReturn(new ResultData().setArrowBatches(List.of(batch(7, false))));
    ResultManifest manifest =
        manifest(CompressionCodec.NONE)
            .setArrowSchema(
                ArrowBatchSchemaBuilder.serialize(manifest(CompressionCodec.NONE).getSchema()))
            .setTotalByteCount(999L);
    manifest =
        new ObjectMapper()
            .readValue(new ObjectMapper().writeValueAsString(manifest), ResultManifest.class);
    assertArrayEquals(
        ArrowBatchSchemaBuilder.serialize(manifest.getSchema()), manifest.getArrowSchema());
    try (ResultCloser result =
        new ResultCloser(
            new ArrowStreamResult(
                manifest, null, ID, session, mock(IDatabricksHttpClient.class)))) {
      assertEquals(List.of(7), readInts(result.result));
    }
    verify(client).getResultChunksData(ID, 0, 0);
  }

  @Test
  void terminalEmptyInitialResultDoesNotFetchChunkZero() throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    IDatabricksSession session = session(client, false);
    try (ResultCloser result =
        new ResultCloser(
            new ArrowStreamResult(
                manifest(CompressionCodec.NONE),
                new ResultData(),
                ID,
                session,
                mock(IDatabricksHttpClient.class)))) {
      assertEquals(List.of(), readInts(result.result));
    }
    verify(client, never()).getResultChunksData(any(), anyLong(), anyLong());
  }

  @Test
  void malformedOptionalArrowSchemaDoesNotOverrideManifest() throws Exception {
    IDatabricksSession session = session(mock(IDatabricksClient.class), false);
    ResultManifest invalid = manifest(CompressionCodec.NONE).setArrowSchema(new byte[] {1, 2});
    try (ResultCloser result =
        new ResultCloser(
            new ArrowStreamResult(
                invalid,
                new ResultData().setArrowBatches(List.of(batch(5, false))),
                ID,
                session,
                mock(IDatabricksHttpClient.class)))) {
      assertEquals(List.of(5), readInts(result.result));
    }
  }

  @Test
  void malformedInlineBatchUsesDriverCode() {
    IDatabricksSession session = session(mock(IDatabricksClient.class), false);
    ResultData page =
        new ResultData()
            .setArrowBatches(
                List.of(
                    new ArrowBatch()
                        .setBatch(
                            java.nio.ByteBuffer.allocate(8)
                                .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                                .putInt(-1)
                                .putInt(1024)
                                .array())
                        .setRowCount(1L)));
    DatabricksSQLException error =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                new ApcArrowBatchChunkProvider(
                    page,
                    manifest(CompressionCodec.NONE),
                    ID,
                    session,
                    mock(IDatabricksHttpClient.class)));
    assertEquals(DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR.name(), error.getSQLState());
    assertEquals(
        DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR.getCode(), error.getErrorCode());
  }

  @Test
  void invalidPresignedUrlDoesNotExposePathOrSignature() throws Exception {
    IDatabricksSession session = session(mock(IDatabricksClient.class), true);
    IDatabricksHttpClient http = mock(IDatabricksHttpClient.class);
    String secret = "private path?Signature=secret-signature";
    ResultData page =
        new ResultData()
            .setExternalLinks(
                List.of(link("https://storage.example.test/" + secret, 0, 0, 1, null)));
    ApcArrowBatchChunkProvider provider =
        new ApcArrowBatchChunkProvider(page, manifest(CompressionCodec.NONE), ID, session, http);
    try {
      DatabricksSQLException error = assertThrows(DatabricksSQLException.class, provider::next);
      assertFalse(error.toString().contains(secret));
      assertFalse(error.toString().contains("secret-signature"));
      verifyNoInteractions(http);
    } finally {
      provider.close();
    }
  }

  @Test
  void httpFailureDoesNotExposePresignedUrlInExceptionChain() throws Exception {
    String url = "https://storage.example.test/private/chunk?Signature=secret-signature";
    IDatabricksHttpClient http = mock(IDatabricksHttpClient.class);
    when(http.execute(any(HttpUriRequest.class), eq(true)))
        .thenThrow(
            new com.databricks.jdbc.exception.DatabricksHttpException(
                "Failed request to " + url, DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR));
    ArrowResultChunk chunk =
        ArrowResultChunk.builder().withStatementId(ID).withChunkMetadata(0, 1, 0).build();
    chunk.setChunkLink(link(url, 0, 0, 1, null));
    try {
      DatabricksSQLException error =
          assertThrows(
              DatabricksSQLException.class,
              () -> chunk.downloadData(http, CompressionCodec.NONE, 0));
      for (Throwable cause = error; cause != null; cause = cause.getCause()) {
        assertFalse(cause.toString().contains("secret-signature"));
        assertFalse(cause.toString().contains("private/chunk"));
      }
    } finally {
      chunk.releaseChunk();
    }
  }

  @Test
  void emptyArrowPageContinuesToNextPage() throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    IDatabricksSession session = session(client, false);
    ResultData empty =
        new ResultData()
            .setArrowBatches(List.of(new ArrowBatch().setBatch(new byte[0]).setRowCount(0L)));
    empty.setNextChunkIndex(1L);
    when(client.getResultChunksData(ID, 1, 0))
        .thenReturn(new ResultData().setArrowBatches(List.of(batch(7, false))));
    try (ResultCloser result =
        new ResultCloser(
            new ArrowStreamResult(
                manifest(CompressionCodec.NONE),
                empty,
                ID,
                session,
                mock(IDatabricksHttpClient.class)))) {
      assertEquals(List.of(7), readInts(result.result));
    }
    verify(client).getResultChunksData(ID, 1, 0);
  }

  @Test
  void stalledExternalDownloadRespectsChunkReadyTimeout() throws Exception {
    IDatabricksSession session = session(mock(IDatabricksClient.class), true);
    when(session.getConnectionContext().getChunkReadyTimeoutSeconds()).thenReturn(1);
    IDatabricksHttpClient http = mock(IDatabricksHttpClient.class);
    when(http.execute(any(HttpUriRequest.class), eq(true)))
        .thenAnswer(
            invocation -> {
              try {
                TimeUnit.SECONDS.sleep(30);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException("download interrupted", e);
              }
              throw new java.io.IOException("download stalled");
            });
    ResultData initial =
        new ResultData()
            .setExternalLinks(
                List.of(
                    link("https://storage.example.test/private/chunk?sig=secret", 0, 0, 1, null)));
    try (ResultCloser result =
        new ResultCloser(
            new ArrowStreamResult(manifest(CompressionCodec.NONE), initial, ID, session, http))) {
      DatabricksSQLException error =
          assertThrows(DatabricksSQLException.class, result.result::next);
      assertEquals(DatabricksDriverErrorCode.CHUNK_READY_ERROR.name(), error.getSQLState());
      assertEquals(DatabricksDriverErrorCode.CHUNK_READY_ERROR.getCode(), error.getErrorCode());
    }
  }

  @Test
  void closeDefersChunkReleaseUntilCancelledDownloadStops() throws Exception {
    IDatabricksSession session = session(mock(IDatabricksClient.class), true);
    when(session.getConnectionContext().getChunkReadyTimeoutSeconds()).thenReturn(1);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    IDatabricksHttpClient http = mock(IDatabricksHttpClient.class);
    when(http.execute(any(HttpUriRequest.class), eq(true)))
        .thenAnswer(
            invocation -> {
              started.countDown();
              boolean released = false;
              boolean interrupted = false;
              while (!released) {
                try {
                  finish.await();
                  released = true;
                } catch (InterruptedException ignored) {
                  // Simulate an HTTP client that completes after cancellation.
                  interrupted = true;
                }
              }
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
              throw new java.io.IOException("late download failure");
            });
    ResultData initial =
        new ResultData()
            .setExternalLinks(
                List.of(
                    link("https://storage.example.test/chunk?Signature=secret", 0, 0, 1, null)));
    ApcArrowBatchChunkProvider provider =
        new ApcArrowBatchChunkProvider(initial, manifest(CompressionCodec.NONE), ID, session, http);
    try {
      assertThrows(DatabricksSQLException.class, provider::next);
      assertTrue(started.await(1, TimeUnit.SECONDS));
      ArrowResultChunk chunk = provider.getChunk();
      provider.close();
      assertFalse(chunk.getStatus() == ChunkStatus.CHUNK_RELEASED);
      finish.countDown();
      for (int i = 0; i < 50 && chunk.getStatus() != ChunkStatus.CHUNK_RELEASED; i++) {
        TimeUnit.MILLISECONDS.sleep(100);
      }
      assertEquals(ChunkStatus.CHUNK_RELEASED, chunk.getStatus());
      assertEquals(0, chunk.rootAllocator.getAllocatedMemory());
    } finally {
      finish.countDown();
      provider.close();
    }
  }

  @Test
  void downloadMetricsDoNotLogPresignedPath() throws Exception {
    String secretPath = "org-secret/cluster-secret/path-token";
    String url = "https://storage.example.test/" + secretPath + "?sig=query-secret";
    IDatabricksSession session = session(mock(IDatabricksClient.class), true);
    IDatabricksHttpClient http = mock(IDatabricksHttpClient.class);
    when(http.execute(any(HttpUriRequest.class), eq(true)))
        .thenAnswer(
            invocation -> {
              HttpUriRequest request = invocation.getArgument(0);
              assertEquals(url, request.getURI().toString());
              TimeUnit.MILLISECONDS.sleep(10);
              CloseableHttpResponse response = mock(CloseableHttpResponse.class);
              when(response.getEntity()).thenReturn(new ByteArrayEntity(stream(4)));
              when(response.getStatusLine())
                  .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
              return response;
            });
    Logger logger = Logger.getLogger(ArrowResultChunk.class.getName());
    Level oldLevel = logger.getLevel();
    List<String> messages = new ArrayList<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            messages.add(record.getMessage());
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    handler.setLevel(Level.FINE);
    logger.setLevel(Level.FINE);
    logger.addHandler(handler);
    try (ResultCloser result =
        new ResultCloser(
            new ArrowStreamResult(
                manifest(CompressionCodec.NONE),
                new ResultData().setExternalLinks(List.of(link(url, 0, 0, 1, null))),
                ID,
                session,
                http))) {
      assertEquals(List.of(4), readInts(result.result));
      assertTrue(messages.stream().anyMatch(message -> message.contains("CloudFetch download:")));
      assertTrue(messages.stream().noneMatch(message -> message.contains(secretPath)));
      assertTrue(messages.stream().noneMatch(message -> message.contains("query-secret")));
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(oldLevel);
    }
  }

  @Test
  void twoExternalLinksContinueFromFinalLinkAndKeepPresignedRequest() throws Exception {
    IDatabricksClient client = mock(IDatabricksClient.class);
    IDatabricksSession session = session(client, true);
    IDatabricksHttpClient http = mock(IDatabricksHttpClient.class);
    String prefix = "https://storage.example.test/chunk";
    ExternalLink first = link(prefix + "/one?sig=a%2Bb", 1, 1, 1, 2);
    ExternalLink second = link(prefix + "/two?sig=c%2Fd", 2, 2, 1, 3);
    ExternalLink third = link(prefix + "/three?sig=e%2Ff", 3, 3, 1, null);
    ResultData initial = new ResultData().setArrowBatches(List.of(batch(0, false)));
    initial.setNextChunkIndex(1L);
    when(client.getResultChunksData(ID, 1, 1))
        .thenReturn(new ResultData().setExternalLinks(List.of(first, second)));
    when(client.getResultChunksData(ID, 3, 3))
        .thenReturn(new ResultData().setExternalLinks(List.of(third)));
    List<String> requested = new ArrayList<>();
    when(http.execute(any(HttpUriRequest.class), eq(true)))
        .thenAnswer(
            invocation -> {
              HttpUriRequest request = invocation.getArgument(0);
              requested.add(request.getURI().toString());
              assertEquals("keep-me", request.getFirstHeader("x-presigned").getValue());
              int value = requested.size();
              CloseableHttpResponse response = mock(CloseableHttpResponse.class);
              HttpEntity entity = new ByteArrayEntity(stream(value));
              when(response.getEntity()).thenReturn(entity);
              when(response.getStatusLine())
                  .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
              return response;
            });
    try (ResultCloser result =
        new ResultCloser(
            new ArrowStreamResult(manifest(CompressionCodec.NONE), initial, ID, session, http))) {
      assertEquals(List.of(0, 1, 2, 3), readInts(result.result));
    }
    verify(client).getResultChunksData(ID, 1, 1);
    verify(client).getResultChunksData(ID, 3, 3);
    assertEquals(
        List.of(first.getExternalLink(), second.getExternalLink(), third.getExternalLink()),
        requested);
  }

  @Test
  void inlineOnlyRejectsExternalPageWithDriverErrorCode() {
    IDatabricksSession session = session(mock(IDatabricksClient.class), false);
    ResultData page =
        new ResultData()
            .setExternalLinks(List.of(link("https://storage.example.test/chunk", 0, 0, 1, null)));
    com.databricks.jdbc.exception.DatabricksParsingException error =
        assertThrows(
            com.databricks.jdbc.exception.DatabricksParsingException.class,
            () ->
                new ApcArrowBatchChunkProvider(
                    page,
                    manifest(CompressionCodec.NONE),
                    ID,
                    session,
                    mock(IDatabricksHttpClient.class)));
    assertEquals(DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR.name(), error.getSQLState());
    assertEquals(
        DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR.getCode(), error.getErrorCode());
  }

  private static ExternalLink link(
      String url, long index, long offset, long count, Integer nextIndex) {
    return new ExternalLink()
        .setExternalLink(url)
        .setHttpHeaders(Map.of("x-presigned", "keep-me"))
        .setExpiration("2099-01-01T00:00:00Z")
        .setChunkIndex(index)
        .setRowOffset(offset)
        .setRowCount(count)
        .setNextChunkIndex(nextIndex == null ? null : nextIndex.longValue());
  }

  private static List<Integer> readInts(ArrowStreamResult result) throws Exception {
    List<Integer> values = new ArrayList<>();
    while (result.next()) {
      values.add((Integer) result.getObject(0));
    }
    return values;
  }

  private static final class ResultCloser implements AutoCloseable {
    private final ArrowStreamResult result;

    private ResultCloser(ArrowStreamResult result) {
      this.result = result;
    }

    @Override
    public void close() {
      result.close();
    }
  }
}
