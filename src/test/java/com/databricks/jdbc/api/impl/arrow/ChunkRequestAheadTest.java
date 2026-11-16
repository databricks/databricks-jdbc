package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Downloads that request chunks ahead of the in-memory window ({@code CloudFetchRequestAhead}). */
public class ChunkRequestAheadTest {

  @AfterEach
  void clearThreadContext() {
    DatabricksThreadContextHolder.clearAllContext();
  }

  private static final StatementId STATEMENT_ID = new StatementId("request-ahead-test");
  private static final int ROWS = 50;
  private static final String EXPIRATION = Instant.now().plusSeconds(3600).toString();

  // ==================== ChunkAdmissionWindow ====================

  @Test
  void window_admitsPositionsBelowReleasedPlusSize() throws Exception {
    ChunkAdmissionWindow window = new ChunkAdmissionWindow(2, 1);
    assertTrue(window.await(0, 1));
    assertTrue(window.await(1, 1));
    assertFalse(window.await(2, 20));
    window.release();
    assertTrue(window.await(2, 1));
    assertFalse(window.await(3, 1));
  }

  @Test
  void window_releaseWakesUnlimitedWait() throws Exception {
    ChunkAdmissionWindow window = new ChunkAdmissionWindow(1);
    CompletableFuture<Boolean> admitted = new CompletableFuture<>();
    Thread waiter =
        new Thread(
            () -> {
              try {
                admitted.complete(window.await(1, 0));
              } catch (Throwable t) {
                admitted.completeExceptionally(t);
              }
            });
    waiter.start();
    TimeUnit.MILLISECONDS.sleep(100);
    assertFalse(admitted.isDone());
    window.release();
    assertTrue(admitted.get(5, TimeUnit.SECONDS));
  }

  @Test
  void window_interruptedWaitIsCancellation() throws Exception {
    ChunkAdmissionWindow window = new ChunkAdmissionWindow(1);
    AtomicReference<Throwable> thrown = new AtomicReference<>();
    AtomicReference<Boolean> stillInterrupted = new AtomicReference<>();
    Thread waiter =
        new Thread(
            () -> {
              try {
                window.await(1, 0);
              } catch (Throwable t) {
                thrown.set(t);
                stillInterrupted.set(Thread.currentThread().isInterrupted());
              }
            });
    waiter.start();
    TimeUnit.MILLISECONDS.sleep(50);
    waiter.interrupt();
    waiter.join(5_000);
    assertInstanceOf(InterruptedIOException.class, thrown.get());
    assertTrue(stillInterrupted.get());
  }

  // ==================== ArrowResultChunk.downloadData ====================

  @Test
  void download_readsBodyOnlyOnceAdmitted() throws Exception {
    FakeStorage storage = new FakeStorage(0, 0);
    ChunkAdmissionWindow window = new ChunkAdmissionWindow(1);
    ArrowResultChunk chunk = chunk(1);
    chunk.setAdmissionWindow(window, 1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread worker = downloadInBackground(chunk, storage, failure);

    TimeUnit.MILLISECONDS.sleep(200);
    assertEquals(1, storage.requests.get());
    assertEquals(0, storage.bodiesStarted.get());
    assertTrue(worker.isAlive());

    window.release();
    worker.join(5_000);
    assertNull(failure.get());
    assertEquals(ChunkStatus.PROCESSING_SUCCEEDED, chunk.getStatus());
    assertValues(chunk, 1);
    assertEquals(1, storage.requests.get());
    assertEquals(0, storage.closedUnread.get());
    assertTrue(chunk.releaseChunk());
  }

  @Test
  void download_releasesResponseHeldPastLimit_thenRequestsAgain() throws Exception {
    FakeStorage storage = new FakeStorage(0, 0);
    ChunkAdmissionWindow window = new ChunkAdmissionWindow(1, 100);
    ArrowResultChunk chunk = chunk(1);
    chunk.setAdmissionWindow(window, 1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread worker = downloadInBackground(chunk, storage, failure);

    // The unread response is closed once the limit passes; the download keeps waiting without it.
    assertTrue(waitFor(() -> storage.closedUnread.get() == 1));
    assertEquals(0, storage.bodiesStarted.get());
    TimeUnit.MILLISECONDS.sleep(100);
    assertTrue(worker.isAlive());

    window.release();
    worker.join(5_000);
    assertInstanceOf(ChunkAdmissionWindow.ResponseReleasedException.class, failure.get());
    // Not a failure: no error recorded, nothing allocated, ready for another attempt.
    assertNull(chunk.errorMessage);
    assertEquals(ChunkStatus.URL_FETCHED, chunk.getStatus());
    assertEquals(0, chunk.rootAllocator.getAllocatedMemory());

    chunk.downloadData(storage, CompressionCodec.NONE, 0.0);
    assertEquals(ChunkStatus.PROCESSING_SUCCEEDED, chunk.getStatus());
    assertValues(chunk, 1);
    assertEquals(2, storage.requests.get());
    assertTrue(chunk.releaseChunk());
  }

  @Test
  void download_interruptedWhileWaitingForAdmission_isCancellation() throws Exception {
    FakeStorage storage = new FakeStorage(0, 0);
    ChunkAdmissionWindow window = new ChunkAdmissionWindow(1);
    ArrowResultChunk chunk = chunk(1);
    chunk.setAdmissionWindow(window, 1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread worker = downloadInBackground(chunk, storage, failure);
    assertTrue(waitFor(() -> storage.requests.get() == 1));
    TimeUnit.MILLISECONDS.sleep(50);

    worker.interrupt();
    worker.join(5_000);
    assertInstanceOf(InterruptedIOException.class, failure.get());
    assertNull(chunk.errorMessage);
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, chunk.getStatus());
    assertEquals(1, storage.closedUnread.get());
    assertEquals(0, storage.bodiesStarted.get());
  }

  // ==================== Download tasks ====================

  @Test
  void streamingTask_requestsAgainAfterReleasedResponse_withoutRetry() throws Exception {
    ArrowResultChunk chunk = mock(ArrowResultChunk.class);
    when(chunk.getChunkReadyFuture()).thenReturn(new CompletableFuture<>());
    when(chunk.isChunkLinkInvalid()).thenReturn(false, true);
    doThrow(new ChunkAdmissionWindow.ResponseReleasedException(3))
        .doNothing()
        .when(chunk)
        .downloadData(any(), any(), anyDouble());
    LinkRefresher refresher = mock(LinkRefresher.class);

    long start = System.nanoTime();
    new StreamingChunkDownloadTask(chunk, mock(IDatabricksHttpClient.class), null, refresher, 0.1)
        .call();

    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1_000);
    verify(chunk, times(2)).downloadData(any(), any(), anyDouble());
    // The second attempt refreshes a link that expired while waiting, like any first attempt.
    verify(refresher).refreshLink(anyLong(), anyLong());
    verify(chunk, never()).setStatus(ChunkStatus.DOWNLOAD_RETRY);
    assertTrue(chunk.getChunkReadyFuture().isDone());
    assertFalse(chunk.getChunkReadyFuture().isCompletedExceptionally());
  }

  @Test
  void chunkDownloadTask_requestsAgainAfterReleasedResponse_withoutRetry() throws Exception {
    ArrowResultChunk chunk = mock(ArrowResultChunk.class);
    when(chunk.getChunkReadyFuture()).thenReturn(new CompletableFuture<>());
    when(chunk.isChunkLinkInvalid()).thenReturn(false);
    doThrow(new ChunkAdmissionWindow.ResponseReleasedException(3))
        .doNothing()
        .when(chunk)
        .downloadData(any(), any(), anyDouble());
    ChunkDownloadManager manager = mock(ChunkDownloadManager.class);

    long start = System.nanoTime();
    new ChunkDownloadTask(chunk, mock(IDatabricksHttpClient.class), manager, null).call();

    assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1_000);
    verify(chunk, times(2)).downloadData(any(), any(), anyDouble());
    verify(chunk, never()).setStatus(ChunkStatus.DOWNLOAD_RETRY);
    assertFalse(chunk.getChunkReadyFuture().isCompletedExceptionally());
  }

  // ==================== Providers ====================

  private static final int WINDOW = 4;
  private static final int CHUNKS = 24;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void streamingProvider_drain(boolean requestAhead) throws Exception {
    FakeStorage storage = new FakeStorage(40, 10);
    StreamingChunkProvider provider = streamingProvider(storage, requestAhead, WINDOW, CHUNKS);
    try {
      drain(provider, storage, CHUNKS, 0);
    } finally {
      provider.close();
    }
    assertDrainShape(storage, requestAhead);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void remoteProvider_drain(boolean requestAhead) throws Exception {
    FakeStorage storage = new FakeStorage(40, 10);
    RemoteChunkProvider provider = remoteProvider(storage, requestAhead, WINDOW, CHUNKS);
    try {
      drain(provider, storage, CHUNKS, 0);
    } finally {
      provider.close();
    }
    assertDrainShape(storage, requestAhead);
  }

  private static void assertDrainShape(FakeStorage storage, boolean requestAhead) {
    // Bodies in memory never exceed the window, as without requests ahead.
    assertTrue(storage.maxBodiesInMemory.get() <= WINDOW, "bodies " + storage.maxBodiesInMemory);
    assertEquals(CHUNKS, storage.requests.get());
    assertEquals(0, storage.closedUnread.get());
    if (requestAhead) {
      assertTrue(storage.maxRequestLead.get() >= WINDOW, "lead " + storage.maxRequestLead);
      assertTrue(storage.maxRequestLead.get() < 2 * WINDOW, "lead " + storage.maxRequestLead);
    } else {
      assertTrue(storage.maxRequestLead.get() < WINDOW, "lead " + storage.maxRequestLead);
    }
  }

  @Test
  void streamingProvider_slowReader_neverRequestsAhead() throws Exception {
    FakeStorage storage = new FakeStorage(5, 0);
    StreamingChunkProvider provider = streamingProvider(storage, true, WINDOW, 12);
    try {
      TimeUnit.MILLISECONDS.sleep(500);
      drain(provider, storage, 12, 150);
    } finally {
      provider.close();
    }
    assertTrue(storage.maxRequestLead.get() < WINDOW, "lead " + storage.maxRequestLead);
    assertEquals(12, storage.requests.get());
  }

  @Test
  void remoteProvider_slowReader_neverRequestsAhead() throws Exception {
    FakeStorage storage = new FakeStorage(5, 0);
    RemoteChunkProvider provider = remoteProvider(storage, true, WINDOW, 12);
    try {
      TimeUnit.MILLISECONDS.sleep(500);
      drain(provider, storage, 12, 150);
    } finally {
      provider.close();
    }
    assertTrue(storage.maxRequestLead.get() < WINDOW, "lead " + storage.maxRequestLead);
    assertEquals(12, storage.requests.get());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void closeWhileRequestsWaitForAdmission_cancelsQuietly(boolean streaming) throws Exception {
    FakeStorage storage = new FakeStorage(20, 0);
    ChunkProvider provider =
        streaming ? streamingProvider(storage, true, 2, 10) : remoteProvider(storage, true, 2, 10);
    List<LogRecord> warnings = Collections.synchronizedList(new ArrayList<>());
    Logger logger = Logger.getLogger("com.databricks.jdbc");
    Level previousLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    Handler handler = recordWarnings(warnings);
    logger.addHandler(handler);
    try {
      assertTrue(provider.next());
      assertValues((ArrowResultChunk) provider.getChunk(), 0);
      // Chunks 2 and 3 were requested while the reader waited and now wait to be admitted.
      assertTrue(waitFor(() -> storage.requests.get() == 4));
      TimeUnit.MILLISECONDS.sleep(100);
      assertEquals(2, storage.bodiesStarted.get());

      long start = System.nanoTime();
      provider.close();
      assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 2_000);
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
    }
    assertTrue(warnings.isEmpty(), () -> "unexpected log records: " + messages(warnings));
    assertEquals(2, storage.closedUnread.get());
    assertEquals(2, storage.bodiesStarted.get());
  }

  // ==================== Helpers ====================

  /** Reads every chunk in order, checking values, the way ArrowStreamResult drives a provider. */
  private static void drain(
      ChunkProvider provider, FakeStorage storage, int chunks, long readerDelayMs)
      throws Exception {
    int read = 0;
    while (true) {
      if (read > 0) {
        // Count the release before next() performs it, so the storage never sees fewer releases.
        storage.released.incrementAndGet();
      }
      if (!provider.next()) {
        break;
      }
      assertValues((ArrowResultChunk) provider.getChunk(), read);
      read++;
      if (readerDelayMs > 0) {
        TimeUnit.MILLISECONDS.sleep(readerDelayMs);
      }
    }
    assertEquals(chunks, read);
  }

  private static StreamingChunkProvider streamingProvider(
      FakeStorage storage, boolean requestAhead, int window, int chunks) throws Exception {
    List<ExternalLink> links = new ArrayList<>();
    for (int i = 0; i < chunks; i++) {
      links.add(link(i).setNextChunkIndex(i + 1 < chunks ? (long) i + 1 : null));
    }
    return new StreamingChunkProvider(
        mock(ChunkLinkFetcher.class),
        storage,
        CompressionCodec.NONE,
        STATEMENT_ID,
        window,
        128,
        30,
        0.0,
        context(requestAhead),
        ChunkLinkFetchResult.of(links, false, -1, (long) chunks * ROWS));
  }

  private static RemoteChunkProvider remoteProvider(
      FakeStorage storage, boolean requestAhead, int window, int chunks) throws Exception {
    List<BaseChunkInfo> infos = new ArrayList<>();
    List<ExternalLink> links = new ArrayList<>();
    for (int i = 0; i < chunks; i++) {
      infos.add(
          new BaseChunkInfo()
              .setChunkIndex((long) i)
              .setRowCount((long) ROWS)
              .setRowOffset((long) i * ROWS));
      links.add(link(i));
    }
    ResultManifest manifest =
        new ResultManifest()
            .setTotalChunkCount((long) chunks)
            .setTotalRowCount((long) chunks * ROWS)
            .setChunks(infos)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()));
    IDatabricksSession session = mock(IDatabricksSession.class);
    IDatabricksConnectionContext context = context(requestAhead);
    when(context.getChunkReadyTimeoutSeconds()).thenReturn(30);
    when(session.getConnectionContext()).thenReturn(context);
    // Download tasks read the speed threshold from the caller's context (0 here: no slow warnings).
    DatabricksThreadContextHolder.setConnectionContext(context);
    return new RemoteChunkProvider(
        STATEMENT_ID, manifest, new ResultData().setExternalLinks(links), session, storage, window);
  }

  private static IDatabricksConnectionContext context(boolean requestAhead) {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.isCloudFetchRequestAheadEnabled()).thenReturn(requestAhead);
    return context;
  }

  private static ExternalLink link(int index) {
    return new ExternalLink()
        .setExternalLink("http://storage.test/chunk-" + index)
        .setChunkIndex((long) index)
        .setRowCount((long) ROWS)
        .setRowOffset((long) index * ROWS)
        .setHttpHeaders(Collections.emptyMap())
        .setExpiration(EXPIRATION);
  }

  private static ArrowResultChunk chunk(int index) throws Exception {
    ArrowResultChunk chunk =
        ArrowResultChunk.builder()
            .withStatementId(STATEMENT_ID)
            .withChunkMetadata(index, ROWS, (long) index * ROWS)
            .build();
    chunk.setChunkLink(link(index));
    return chunk;
  }

  private static Thread downloadInBackground(
      ArrowResultChunk chunk, FakeStorage storage, AtomicReference<Throwable> failure) {
    Thread worker =
        new Thread(
            () -> {
              try {
                chunk.downloadData(storage, CompressionCodec.NONE, 0.0);
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();
    return worker;
  }

  private static void assertValues(ArrowResultChunk chunk, int index) {
    int row = 0;
    for (List<ValueVector> batch : chunk.getRecordBatchList()) {
      IntVector v = (IntVector) batch.get(0);
      for (int i = 0; i < v.getValueCount(); i++, row++) {
        assertEquals(index * 1000 + row, v.get(i), "chunk " + index + " row " + row);
      }
    }
    assertEquals(ROWS, row);
  }

  private static boolean waitFor(java.util.function.BooleanSupplier condition)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      TimeUnit.MILLISECONDS.sleep(5);
    }
    return false;
  }

  private static Handler recordWarnings(List<LogRecord> records) {
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
              records.add(record);
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    handler.setLevel(Level.ALL);
    return handler;
  }

  private static String messages(List<LogRecord> records) {
    StringBuilder sb = new StringBuilder();
    for (LogRecord r : records) {
      sb.append(r.getLevel()).append(' ').append(r.getMessage()).append('\n');
    }
    return sb.toString();
  }

  private static final ConcurrentHashMap<Integer, byte[]> PAYLOADS = new ConcurrentHashMap<>();

  /** One Arrow batch of {@link #ROWS} ints, {@code index * 1000 + row}. */
  private static byte[] payload(int index) {
    return PAYLOADS.computeIfAbsent(
        index,
        i -> {
          try (BufferAllocator allocator = new RootAllocator();
              IntVector v = new IntVector("v", allocator);
              VectorSchemaRoot root = VectorSchemaRoot.of(v)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
              writer.start();
              v.allocateNew(ROWS);
              for (int r = 0; r < ROWS; r++) {
                v.set(r, i * 1000 + r);
              }
              root.setRowCount(ROWS);
              writer.writeBatch();
              writer.end();
            }
            return out.toByteArray();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Storage double: answers each GET after {@code headerDelayMs}, delays the first body read by
   * {@code bodyDelayMs}, and records how far requests run ahead of the reader and how many bodies
   * are in memory. {@link #released} is the reader's release count, kept by the test.
   */
  private static final class FakeStorage implements IDatabricksHttpClient {
    final long headerDelayMs;
    final long bodyDelayMs;
    final AtomicInteger released = new AtomicInteger();
    final AtomicInteger requests = new AtomicInteger();
    final AtomicInteger maxRequestLead = new AtomicInteger(Integer.MIN_VALUE);
    final AtomicInteger bodiesStarted = new AtomicInteger();
    final AtomicInteger maxBodiesInMemory = new AtomicInteger();
    final AtomicInteger closedUnread = new AtomicInteger();

    FakeStorage(long headerDelayMs, long bodyDelayMs) {
      this.headerDelayMs = headerDelayMs;
      this.bodyDelayMs = bodyDelayMs;
    }

    @Override
    public CloseableHttpResponse execute(HttpUriRequest request) {
      return execute(request, false);
    }

    @Override
    public CloseableHttpResponse execute(HttpUriRequest request, boolean supportGzipEncoding) {
      String url = request.getURI().toString();
      int index = Integer.parseInt(url.substring(url.lastIndexOf('-') + 1));
      requests.incrementAndGet();
      maxRequestLead.accumulateAndGet(index - released.get(), Math::max);
      sleep(headerDelayMs);
      byte[] data = payload(index);
      AtomicInteger started = new AtomicInteger();
      FilterInputStream body =
          new FilterInputStream(new ByteArrayInputStream(data)) {
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
              if (started.compareAndSet(0, 1)) {
                int inMemory = bodiesStarted.incrementAndGet() - released.get();
                maxBodiesInMemory.accumulateAndGet(inMemory, Math::max);
                sleep(bodyDelayMs);
              }
              return super.read(b, off, len);
            }

            @Override
            public int read() throws IOException {
              byte[] one = new byte[1];
              return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
            }
          };
      CloseableHttpResponse response = mock(CloseableHttpResponse.class);
      when(response.getStatusLine())
          .thenReturn(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK"));
      when(response.getEntity()).thenReturn(new InputStreamEntity(body, data.length));
      try {
        doAnswer(
                inv -> {
                  if (started.get() == 0) {
                    closedUnread.incrementAndGet();
                  }
                  return null;
                })
            .when(response)
            .close();
      } catch (IOException e) {
        throw new AssertionError(e);
      }
      return response;
    }

    @Override
    public <T> java.util.concurrent.Future<T> executeAsync(
        org.apache.hc.core5.http.nio.AsyncRequestProducer requestProducer,
        org.apache.hc.core5.http.nio.AsyncResponseConsumer<T> responseConsumer,
        org.apache.hc.core5.concurrent.FutureCallback<T> callback) {
      throw new UnsupportedOperationException();
    }

    private static void sleep(long ms) {
      if (ms <= 0) {
        return;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(ms);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
