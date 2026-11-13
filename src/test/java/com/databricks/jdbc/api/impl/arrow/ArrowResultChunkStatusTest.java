package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.telemetry.latency.TelemetryCollectorManager;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.http.Header;
import org.apache.http.HeaderIterator;
import org.apache.http.HttpEntity;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.params.HttpParams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests around when DOWNLOAD_SUCCEEDED is set and error handling during streamed body read. */
public class ArrowResultChunkStatusTest {

  @AfterEach
  void clearTelemetry() {
    TelemetryCollectorManager.getInstance().clear();
  }

  @Test
  void setsDownloadSucceededOnlyAfterBodyRead_successfulFlow() throws Exception {
    byte[] payload = minimalArrowStream();
    ArrowResultChunk chunk = newChunk();
    IDatabricksHttpClient http = httpWithEntity(new ByteArrayInputStream(payload), payload.length);

    // Act
    chunk.downloadData(http, CompressionCodec.NONE, 0.0);

    // Assert final state is PROCESSING_SUCCEEDED (download succeeded then processed)
    assertEquals(ChunkStatus.PROCESSING_SUCCEEDED, chunk.getStatus());
  }

  @Test
  void readError_setsDownloadFailed_notDownloadSucceeded() {
    byte[] payload = "ignored".getBytes();
    ArrowResultChunk chunk = newChunk();
    InputStream erroring = new ErrorInputStream(new ByteArrayInputStream(payload));
    IDatabricksHttpClient http = httpWithEntity(erroring, payload.length);

    // Act + Assert: downloadData should throw parsing exception and status should be
    // DOWNLOAD_FAILED
    assertThrows(
        DatabricksParsingException.class,
        () -> chunk.downloadData(http, CompressionCodec.NONE, 0.0));
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, chunk.getStatus());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void lz4Body_isStreamedIntoArrowAndClosed(boolean knownLength) throws Exception {
    byte[] payload = lz4(arrowStream(3, 100));
    ArrowResultChunk chunk = newChunk();
    TrackingInputStream body = new TrackingInputStream(payload, Integer.MAX_VALUE);

    chunk.downloadData(
        httpWithEntity(body, knownLength ? payload.length : -1), CompressionCodec.LZ4_FRAME, 0.0);

    assertEquals(ChunkStatus.PROCESSING_SUCCEEDED, chunk.getStatus());
    assertEquals(300, rowCount(chunk));
    // Closing the body lets the HTTP client drain it and reuse the connection.
    assertTrue(body.closed);
    assertTrue(chunk.releaseChunk());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void midBodyFailure_isRetryableAndFreesPartialBatches(boolean unchecked) throws Exception {
    byte[] payload = lz4(arrowStream(4, 20_000));
    ArrowResultChunk chunk = newChunk();

    // Fail after most of the body, i.e. after some batches were already parsed.
    TrackingInputStream body = new TrackingInputStream(payload, payload.length * 3 / 4);
    body.unchecked = unchecked;
    IDatabricksHttpClient failing = httpWithEntity(body, payload.length);
    assertThrows(
        DatabricksParsingException.class,
        () -> chunk.downloadData(failing, CompressionCodec.LZ4_FRAME, 0.0));
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, chunk.getStatus());
    assertEquals(0, chunk.rootAllocator.getAllocatedMemory());

    // Retry the way ChunkDownloadTask does.
    chunk.setStatus(ChunkStatus.DOWNLOAD_RETRY);
    chunk.downloadData(
        httpWithEntity(new TrackingInputStream(payload, Integer.MAX_VALUE), payload.length),
        CompressionCodec.LZ4_FRAME,
        0.0);
    assertEquals(ChunkStatus.PROCESSING_SUCCEEDED, chunk.getStatus());
    assertEquals(80_000, rowCount(chunk));
    assertTrue(chunk.releaseChunk());
  }

  @Test
  void interruptedRead_isNotReportedAsSucceeded() throws Exception {
    byte[] payload = lz4(arrowStream(4, 20_000));
    ArrowResultChunk chunk = newChunk();
    IDatabricksHttpClient http =
        httpWithEntity(new TrackingInputStream(payload, Integer.MAX_VALUE), payload.length);

    // Arrow reads through an interruptible channel, which fails with ClosedByInterruptException.
    Thread.currentThread().interrupt();
    try {
      // A cancellation surfaces as InterruptedIOException, not a reported parsing failure.
      assertThrows(
          InterruptedIOException.class,
          () -> chunk.downloadData(http, CompressionCodec.LZ4_FRAME, 0.0));
    } finally {
      Thread.interrupted();
    }
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, chunk.getStatus());
    assertNull(chunk.errorMessage);
    assertEquals(0, chunk.rootAllocator.getAllocatedMemory());
  }

  @Test
  void socketTimeout_remainsRetryableFailure() {
    ArrowResultChunk chunk = newChunk();
    // SocketTimeoutException is an InterruptedIOException but must not be treated as cancellation.
    InputStream timingOut =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw new SocketTimeoutException("Read timed out");
          }
        };
    IDatabricksHttpClient http = httpWithEntity(timingOut, 100);

    DatabricksParsingException e =
        assertThrows(
            DatabricksParsingException.class,
            () -> chunk.downloadData(http, CompressionCodec.LZ4_FRAME, 0.0));
    assertInstanceOf(SocketTimeoutException.class, e.getCause());
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, chunk.getStatus());
  }

  @Test
  void releaseWhileBodyDrainStalls_freesBatches() throws Exception {
    byte[] payload = lz4(arrowStream(4, 20_000));
    ArrowResultChunk chunk = newChunk();
    CountDownLatch closing = new CountDownLatch(1);
    CountDownLatch released = new CountDownLatch(1);
    // The final drain blocks until the chunk has been released by another thread.
    InputStream body =
        new FilterInputStream(new ByteArrayInputStream(payload)) {
          @Override
          public void close() throws IOException {
            closing.countDown();
            try {
              released.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            super.close();
          }
        };
    IDatabricksHttpClient http = httpWithEntity(body, payload.length);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread worker =
        new Thread(
            () -> {
              try {
                chunk.downloadData(http, CompressionCodec.LZ4_FRAME, 0.0);
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();
    assertTrue(closing.await(10, TimeUnit.SECONDS));

    assertTrue(chunk.releaseChunk());
    released.countDown();
    worker.join(10_000);
    assertFalse(worker.isAlive());

    assertNull(failure.get());
    assertEquals(ChunkStatus.CHUNK_RELEASED, chunk.getStatus());
    assertEquals(0, chunk.rootAllocator.getAllocatedMemory());
  }

  @Test
  void interruptFromAnotherThread_doesNotCloseBodyOnInterruptingThread() throws Exception {
    byte[] payload = lz4(arrowStream(4, 20_000));
    ArrowResultChunk chunk = newChunk();
    CountDownLatch blocked = new CountDownLatch(1);
    AtomicBoolean release = new AtomicBoolean();
    AtomicReference<Thread> closedBy = new AtomicReference<>();
    // Blocks mid-body like a socket read: ignores interrupts until released.
    InputStream body =
        new FilterInputStream(new ByteArrayInputStream(payload)) {
          private int read;

          @Override
          public int read(byte[] b, int off, int len) throws IOException {
            if (read > payload.length / 2) {
              blocked.countDown();
              while (!release.get()) {
                LockSupport.parkNanos(1_000_000);
              }
            }
            int n = super.read(b, off, len);
            read += Math.max(n, 0);
            return n;
          }

          @Override
          public void close() throws IOException {
            closedBy.compareAndSet(null, Thread.currentThread());
            super.close();
          }
        };
    IDatabricksHttpClient http = httpWithEntity(body, payload.length);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread worker =
        new Thread(
            () -> {
              try {
                chunk.downloadData(http, CompressionCodec.LZ4_FRAME, 0.0);
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();
    assertTrue(blocked.await(10, TimeUnit.SECONDS));

    worker.interrupt();
    assertNotEquals(Thread.currentThread(), closedBy.get());
    release.set(true);
    worker.join(10_000);
    assertFalse(worker.isAlive());

    assertInstanceOf(InterruptedIOException.class, failure.get());
    assertNull(chunk.errorMessage);
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, chunk.getStatus());
    assertEquals(0, chunk.rootAllocator.getAllocatedMemory());
  }

  @Test
  void corruptBody_isRetryableDownloadFailure() {
    byte[] payload = "not an lz4 frame".getBytes();
    ArrowResultChunk chunk = newChunk();
    IDatabricksHttpClient http = httpWithEntity(new ByteArrayInputStream(payload), payload.length);

    assertThrows(
        DatabricksParsingException.class,
        () -> chunk.downloadData(http, CompressionCodec.LZ4_FRAME, 0.0));
    assertEquals(ChunkStatus.DOWNLOAD_FAILED, chunk.getStatus());
  }

  private static long rowCount(ArrowResultChunk chunk) {
    long rows = 0;
    for (List<ValueVector> batch : chunk.getRecordBatchList()) {
      rows += batch.get(0).getValueCount();
    }
    return rows;
  }

  private static ArrowResultChunk newChunk() {
    StatementId statementId = new StatementId("stmt-status-test");
    ArrowResultChunk chunk;
    try {
      chunk =
          ArrowResultChunk.builder()
              .withStatementId(statementId)
              .withChunkStatus(ChunkStatus.PENDING)
              .build();
    } catch (DatabricksParsingException e) {
      throw new RuntimeException(e);
    }
    ExternalLink link =
        new ExternalLink()
            .setExternalLink("https://example.com/chunk")
            .setExpiration(Instant.now().plusSeconds(600).toString());
    chunk.setChunkLink(link);
    return chunk;
  }

  private static IDatabricksHttpClient httpWithEntity(InputStream content, long length) {
    return new IDatabricksHttpClient() {
      @Override
      public CloseableHttpResponse execute(org.apache.http.client.methods.HttpUriRequest request)
          throws DatabricksHttpException {
        return response(content, length);
      }

      @Override
      public CloseableHttpResponse execute(
          org.apache.http.client.methods.HttpUriRequest request, boolean supportGzipEncoding)
          throws DatabricksHttpException {
        return response(content, length);
      }

      @Override
      public <T> java.util.concurrent.Future<T> executeAsync(
          org.apache.hc.core5.http.nio.AsyncRequestProducer requestProducer,
          org.apache.hc.core5.http.nio.AsyncResponseConsumer<T> responseConsumer,
          org.apache.hc.core5.concurrent.FutureCallback<T> callback) {
        throw new UnsupportedOperationException("Not used");
      }
    };
  }

  private static CloseableHttpResponse response(InputStream content, long length) {
    HttpEntity entity = new InputStreamEntity(content, length);
    return new CloseableHttpResponse() {
      @Override
      public void close() {}

      @Override
      public StatusLine getStatusLine() {
        return new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 200, "OK");
      }

      @Override
      public void setStatusLine(StatusLine statusline) {}

      @Override
      public void setStatusLine(ProtocolVersion ver, int code) {}

      @Override
      public void setStatusLine(ProtocolVersion ver, int code, String reason) {}

      @Override
      public void setStatusCode(int code) throws IllegalStateException {}

      @Override
      public void setReasonPhrase(String reason) throws IllegalStateException {}

      @Override
      public HttpEntity getEntity() {
        return entity;
      }

      @Override
      public void setEntity(HttpEntity entity) {}

      @Override
      public Locale getLocale() {
        return java.util.Locale.ROOT;
      }

      @Override
      public void setLocale(Locale loc) {}

      @Override
      public ProtocolVersion getProtocolVersion() {
        return new ProtocolVersion("HTTP", 1, 1);
      }

      @Override
      public boolean containsHeader(String name) {
        return false;
      }

      @Override
      public Header[] getHeaders(String name) {
        return new Header[0];
      }

      @Override
      public Header getFirstHeader(String name) {
        return new BasicHeader(name, "");
      }

      @Override
      public Header getLastHeader(String name) {
        return null;
      }

      @Override
      public Header[] getAllHeaders() {
        return new Header[0];
      }

      @Override
      public void addHeader(Header header) {}

      @Override
      public void addHeader(String name, String value) {}

      @Override
      public void setHeader(Header header) {}

      @Override
      public void setHeader(String name, String value) {}

      @Override
      public void setHeaders(Header[] headers) {}

      @Override
      public void removeHeader(Header header) {}

      @Override
      public void removeHeaders(String name) {}

      @Override
      public HeaderIterator headerIterator() {
        return new org.apache.http.message.BasicHeaderIterator(new Header[0], null);
      }

      @Override
      public HeaderIterator headerIterator(String name) {
        return new org.apache.http.message.BasicHeaderIterator(new Header[0], name);
      }

      @Override
      public HttpParams getParams() {
        return null;
      }

      @Override
      public void setParams(HttpParams params) {}
    };
  }

  private static byte[] minimalArrowStream() throws IOException {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    try (IntVector intVector = new IntVector("c1", allocator)) {
      intVector.allocateNew(1);
      intVector.set(0, 1);
      intVector.setValueCount(1);
      try (VectorSchemaRoot root = VectorSchemaRoot.of(intVector)) {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
          writer.start();
          writer.writeBatch();
          writer.end();
        }
        return baos.toByteArray();
      }
    } finally {
      allocator.close();
    }
  }

  private static byte[] arrowStream(int batches, int rowsPerBatch) throws IOException {
    try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        IntVector intVector = new IntVector("c1", allocator);
        VectorSchemaRoot root = VectorSchemaRoot.of(intVector)) {
      java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, baos)) {
        writer.start();
        for (int b = 0; b < batches; b++) {
          intVector.allocateNew(rowsPerBatch);
          for (int i = 0; i < rowsPerBatch; i++) {
            intVector.set(i, b * rowsPerBatch + i);
          }
          root.setRowCount(rowsPerBatch);
          writer.writeBatch();
        }
        writer.end();
      }
      return baos.toByteArray();
    }
  }

  private static byte[] lz4(byte[] raw) throws IOException {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    // Small blocks so a mid-body failure lands after some batches were parsed.
    try (LZ4FrameOutputStream lz4 =
        new LZ4FrameOutputStream(out, LZ4FrameOutputStream.BLOCKSIZE.SIZE_64KB)) {
      lz4.write(raw);
    }
    return out.toByteArray();
  }

  /**
   * Serves {@code data}, throws (checked or {@code unchecked}) once {@code failAt} bytes were read,
   * and records close().
   */
  private static final class TrackingInputStream extends FilterInputStream {
    private final int failAt;
    private int read;
    private boolean closed;
    private boolean unchecked;

    TrackingInputStream(byte[] data, int failAt) {
      super(new ByteArrayInputStream(data));
      this.failAt = failAt;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if ((long) read + len > failAt) {
        if (unchecked) {
          throw new IllegalStateException("Simulated unchecked failure");
        }
        throw new SocketException("Simulated connection reset");
      }
      int n = super.read(b, off, len);
      read += Math.max(n, 0);
      return n;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }

  private static final class ErrorInputStream extends FilterInputStream {
    private boolean thrown = false;

    protected ErrorInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (!thrown) {
        thrown = true;
        throw new IOException("Simulated read error");
      }
      return super.read(b, off, len);
    }

    @Override
    public int read() throws IOException {
      if (!thrown) {
        thrown = true;
        throw new IOException("Simulated read error");
      }
      return super.read();
    }
  }
}
