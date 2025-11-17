package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.telemetry.TelemetryHelper.getStatementIdString;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
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

/**
 * Unit test verifying we measure "download" time as the time to fully read the HTTP response body.
 *
 * <p>Implementation details:
 *
 * <ul>
 *   <li>Returns a fake HTTP response whose InputStream sleeps once (~200 ms) on first read.
 *   <li>Invokes ArrowResultChunk.downloadData(...) to trigger the read.
 *   <li>Asserts the recorded chunk download latency is close to the injected delay.
 * </ul>
 *
 * <p>Note: Compression-specific tests are intentionally skipped; this focuses on network read
 * timing only.
 */
public class ArrowResultChunkLatencyTest {

  private static final long SLEEP_MS = 200L;
  private static final long LOWER_BOUND_MS = 150L; // account for jitter
  private static final long UPPER_BOUND_MS = 5000L; // generous upper bound to avoid flakes

  @AfterEach
  void tearDown() {
    // Clear telemetry between tests
    TelemetryCollector.getInstance().exportAllPendingTelemetryDetails();
  }

  // Compression-specific test intentionally skipped per request.

  @Test
  void measuresNetworkReadTime_whenNoCompression() throws Exception {
    byte[] arrowBytes = minimalArrowStream();
    runLatencyAssertion(arrowBytes, CompressionCodec.NONE);
  }

  @Test
  void measuresNetworkReadTime_smoke() throws Exception {
    runLatencyAssertion(minimalArrowStream(), CompressionCodec.NONE);
  }

  private void runLatencyAssertion(byte[] payload, CompressionCodec codec) throws Exception {
    String stmt = "stmt-latency-test";
    StatementId statementId = new StatementId(stmt);
    ArrowResultChunk chunk =
        ArrowResultChunk.builder()
            .withStatementId(statementId)
            .withChunkStatus(ChunkStatus.PENDING)
            .build();

    ExternalLink link =
        new ExternalLink()
            .setExternalLink("https://example.com/chunk")
            .setExpiration(Instant.now().plusSeconds(600).toString());
    chunk.setChunkLink(link);

    IDatabricksHttpClient http =
        new IDatabricksHttpClient() {
          @Override
          public CloseableHttpResponse execute(
              org.apache.http.client.methods.HttpUriRequest request)
              throws DatabricksHttpException {
            return buildResponseWithSleepingEntity(payload);
          }

          @Override
          public CloseableHttpResponse execute(
              org.apache.http.client.methods.HttpUriRequest request, boolean supportGzipEncoding)
              throws DatabricksHttpException {
            return buildResponseWithSleepingEntity(payload);
          }

          @Override
          public <T> java.util.concurrent.Future<T> executeAsync(
              org.apache.hc.core5.http.nio.AsyncRequestProducer requestProducer,
              org.apache.hc.core5.http.nio.AsyncResponseConsumer<T> responseConsumer,
              org.apache.hc.core5.concurrent.FutureCallback<T> callback) {
            throw new UnsupportedOperationException("Not used in this test");
          }
        };

    // Act
    chunk.downloadData(http, codec, /*speedThreshold*/ 0.0);

    // Assert telemetry within tolerance
    Long recorded =
        TelemetryCollector.getInstance()
            .getOrCreateTelemetryDetails(getStatementIdString(statementId))
            .getChunkDetails()
            .getSumChunksDownloadTimeMillis();
    assertTrue(
        recorded != null && recorded >= LOWER_BOUND_MS && recorded <= UPPER_BOUND_MS,
        "Expected download latency around " + SLEEP_MS + "ms but was " + recorded + "ms");
  }

  private static CloseableHttpResponse buildResponseWithSleepingEntity(byte[] payload) {
    InputStream content = new SleepInputStream(new ByteArrayInputStream(payload), SLEEP_MS);
    HttpEntity entity = new InputStreamEntity(content, payload.length);
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
      intVector.set(0, 123);
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

  // LZ4 compression helper removed since compression test is skipped.

  private static final class SleepInputStream extends FilterInputStream {
    private final long sleepMillis;
    private boolean slept = false;

    protected SleepInputStream(InputStream in, long sleepMillis) {
      super(in);
      this.sleepMillis = sleepMillis;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      maybeSleepOnce();
      return super.read(b, off, len);
    }

    @Override
    public int read() throws IOException {
      maybeSleepOnce();
      return super.read();
    }

    private void maybeSleepOnce() {
      if (!slept) {
        try {
          TimeUnit.MILLISECONDS.sleep(sleepMillis);
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
        slept = true;
      }
    }
  }
}
