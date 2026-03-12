package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import java.time.Instant;
import java.util.Locale;
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

/** Tests around when DOWNLOAD_SUCCEEDED is set and error handling during body read. */
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
