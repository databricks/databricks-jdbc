package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.DataStreamChannel;

/**
 * Minimal-copy streaming producer for a single-shot PUT/POST with a known {@code Content-Length}.
 * Memory footprint is a single reusable byte[] buffer; data is never duplicated once inside
 * user-space.
 */
public final class InputStreamFixedLenProducer implements AsyncEntityProducer, Closeable {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(InputStreamFixedLenProducer.class);

  /** Default internal buffer (16 KiB). */
  private static final int DEFAULT_BUF = 16 * 1024;

  private final InputStream source;
  private final long contentLength;
  private final byte[] buf;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** Buffer that still has bytes after a partial non-blocking write. */
  private ByteBuffer currentChunk = null;

  private long totalSent = 0;
  private boolean endOfStream = false;
  private final AtomicReference<Exception> failure = new AtomicReference<>();

  /**
   * @param source Input stream to upload. Caller still owns the stream until this producer is
   *     closed or an error occurs.
   * @param contentLength Total number of bytes that will be read from {@code source}.
   * @param bufferSize Size of the reusable transfer buffer.
   */
  public InputStreamFixedLenProducer(InputStream source, long contentLength, int bufferSize) {
    this.source = Objects.requireNonNull(source, "source must not be null");
    if (contentLength < 0) {
      throw new IllegalArgumentException("contentLength must be ≥0");
    }
    this.contentLength = contentLength;
    this.buf = new byte[Math.max(bufferSize, DEFAULT_BUF)];
  }

  public InputStreamFixedLenProducer(InputStream source, long contentLength) {
    this(source, contentLength, DEFAULT_BUF);
  }

  /* -------------------------------------------------------------------- */
  /* -------------------- AsyncEntityProducer methods ------------------- */
  /* -------------------------------------------------------------------- */

  @Override
  public boolean isRepeatable() {
    return false;
  }

  @Override
  public boolean isChunked() {
    return false;
  }

  @Override
  public long getContentLength() {
    return contentLength;
  }

  @Override
  public String getContentType() {
    return ContentType.APPLICATION_OCTET_STREAM.toString();
  }

  @Override
  public String getContentEncoding() {
    return null;
  }

  @Override
  public Set<String> getTrailerNames() {
    return Collections.emptySet();
  }

  @Override
  public int available() {
    return 0;
  } // push producer

  /**
   * Called by the reactor whenever the socket can accept more bytes. Handles partial writes by
   * retaining an outbound {@link ByteBuffer}.
   */
  @Override
  public void produce(DataStreamChannel channel) throws IOException {
    if (closed.get()) {
      return;
    }

    try {
      /* 1 ─ Flush leftovers from a previous partial write. */
      if (currentChunk != null && currentChunk.hasRemaining()) {
        writeChunk(channel);
        if (currentChunk != null && currentChunk.hasRemaining()) {
          return; // socket still back-pressured
        }
      }

      /* 2 ─ If everything sent, close the stream. */
      if (endOfStream || totalSent >= contentLength) {
        channel.endStream();
        endOfStream = true;
        return;
      }

      /* 3 ─ Read new data from source. */
      int toRead = (int) Math.min(buf.length, contentLength - totalSent); // safe cast
      int read = source.read(buf, 0, toRead);

      if (read == -1) { // Premature EOF
        endOfStream = true;
        failAndClose(
            new IOException("Unexpected end of upload stream after " + totalSent + " bytes"));
        channel.endStream();
        return;
      }

      totalSent += read;
      currentChunk = ByteBuffer.wrap(buf, 0, read);
      writeChunk(channel); // try to flush immediately

      if (currentChunk == null && totalSent == contentLength) {
        channel.endStream();
      }
    } catch (Exception ex) {
      failAndClose(ex);
      channel.endStream();
      throw ex;
    }
  }

  @Override
  public void failed(Exception cause) {
    failAndClose(cause);
  }

  @Override
  public void releaseResources() {
    if (closed.compareAndSet(false, true)) {
      try {
        source.close();
      } catch (IOException ioe) {
        LOGGER.warn("Error while closing upload stream", ioe);
      }
    }
  }

  @Override
  public void close() {
    releaseResources();
  }

  private void writeChunk(DataStreamChannel channel) throws IOException {
    if (currentChunk == null) return;

    int written = channel.write(currentChunk); // may be zero
    LOGGER.trace("wrote {} bytes ({} sent/{})", written, totalSent, contentLength);

    if (!currentChunk.hasRemaining()) {
      currentChunk = null;
    } else {
      channel.requestOutput(); // explicit request to I/O reactor to call produce() again
    }
  }

  private void failAndClose(Exception ex) {
    failure.compareAndSet(null, ex);
    LOGGER.error(ex, "Upload failed after {} bytes", totalSent);
    releaseResources();
  }
}
