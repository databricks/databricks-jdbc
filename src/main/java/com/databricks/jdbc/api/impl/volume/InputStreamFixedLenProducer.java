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
  private static final int DEFAULT_BUF = 16 * 1024;

  private final InputStream source;
  private final long contentLength;
  private final byte[] buf;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicReference<Exception> failure = new AtomicReference<>();

  private ByteBuffer currentChunk = null;
  private long totalBytesRead = 0;

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
      // 1. If there's a leftover chunk from a previous partial write, try to send it first.
      if (currentChunk != null) {
        channel.write(currentChunk);
        if (currentChunk.hasRemaining()) {
          // The socket is still back-pressured. Request a callback and wait.
          channel.requestOutput();
          return;
        }
        currentChunk = null; // The leftover chunk has been sent successfully.
      }

      // 2. Check if we have finished sending all the data.
      // At this point, any pre-existing chunk has been sent, so `currentChunk` is null.
      if (totalBytesRead >= contentLength) {
        channel.endStream();
        releaseResources(); // All done, close the stream.
        return;
      }

      // 3. Read the next chunk of data from the source stream.
      int toRead = (int) Math.min(buf.length, contentLength - totalBytesRead);
      int bytesRead = source.read(buf, 0, toRead);

      if (bytesRead == -1) {
        // Premature End-Of-File is an error condition.
        throw new IOException(
            String.format(
                "Unexpected end of stream. Read %d bytes, but expected %d.",
                totalBytesRead, contentLength));
      }

      totalBytesRead += bytesRead;
      currentChunk = ByteBuffer.wrap(buf, 0, bytesRead);

      // 4. Immediately attempt to write the newly read chunk.
      channel.write(currentChunk);
      if (currentChunk.hasRemaining()) {
        // If the write was partial, request a callback to send the rest later.
        channel.requestOutput();
      } else {
        currentChunk = null;
        // If we've read everything and it was written completely, end the stream.
        if (totalBytesRead >= contentLength) {
          channel.endStream();
          releaseResources();
        }
      }

    } catch (Exception ex) {
      channel.endStream();
      // Centralized failure handling for any exception during production.
      if (failure.compareAndSet(null, ex)) {
        LOGGER.error(ex, "Upload failed after reading {} bytes", totalBytesRead);
        releaseResources();
      }
      // Propagate exception to the framework.
      throw ex;
    }
  }

  @Override
  public void failed(Exception cause) {
    // This is called by the HttpClient framework for external failures (e.g., connection reset).
    if (failure.compareAndSet(null, cause)) {
      LOGGER.error(
          cause, "Upload failed due to an external cause after reading {} bytes", totalBytesRead);
      releaseResources();
    }
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
}
