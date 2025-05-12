package com.databricks.jdbc.api.impl.volume;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.DataStreamChannel;

/**
 * Zero‑copy (except the internal buffer) streaming producer for a single‑shot PUT/POST with a known
 * {@code Content‑Length}. No chunked encoding, no Expect: 100‑continue, constant memory ≈ {@code
 * bufferSize}.
 */
public final class InputStreamFixedLenProducer implements AsyncEntityProducer, Closeable {

  private static final int DEFAULT_BUF = 16 * 1024;

  private final InputStream source;
  private final long contentLength;
  private final ContentType contentType;
  private final byte[] buf;

  private long totalSent = 0;
  private final AtomicReference<Exception> failure = new AtomicReference<>();

  public InputStreamFixedLenProducer(
      final InputStream source,
      final long contentLength,
      final ContentType contentType,
      final int bufferSize) {

    this.source = source;
    this.contentLength = contentLength;
    this.contentType = Objects.requireNonNull(contentType, "contentType");
    this.buf = new byte[Math.max(bufferSize, DEFAULT_BUF)];
  }

  public InputStreamFixedLenProducer(
      final InputStream source, final long contentLength, final ContentType contentType) {
    this(source, contentLength, contentType, DEFAULT_BUF);
  }

  /* -------------------------------------------------------------------- */
  /* ---------------  AsyncEntityProducer compulsory API ---------------- */
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
    return contentType.toString();
  }

  @Override
  public String getContentEncoding() {
    return null;
  }

  @Override
  public Set<String> getTrailerNames() {
    return Collections.emptySet();
  }

  //    @Override public Exception getException()             { return failure.get(); }

  /** Bytes already buffered and ready to be flushed (hint only). */
  @Override
  public int available() {
    return 0; // we push straight from the stream
  }

  /**
   * Called by the I/O reactor whenever it can accept more data. We block <b>briefly</b> on {@code
   * InputStream.read}; for very large or slow sources consider off‑loading to an executor.
   */
  @Override
  public void produce(final DataStreamChannel channel) throws IOException {
    try {
      if (totalSent >= contentLength) {
        channel.endStream();
        return;
      }
      int read = source.read(buf, 0, (int) Math.min(buf.length, contentLength - totalSent));
      if (read == -1) { // premature EOF
        throw new IOException("Stream ended before expected length");
      }
      totalSent += read;
      channel.write(ByteBuffer.wrap(buf, 0, read));
      if (totalSent == contentLength) {
        channel.endStream();
      }
    } catch (Exception ex) {
      failure.compareAndSet(null, ex);
      channel.endStream();
      throw ex;
    }
  }

  /** Called by the I/O reactor if the transfer is aborted. */
  @Override
  public void failed(final Exception cause) {
    failure.compareAndSet(null, cause);
    releaseResources();
  }

  @Override
  public void releaseResources() {
    try {
      source.close();
    } catch (IOException ignore) {
    }
  }

  /* java.lang.AutoCloseable ------------------------------------------- */
  @Override
  public void close() {
    releaseResources();
  }
}
