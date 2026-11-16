package com.databricks.jdbc.api.impl.arrow;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.TimeUnit;

/**
 * Bounds how many chunks a result decodes into memory, while letting downloads send their requests
 * before their chunk is within that bound.
 *
 * <p>Chunks are numbered by their position in consumption order. The chunk at position {@code p}
 * may read its body once {@code p < released + size}, where {@code released} counts the chunks the
 * reader has finished with. This is the same bound on decoded chunks as submitting a download only
 * once its chunk fits in the window, but a chunk beyond it can already wait for its response, so
 * that round trip overlaps earlier downloads instead of following them. Meanwhile the kernel may
 * receive the body into the socket buffer; {@link #MAX_REQUEST_LEAD} bounds how many such chunks
 * there are.
 */
final class ChunkAdmissionWindow {

  /**
   * How many chunks beyond the window may be requested while the reader waits. The kernel may
   * already receive their bodies, so this bounds the extra memory per result (about this many
   * compressed chunks, outside the heap) and what a reader that pauses past the unread limit
   * downloads again.
   */
  static final int MAX_REQUEST_LEAD = 8;

  /** Default limit on how long a download holds a response unread while waiting for admission. */
  static final long DEFAULT_MAX_UNREAD_RESPONSE_MILLIS = 10_000;

  private final int size;
  private final long maxUnreadResponseMillis;
  private long released;

  ChunkAdmissionWindow(int size) {
    this(size, DEFAULT_MAX_UNREAD_RESPONSE_MILLIS);
  }

  ChunkAdmissionWindow(int size, long maxUnreadResponseMillis) {
    this.size = size;
    this.maxUnreadResponseMillis = maxUnreadResponseMillis;
  }

  /** Returns how long a download may hold an unread response while waiting for admission. */
  long getMaxUnreadResponseMillis() {
    return maxUnreadResponseMillis;
  }

  /**
   * Waits until the chunk at {@code position} may read its body.
   *
   * @param timeoutMillis the longest to wait, or 0 to wait until admitted
   * @return true if admitted, false if the timeout passed first
   * @throws InterruptedIOException if interrupted (the result is being closed); the interrupt flag
   *     stays set so callers treat it as a cancellation
   */
  synchronized boolean await(long position, long timeoutMillis) throws InterruptedIOException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    while (position >= released + size) {
      long waitMillis = 0;
      if (timeoutMillis > 0) {
        long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
          return false;
        }
        waitMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
      }
      try {
        wait(waitMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new InterruptedIOException("Interrupted while waiting to read chunk " + position);
      }
    }
    return true;
  }

  /** Records that the reader has finished with one more chunk, admitting the next one. */
  synchronized void release() {
    released++;
    notifyAll();
  }

  /**
   * Thrown by a download that released its unread response because the reader did not admit its
   * chunk in time. The download has since been admitted and should request the chunk again; this is
   * not a failure.
   */
  static final class ResponseReleasedException extends IOException {
    ResponseReleasedException(long position) {
      super("Released the unread response for chunk " + position + " until the reader caught up");
    }
  }
}
