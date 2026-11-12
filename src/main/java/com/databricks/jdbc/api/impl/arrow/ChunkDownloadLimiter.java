package com.databricks.jdbc.api.impl.arrow;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.Semaphore;

/**
 * Bounds how many CloudFetch chunks are downloaded and parsed at once across the JVM.
 *
 * <p>Each query still uses up to {@code cloudFetchThreadPoolSize} download threads, but a download
 * holds a permit only while it reads and parses its chunk. This bounds the transient heap that many
 * concurrent queries need. Permits are released when the download ends, not when the chunk is
 * consumed, so a slow reader cannot block other queries. The semaphore is fair, so waiting
 * downloads from different queries are served in arrival order.
 */
final class ChunkDownloadLimiter {
  // A download buffers its compressed chunk on-heap; budget this much heap per permit.
  private static final long HEAP_BYTES_PER_PERMIT = 128L << 20;
  // The default cloudFetchThreadPoolSize, so a single query keeps its parallelism.
  private static final int MIN_PERMITS = 16;
  private static final int MAX_PERMITS = 256;

  private static final Semaphore PERMITS = new Semaphore(0, true);
  private static int totalPermits = 0;

  static {
    ensureAtLeast(defaultPermits(Runtime.getRuntime().maxMemory()));
  }

  private ChunkDownloadLimiter() {}

  @VisibleForTesting
  static int defaultPermits(long maxHeapBytes) {
    long byHeap = maxHeapBytes / HEAP_BYTES_PER_PERMIT;
    return (int) Math.max(MIN_PERMITS, Math.min(MAX_PERMITS, byHeap));
  }

  /** Lets one query use {@code permits} parallel downloads, e.g. its cloudFetchThreadPoolSize. */
  static synchronized void ensureAtLeast(int permits) {
    if (permits > totalPermits) {
      PERMITS.release(permits - totalPermits);
      totalPermits = permits;
    }
  }

  static void acquire() throws InterruptedException {
    PERMITS.acquire();
  }

  static void release() {
    PERMITS.release();
  }

  @VisibleForTesting
  static synchronized int totalPermits() {
    return totalPermits;
  }

  @VisibleForTesting
  static int availablePermits() {
    return PERMITS.availablePermits();
  }

  @VisibleForTesting
  static boolean hasQueuedThreads() {
    return PERMITS.hasQueuedThreads();
  }
}
