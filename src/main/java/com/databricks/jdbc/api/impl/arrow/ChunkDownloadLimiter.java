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
  // Heap budget per in-progress download. Generous for the ~10-20 MB a download needs; more
  // permits did not raise aggregate throughput in benchmarks.
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

  /**
   * Lets one query use up to {@code permits} parallel downloads, e.g. its cloudFetchThreadPoolSize.
   * The total only grows (it is the largest size seen so far) and never exceeds MAX_PERMITS.
   */
  static synchronized void ensureAtLeast(int permits) {
    int target = Math.min(permits, MAX_PERMITS);
    if (target > totalPermits) {
      PERMITS.release(target - totalPermits);
      totalPermits = target;
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
