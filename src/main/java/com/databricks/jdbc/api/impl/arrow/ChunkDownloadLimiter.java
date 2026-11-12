package com.databricks.jdbc.api.impl.arrow;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Bounds how many CloudFetch chunks are downloaded and parsed at once for one workspace host.
 *
 * <p>Each query still uses up to {@code cloudFetchThreadPoolSize} download threads, but a download
 * holds a permit only while it fetches and parses its chunk. This bounds the transient heap that
 * many concurrent queries need. Permits are released when the download ends, not when the chunk is
 * consumed, so a slow reader cannot block other queries. The semaphore is fair, so waiting
 * downloads from different queries are served in arrival order.
 *
 * <p>There is one limiter per host, so throttled or stalled storage for one workspace cannot hold
 * up downloads for another. Within one host, a download keeps its permit through the HTTP client's
 * own retries and slow reads (but not through the task's retry backoff), which can delay that
 * host's other queries.
 *
 * <p>A query whose {@code cloudFetchThreadPoolSize} is above the heap-based default raises its
 * host's total to that size (up to 256) for the life of the JVM, so it keeps the parallelism it
 * asked for, as before this limiter.
 */
final class ChunkDownloadLimiter {
  // Heap budget per in-progress download. Generous for the ~10-20 MB a download needs; more
  // permits did not raise aggregate throughput in benchmarks.
  private static final long HEAP_BYTES_PER_PERMIT = 128L << 20;
  // The default cloudFetchThreadPoolSize, so a single query keeps its parallelism.
  private static final int MIN_PERMITS = 16;
  private static final int MAX_PERMITS = 256;

  private static final ConcurrentHashMap<String, ChunkDownloadLimiter> BY_HOST =
      new ConcurrentHashMap<>();

  private final Semaphore permits = new Semaphore(0, true);
  private int totalPermits = 0;

  private ChunkDownloadLimiter(int initialPermits) {
    ensureAtLeast(initialPermits);
  }

  /** Returns the limiter for a workspace host; an unknown host shares one default limiter. */
  static ChunkDownloadLimiter forHost(String host) {
    return BY_HOST.computeIfAbsent(
        host == null ? "" : host,
        h -> new ChunkDownloadLimiter(defaultPermits(Runtime.getRuntime().maxMemory())));
  }

  @VisibleForTesting
  static int defaultPermits(long maxHeapBytes) {
    long byHeap = maxHeapBytes / HEAP_BYTES_PER_PERMIT;
    return (int) Math.max(MIN_PERMITS, Math.min(MAX_PERMITS, byHeap));
  }

  /**
   * Lets one query use up to {@code permits} parallel downloads, e.g. its cloudFetchThreadPoolSize.
   * The total only grows (it is the largest size seen so far) and never exceeds MAX_PERMITS.
   */
  synchronized void ensureAtLeast(int permits) {
    int target = Math.min(permits, MAX_PERMITS);
    if (target > totalPermits) {
      this.permits.release(target - totalPermits);
      totalPermits = target;
    }
  }

  /** Acquires a permit and returns whether the caller had to wait for it. */
  boolean acquire() throws InterruptedException {
    // tryAcquire(0, unit) honors fairness, unlike tryAcquire().
    if (permits.tryAcquire(0, TimeUnit.SECONDS)) {
      return false;
    }
    permits.acquire();
    return true;
  }

  void release() {
    permits.release();
  }

  @VisibleForTesting
  synchronized int totalPermits() {
    return totalPermits;
  }

  @VisibleForTesting
  int availablePermits() {
    return permits.availablePermits();
  }

  @VisibleForTesting
  boolean hasQueuedThreads() {
    return permits.hasQueuedThreads();
  }
}
