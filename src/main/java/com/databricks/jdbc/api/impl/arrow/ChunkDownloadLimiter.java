package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * up downloads for another. Each host gets the full heap-based budget, so a JVM querying several
 * workspaces at once can have that many downloads in progress per host. Within one host, a download
 * keeps its permit through the HTTP client's own retries and slow reads, which can delay that
 * host's other queries, but never while waiting on a link-fetch RPC or in the task's retry backoff
 * (a Thrift task may briefly hold it while it checks for an already refreshed link). A user-set
 * {@code ChunkReadyTimeoutSeconds} includes the time spent waiting for a permit.
 *
 * <p>While a query whose {@code cloudFetchThreadPoolSize} is above the heap-based default is open,
 * its host's total is raised to that size (up to 256), so it keeps the parallelism it asked for.
 * The total drops back when the query closes; a result set that is never closed keeps it raised,
 * just as it keeps its download threads.
 */
final class ChunkDownloadLimiter {
  // Heap budget per in-progress download. Generous for the ~10-20 MB a download needs; more
  // permits did not raise aggregate throughput in benchmarks.
  private static final long HEAP_BYTES_PER_PERMIT = 128L << 20;
  // The default cloudFetchThreadPoolSize, so a single query keeps its parallelism.
  private static final int MIN_PERMITS = 16;
  private static final int MAX_PERMITS = 256;

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ChunkDownloadLimiter.class);

  // Never evicted: an entry is small, and dropping one that a provider or task still holds would
  // give its host a second limiter and twice the bound.
  private static final ConcurrentHashMap<String, ChunkDownloadLimiter> BY_HOST =
      new ConcurrentHashMap<>();

  private final Permits permits = new Permits();
  private final String host;
  private final int defaultPermits;
  // Pool size -> number of open queries with that size. Guarded by this.
  private final TreeMap<Integer, Integer> openPoolSizes = new TreeMap<>();
  private int totalPermits = 0;
  private boolean loggedRaise = false; // Guarded by this.

  private ChunkDownloadLimiter(String host, int defaultPermits) {
    this.host = host;
    this.defaultPermits = defaultPermits;
    permits.release(defaultPermits);
    totalPermits = defaultPermits;
  }

  /** Returns the limiter for a workspace host; an unknown host shares one default limiter. */
  static ChunkDownloadLimiter forHost(String host) {
    return BY_HOST.computeIfAbsent(
        host == null ? "" : host,
        h -> new ChunkDownloadLimiter(h, defaultPermits(Runtime.getRuntime().maxMemory())));
  }

  /** A limiter with a fixed default that is not shared with any host. */
  @VisibleForTesting
  static ChunkDownloadLimiter forTesting(int defaultPermits) {
    return new ChunkDownloadLimiter("test", defaultPermits);
  }

  /** Registers a limiter with a fixed default for {@code host}, for tests that build providers. */
  @VisibleForTesting
  static ChunkDownloadLimiter registerForTesting(String host, int defaultPermits) {
    ChunkDownloadLimiter limiter = new ChunkDownloadLimiter(host, defaultPermits);
    BY_HOST.put(host, limiter);
    return limiter;
  }

  @VisibleForTesting
  static int defaultPermits(long maxHeapBytes) {
    long byHeap = maxHeapBytes / HEAP_BYTES_PER_PERMIT;
    return (int) Math.max(MIN_PERMITS, Math.min(MAX_PERMITS, byHeap));
  }

  /**
   * Lets an open query use up to {@code poolSize} parallel downloads (at most its
   * cloudFetchThreadPoolSize). The total is the largest pool size among open queries, at least the
   * heap-based default and at most MAX_PERMITS. Close the returned registration when the query
   * closes.
   */
  synchronized PoolRegistration registerPool(int poolSize) {
    openPoolSizes.merge(poolSize, 1, Integer::sum);
    resize();
    return new PoolRegistration(poolSize);
  }

  private synchronized void unregisterPool(int poolSize) {
    openPoolSizes.computeIfPresent(poolSize, (size, count) -> count == 1 ? null : count - 1);
    resize();
  }

  private void resize() {
    int largestOpen = openPoolSizes.isEmpty() ? 0 : openPoolSizes.lastKey();
    int target = Math.max(defaultPermits, Math.min(largestOpen, MAX_PERMITS));
    if (target == totalPermits) {
      return;
    }
    if (target > totalPermits) {
      permits.release(target - totalPermits);
    } else {
      // Permits still in use make availablePermits negative, so no new download starts until
      // enough in-progress downloads finish.
      permits.reduce(totalPermits - target);
    }
    String message =
        "Set the concurrent CloudFetch download limit for host [{}] to {} (largest open query"
            + " parallelism {}, heap-based default {})";
    // INFO only once per host, so queries that keep raising and restoring it do not add noise.
    if (target > defaultPermits && !loggedRaise) {
      loggedRaise = true;
      LOGGER.info(message, host, target, largestOpen, defaultPermits);
    } else {
      LOGGER.debug(message, host, target, largestOpen, defaultPermits);
    }
    totalPermits = target;
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

  /** An open query's share of its host's download limit; see {@link #registerPool}. */
  final class PoolRegistration {
    private final int poolSize;
    private final AtomicBoolean closed = new AtomicBoolean();

    private PoolRegistration(int poolSize) {
      this.poolSize = poolSize;
    }

    /** Idempotent. */
    void close() {
      if (closed.compareAndSet(false, true)) {
        unregisterPool(poolSize);
      }
    }
  }

  private static final class Permits extends Semaphore {
    Permits() {
      super(0, true);
    }

    void reduce(int reduction) {
      reducePermits(reduction);
    }
  }
}
