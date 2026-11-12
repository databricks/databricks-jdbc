package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ChunkDownloadLimiterTest {
  @Mock ArrowResultChunk chunk;
  @Mock IDatabricksHttpClient httpClient;
  @Mock RemoteChunkProvider remoteChunkProvider;
  @Mock ChunkLinkDownloadService<ArrowResultChunk> linkDownloadService;
  @Mock LinkRefresher linkRefresher;

  private static final int TEST_DEFAULT_PERMITS = 16;

  /** A limiter of its own for each test, with a fixed default that does not depend on the heap. */
  private static ChunkDownloadLimiter newLimiter() {
    return ChunkDownloadLimiter.forTesting(TEST_DEFAULT_PERMITS);
  }

  /** Registers a fresh host whose limiter starts at TEST_DEFAULT_PERMITS. */
  private static String newHost() {
    String host = "limiter-test-" + UUID.randomUUID();
    ChunkDownloadLimiter.registerForTesting(host, TEST_DEFAULT_PERMITS);
    return host;
  }

  @Test
  void testDefaultPermitsFollowHeapSize() {
    assertEquals(16, ChunkDownloadLimiter.defaultPermits(512L << 20));
    assertEquals(16, ChunkDownloadLimiter.defaultPermits(2L << 30));
    assertEquals(32, ChunkDownloadLimiter.defaultPermits(4L << 30));
    assertEquals(64, ChunkDownloadLimiter.defaultPermits(8L << 30));
    assertEquals(256, ChunkDownloadLimiter.defaultPermits(64L << 30));
    assertEquals(256, ChunkDownloadLimiter.defaultPermits(Long.MAX_VALUE));
  }

  @Test
  void testForHostReturnsOneLimiterPerHost() {
    String host = "limiter-test-" + UUID.randomUUID();
    assertSame(ChunkDownloadLimiter.forHost(host), ChunkDownloadLimiter.forHost(host));
    assertNotSame(
        ChunkDownloadLimiter.forHost(host), ChunkDownloadLimiter.forHost(host + "-other"));
    assertSame(ChunkDownloadLimiter.forHost(null), ChunkDownloadLimiter.forHost(null));
  }

  @Test
  void testTotalFollowsLargestOpenPoolSize() {
    ChunkDownloadLimiter limiter = newLimiter();
    int base = TEST_DEFAULT_PERMITS;

    ChunkDownloadLimiter.PoolRegistration small = limiter.registerPool(base - 1);
    assertTotal(limiter, base);
    ChunkDownloadLimiter.PoolRegistration medium = limiter.registerPool(base + 1);
    assertTotal(limiter, base + 1);
    ChunkDownloadLimiter.PoolRegistration huge = limiter.registerPool(10_000);
    assertTotal(limiter, 256);

    huge.close();
    assertTotal(limiter, base + 1);
    medium.close();
    assertTotal(limiter, base);
    small.close();
    assertTotal(limiter, base);
  }

  @Test
  void testRegistrationCloseIsIdempotent() {
    ChunkDownloadLimiter limiter = newLimiter();
    int size = TEST_DEFAULT_PERMITS + 4;
    ChunkDownloadLimiter.PoolRegistration first = limiter.registerPool(size);
    ChunkDownloadLimiter.PoolRegistration second = limiter.registerPool(size);

    first.close();
    first.close();
    assertTotal(limiter, size);
    second.close();
    assertTotal(limiter, TEST_DEFAULT_PERMITS);
  }

  @Test
  void testLoweringTotalWaitsForDownloadsInProgress() throws Exception {
    ChunkDownloadLimiter limiter = newLimiter();
    int size = TEST_DEFAULT_PERMITS + 4;
    ChunkDownloadLimiter.PoolRegistration registration = limiter.registerPool(size);
    for (int i = 0; i < size; i++) {
      limiter.acquire();
    }

    registration.close();
    assertEquals(TEST_DEFAULT_PERMITS, limiter.totalPermits());
    assertEquals(-4, limiter.availablePermits());
    for (int i = 0; i < 4; i++) {
      limiter.release();
    }
    // Back at the lowered total with every permit in use, so a new download has to wait.
    assertEquals(0, limiter.availablePermits());
    for (int i = 4; i < size; i++) {
      limiter.release();
    }
    assertEquals(TEST_DEFAULT_PERMITS, limiter.availablePermits());
  }

  private static void assertTotal(ChunkDownloadLimiter limiter, int expected) {
    assertEquals(expected, limiter.totalPermits());
    assertEquals(expected, limiter.availablePermits());
  }

  @Test
  void testHostsDoNotBlockEachOther() throws Exception {
    ChunkDownloadLimiter stalled = ChunkDownloadLimiter.forHost(newHost());
    ChunkDownloadLimiter other = ChunkDownloadLimiter.forHost(newHost());
    assertNotSame(stalled, other);
    int held = stalled.availablePermits();
    for (int i = 0; i < held; i++) {
      stalled.acquire();
    }
    try {
      assertEquals(0, stalled.availablePermits());
      other.acquire();
      other.release();
    } finally {
      for (int i = 0; i < held; i++) {
        stalled.release();
      }
    }
  }

  @Test
  void testConcurrentDownloadsNeverExceedPermits() throws Exception {
    ChunkDownloadLimiter limiter = newLimiter();
    // Hold all but a few permits so the bound under test is small.
    int permits = 4;
    int held = limiter.availablePermits() - permits;
    assertTrue(held >= 0, "limiter has fewer than " + permits + " permits");
    for (int i = 0; i < held; i++) {
      limiter.acquire();
    }
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger maxInFlight = new AtomicInteger();
    when(remoteChunkProvider.getDownloadLimiter()).thenReturn(limiter);
    when(chunk.getChunkReadyFuture()).thenAnswer(invocation -> new CompletableFuture<Void>());
    when(remoteChunkProvider.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    doAnswer(
            invocation -> {
              maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
              Thread.sleep(20);
              inFlight.decrementAndGet();
              return null;
            })
        .when(chunk)
        .downloadData(any(), any(), anyDouble());

    int tasks = permits * 3;
    ExecutorService pool = Executors.newFixedThreadPool(tasks);
    try {
      List<Future<Void>> results = new ArrayList<>();
      for (int i = 0; i < tasks; i++) {
        results.add(
            pool.submit(
                new ChunkDownloadTask(
                    chunk, httpClient, remoteChunkProvider, linkDownloadService)));
      }
      for (Future<Void> result : results) {
        result.get(30, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
      for (int i = 0; i < held; i++) {
        limiter.release();
      }
    }
    assertTrue(maxInFlight.get() <= permits, "max in flight " + maxInFlight.get());
    assertTrue(maxInFlight.get() > 1, "downloads should still run in parallel");
    assertEquals(limiter.totalPermits(), limiter.availablePermits());
  }

  @Test
  void testPermitReleasedWhenDownloadFails() throws Exception {
    ChunkDownloadLimiter limiter = newLimiter();
    CompletableFuture<Void> ready = new CompletableFuture<>();
    when(remoteChunkProvider.getDownloadLimiter()).thenReturn(limiter);
    when(chunk.getChunkReadyFuture()).thenReturn(ready);
    when(remoteChunkProvider.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    doThrow(new IllegalStateException("boom")).when(chunk).downloadData(any(), any(), anyDouble());

    assertThrows(
        IllegalStateException.class,
        () ->
            new ChunkDownloadTask(chunk, httpClient, remoteChunkProvider, linkDownloadService)
                .call());
    assertTrue(ready.isCompletedExceptionally());
    assertEquals(limiter.totalPermits(), limiter.availablePermits());
  }

  @Test
  void testInterruptWhileWaitingForPermitFailsChunk() throws Exception {
    ChunkDownloadLimiter limiter = newLimiter();
    when(remoteChunkProvider.getDownloadLimiter()).thenReturn(limiter);
    when(chunk.getChunkIndex()).thenReturn(3L);
    assertInterruptedWhileWaitingFailsChunk(
        limiter,
        new ChunkDownloadTask(chunk, httpClient, remoteChunkProvider, linkDownloadService));
  }

  @Test
  void testInterruptWhileWaitingForPermitFailsStreamingChunk() throws Exception {
    ChunkDownloadLimiter limiter = newLimiter();
    when(chunk.getChunkIndex()).thenReturn(3L);
    assertInterruptedWhileWaitingFailsChunk(
        limiter,
        new StreamingChunkDownloadTask(
            chunk, httpClient, CompressionCodec.NONE, linkRefresher, 0.1, limiter));
    // An interrupt while waiting for a permit fails the chunk without going through a retry.
    verify(chunk, never()).setStatus(ChunkStatus.DOWNLOAD_RETRY);
  }

  @Test
  void testLinkExpiredWhileWaitingIsRefreshedWithoutRetry() throws Exception {
    ChunkDownloadLimiter limiter = newLimiter();
    CompletableFuture<Void> ready = new CompletableFuture<>();
    when(remoteChunkProvider.getDownloadLimiter()).thenReturn(limiter);
    when(remoteChunkProvider.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(chunk.getChunkReadyFuture()).thenReturn(ready);
    // Valid before the permit wait, expired after it, then refreshed.
    when(chunk.isChunkLinkInvalid()).thenReturn(false, true, true);
    ExternalLink link = mock(ExternalLink.class);
    AtomicInteger permitsDuringRefresh = new AtomicInteger(-1);
    when(linkDownloadService.getLinkForChunk(anyLong()))
        .thenAnswer(
            invocation -> {
              permitsDuringRefresh.set(limiter.availablePermits());
              return CompletableFuture.completedFuture(link);
            });

    runAfterWaitingForPermit(
        limiter,
        new ChunkDownloadTask(chunk, httpClient, remoteChunkProvider, linkDownloadService));

    // The task refreshed while holding the only free permit, so it did not queue again.
    assertEquals(0, permitsDuringRefresh.get());
    verify(chunk).setChunkLink(link);
    verify(chunk, times(1)).downloadData(any(), any(), anyDouble());
    verify(chunk, never()).setStatus(ChunkStatus.DOWNLOAD_RETRY);
    assertTrue(ready.isDone() && !ready.isCompletedExceptionally());
    assertEquals(limiter.totalPermits(), limiter.availablePermits());
  }

  @Test
  void testPendingLinkRefreshAfterWaitDoesNotHoldPermit() throws Exception {
    ChunkDownloadLimiter limiter = newLimiter();
    CompletableFuture<Void> ready = new CompletableFuture<>();
    when(remoteChunkProvider.getDownloadLimiter()).thenReturn(limiter);
    when(remoteChunkProvider.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(chunk.getChunkReadyFuture()).thenReturn(ready);
    // Valid before the permit wait, expired after it and until the refresh lands.
    when(chunk.isChunkLinkInvalid()).thenReturn(false, true, true);
    CompletableFuture<ExternalLink> pendingLink = new CompletableFuture<>();
    AtomicInteger linkRequests = new AtomicInteger();
    when(linkDownloadService.getLinkForChunk(anyLong()))
        .thenAnswer(
            invocation -> {
              linkRequests.incrementAndGet();
              return pendingLink;
            });

    int held = limiter.availablePermits();
    for (int i = 0; i < held; i++) {
      limiter.acquire();
    }
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<Void> result =
          pool.submit(
              new ChunkDownloadTask(chunk, httpClient, remoteChunkProvider, linkDownloadService));
      awaitTrue(limiter::hasQueuedThreads, "task never waited for a permit");
      limiter.release();
      // The second request comes after the task gave its permit back to wait for the link.
      awaitTrue(() -> linkRequests.get() == 2, "task never waited for the pending link");
      assertEquals(1, limiter.availablePermits());
      verify(chunk, never()).downloadData(any(), any(), anyDouble());

      pendingLink.complete(mock(ExternalLink.class));
      result.get(30, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
      for (int i = 1; i < held; i++) {
        limiter.release();
      }
    }
    verify(chunk, times(1)).downloadData(any(), any(), anyDouble());
    verify(chunk, never()).setStatus(ChunkStatus.DOWNLOAD_RETRY);
    assertTrue(ready.isDone() && !ready.isCompletedExceptionally());
    assertEquals(limiter.totalPermits(), limiter.availablePermits());
  }

  private static void awaitTrue(java.util.function.BooleanSupplier condition, String message)
      throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertTrue(condition.getAsBoolean(), message);
  }

  @Test
  void testStreamingLinkExpiredWhileWaitingIsRefreshedWithoutRetry() throws Exception {
    ChunkDownloadLimiter limiter = newLimiter();
    CompletableFuture<Void> ready = new CompletableFuture<>();
    when(chunk.getChunkReadyFuture()).thenReturn(ready);
    when(chunk.isChunkLinkInvalid()).thenReturn(false, true, true);
    AtomicInteger permitsDuringRefresh = new AtomicInteger(-1);
    when(linkRefresher.refreshLink(anyLong(), anyLong()))
        .thenAnswer(
            invocation -> {
              permitsDuringRefresh.set(limiter.availablePermits());
              return null;
            });

    runAfterWaitingForPermit(
        limiter,
        new StreamingChunkDownloadTask(
            chunk, httpClient, CompressionCodec.NONE, linkRefresher, 0.1, limiter));

    assertEquals(0, permitsDuringRefresh.get());
    verify(linkRefresher, times(1)).refreshLink(anyLong(), anyLong());
    verify(chunk, times(1)).downloadData(any(), any(), anyDouble());
    verify(chunk, never()).setStatus(ChunkStatus.DOWNLOAD_RETRY);
    assertTrue(ready.isDone() && !ready.isCompletedExceptionally());
    assertEquals(limiter.totalPermits(), limiter.availablePermits());
  }

  /** Runs the task so it has to wait for a permit: holds them all, then frees one once it waits. */
  private static void runAfterWaitingForPermit(ChunkDownloadLimiter limiter, Callable<Void> task)
      throws Exception {
    int held = limiter.availablePermits();
    for (int i = 0; i < held; i++) {
      limiter.acquire();
    }
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<Void> result = pool.submit(task);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!limiter.hasQueuedThreads() && System.nanoTime() < deadline) {
        Thread.sleep(5);
      }
      assertTrue(limiter.hasQueuedThreads(), "task never waited for a permit");
      limiter.release();
      result.get(30, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
      for (int i = 1; i < held; i++) {
        limiter.release();
      }
    }
  }

  @Test
  void testRemoteChunkProviderDoesNotRaiseItsHostLimitForAnEmptyResult() throws Exception {
    String host = newHost();
    RemoteChunkProvider provider = newRemoteChunkProvider(host, 0, TEST_DEFAULT_PERMITS + 1);
    try {
      assertSame(ChunkDownloadLimiter.forHost(host), provider.getDownloadLimiter());
      assertEquals(TEST_DEFAULT_PERMITS, ChunkDownloadLimiter.forHost(host).totalPermits());
    } finally {
      provider.close();
    }
  }

  @Test
  void testRemoteChunkProviderRaisesItsHostLimitOnlyToItsChunkCountWhileOpen() throws Exception {
    String host = "limiter-test-" + UUID.randomUUID();
    ChunkDownloadLimiter.registerForTesting(host, 1);
    // 17 download threads, but only 2 chunks can ever download at once.
    RemoteChunkProvider provider = newRemoteChunkProvider(host, 2, TEST_DEFAULT_PERMITS + 1);
    try {
      assertEquals(2, ChunkDownloadLimiter.forHost(host).totalPermits());
    } finally {
      provider.close();
    }
    assertEquals(1, ChunkDownloadLimiter.forHost(host).totalPermits());
  }

  /** A provider whose chunks have valid links; their downloads fail fast on the mock client. */
  private RemoteChunkProvider newRemoteChunkProvider(String host, int chunkCount, int poolSize)
      throws Exception {
    IDatabricksSession session = mock(IDatabricksSession.class);
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(session.getConnectionContext()).thenReturn(context);
    when(context.getHost()).thenReturn(host);
    List<BaseChunkInfo> chunks = new ArrayList<>();
    List<ExternalLink> links = new ArrayList<>();
    String expiration = Instant.now().plus(1, ChronoUnit.HOURS).toString();
    for (long i = 0; i < chunkCount; i++) {
      chunks.add(new BaseChunkInfo().setChunkIndex(i).setRowOffset(i).setRowCount(1L));
      links.add(
          new ExternalLink()
              .setChunkIndex(i)
              .setExternalLink("https://storage.invalid/chunk-" + i)
              .setExpiration(expiration));
    }
    ResultManifest manifest =
        new ResultManifest()
            .setTotalChunkCount((long) chunkCount)
            .setTotalRowCount((long) chunkCount)
            .setChunks(chunks)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()));
    ResultData data = new ResultData().setExternalLinks(links);
    return new RemoteChunkProvider(
        new StatementId("statement"), manifest, data, session, httpClient, poolSize);
  }

  @Test
  void testStreamingChunkProviderRaisesItsHostLimitWhileOpen() throws Exception {
    String host = newHost();
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getHost()).thenReturn(host);
    int poolSize = TEST_DEFAULT_PERMITS + 1;

    StreamingChunkProvider provider =
        new StreamingChunkProvider(
            mock(ChunkLinkFetcher.class),
            httpClient,
            CompressionCodec.NONE,
            new StatementId("statement"),
            poolSize,
            1,
            0,
            0.1,
            context,
            ChunkLinkFetchResult.endOfStream());
    try {
      assertEquals(poolSize, ChunkDownloadLimiter.forHost(host).totalPermits());
    } finally {
      provider.close();
    }
    assertEquals(TEST_DEFAULT_PERMITS, ChunkDownloadLimiter.forHost(host).totalPermits());
  }

  /** Holds every permit, then interrupts a task blocked on the limiter, as close() does. */
  private void assertInterruptedWhileWaitingFailsChunk(
      ChunkDownloadLimiter limiter, Callable<Void> task) throws Exception {
    CompletableFuture<Void> ready = new CompletableFuture<>();
    when(chunk.getChunkReadyFuture()).thenReturn(ready);
    int held = limiter.availablePermits();
    for (int i = 0; i < held; i++) {
      limiter.acquire();
    }
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<Void> result = pool.submit(task);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!limiter.hasQueuedThreads() && System.nanoTime() < deadline) {
        Thread.sleep(5);
      }
      assertTrue(limiter.hasQueuedThreads(), "task never waited for a permit");
      pool.shutdownNow(); // interrupts the waiting task
      assertThrows(Exception.class, () -> result.get(10, TimeUnit.SECONDS));
      assertTrue(ready.isCompletedExceptionally());
      verify(chunk, never()).downloadData(any(), any(), anyDouble());
    } finally {
      for (int i = 0; i < held; i++) {
        limiter.release();
      }
    }
    assertEquals(held, limiter.availablePermits());
  }
}
