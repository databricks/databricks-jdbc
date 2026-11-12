package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
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

  /** A limiter of its own for each test, so tests do not share permits. */
  private static ChunkDownloadLimiter newLimiter() {
    return ChunkDownloadLimiter.forHost("limiter-test-" + UUID.randomUUID());
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
  void testEnsureAtLeastOnlyGrowsUpToMaxPermits() {
    ChunkDownloadLimiter limiter = newLimiter();
    int total = limiter.totalPermits();

    limiter.ensureAtLeast(total - 1);
    assertEquals(total, limiter.totalPermits());
    assertEquals(total, limiter.availablePermits());

    limiter.ensureAtLeast(total + 1);
    assertEquals(total + 1, limiter.totalPermits());
    assertEquals(total + 1, limiter.availablePermits());

    limiter.ensureAtLeast(10_000);
    assertEquals(256, limiter.totalPermits());
    assertEquals(256, limiter.availablePermits());
  }

  @Test
  void testHostsDoNotBlockEachOther() throws Exception {
    ChunkDownloadLimiter stalled = newLimiter();
    ChunkDownloadLimiter other = newLimiter();
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
  void testRemoteChunkProviderGrowsItsHostLimiterToPoolSize() throws Exception {
    String host = "limiter-test-" + UUID.randomUUID();
    IDatabricksSession session = mock(IDatabricksSession.class);
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(session.getConnectionContext()).thenReturn(context);
    when(context.getHost()).thenReturn(host);
    ResultManifest manifest =
        new ResultManifest()
            .setTotalChunkCount(0L)
            .setTotalRowCount(0L)
            .setSchema(new ResultSchema().setColumns(new ArrayList<>()));
    ResultData data = new ResultData().setExternalLinks(new ArrayList<>());

    RemoteChunkProvider provider =
        new RemoteChunkProvider(new StatementId("statement"), manifest, data, session, null, 100);
    try {
      assertSame(ChunkDownloadLimiter.forHost(host), provider.getDownloadLimiter());
      assertEquals(100, ChunkDownloadLimiter.forHost(host).totalPermits());
    } finally {
      provider.close();
    }
  }

  @Test
  void testStreamingChunkProviderGrowsItsHostLimiterToPoolSize() throws Exception {
    String host = "limiter-test-" + UUID.randomUUID();
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getHost()).thenReturn(host);

    StreamingChunkProvider provider =
        new StreamingChunkProvider(
            mock(ChunkLinkFetcher.class),
            httpClient,
            CompressionCodec.NONE,
            new StatementId("statement"),
            100,
            1,
            0,
            0.1,
            context,
            ChunkLinkFetchResult.endOfStream());
    try {
      assertEquals(100, ChunkDownloadLimiter.forHost(host).totalPermits());
    } finally {
      provider.close();
    }
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
