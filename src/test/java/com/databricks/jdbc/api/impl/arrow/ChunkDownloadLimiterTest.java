package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import java.util.ArrayList;
import java.util.List;
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
  void testEnsureAtLeastOnlyGrowsUpToMaxPermits() {
    int total = ChunkDownloadLimiter.totalPermits();
    int available = ChunkDownloadLimiter.availablePermits();

    ChunkDownloadLimiter.ensureAtLeast(total - 1);
    assertEquals(total, ChunkDownloadLimiter.totalPermits());
    assertEquals(available, ChunkDownloadLimiter.availablePermits());

    if (total < 256) {
      ChunkDownloadLimiter.ensureAtLeast(total + 1);
      assertEquals(total + 1, ChunkDownloadLimiter.totalPermits());
      assertEquals(available + 1, ChunkDownloadLimiter.availablePermits());
    }

    ChunkDownloadLimiter.ensureAtLeast(10_000);
    assertEquals(256, ChunkDownloadLimiter.totalPermits());
    assertEquals(available + 256 - total, ChunkDownloadLimiter.availablePermits());
  }

  @Test
  void testConcurrentDownloadsNeverExceedPermits() throws Exception {
    // Hold all but a few permits so the bound is small no matter how large the limiter is.
    int permits = 4;
    int held = ChunkDownloadLimiter.availablePermits() - permits;
    for (int i = 0; i < held; i++) {
      ChunkDownloadLimiter.acquire();
    }
    try {
      runDownloadsAndCheckBound(permits);
    } finally {
      for (int i = 0; i < held; i++) {
        ChunkDownloadLimiter.release();
      }
    }
  }

  private void runDownloadsAndCheckBound(int permits) throws Exception {
    int available = ChunkDownloadLimiter.availablePermits();
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger maxInFlight = new AtomicInteger();
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
    }
    assertTrue(maxInFlight.get() <= permits, "max in flight " + maxInFlight.get());
    assertTrue(maxInFlight.get() > 1, "downloads should still run in parallel");
    assertEquals(available, ChunkDownloadLimiter.availablePermits());
  }

  @Test
  void testPermitReleasedWhenDownloadFails() throws Exception {
    int available = ChunkDownloadLimiter.availablePermits();
    CompletableFuture<Void> ready = new CompletableFuture<>();
    when(chunk.getChunkReadyFuture()).thenReturn(ready);
    when(remoteChunkProvider.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    doThrow(new IllegalStateException("boom")).when(chunk).downloadData(any(), any(), anyDouble());

    assertThrows(
        IllegalStateException.class,
        () ->
            new ChunkDownloadTask(chunk, httpClient, remoteChunkProvider, linkDownloadService)
                .call());
    assertTrue(ready.isCompletedExceptionally());
    assertEquals(available, ChunkDownloadLimiter.availablePermits());
  }

  @Test
  void testInterruptWhileWaitingForPermitFailsChunk() throws Exception {
    when(chunk.getChunkIndex()).thenReturn(3L);
    assertInterruptedWhileWaitingFailsChunk(
        new ChunkDownloadTask(chunk, httpClient, remoteChunkProvider, linkDownloadService));
  }

  @Test
  void testInterruptWhileWaitingForPermitFailsStreamingChunk() throws Exception {
    when(chunk.getChunkIndex()).thenReturn(3L);
    assertInterruptedWhileWaitingFailsChunk(
        new StreamingChunkDownloadTask(
            chunk, httpClient, CompressionCodec.NONE, linkRefresher, 0.1));
    // An interrupt while waiting for a permit fails the chunk without going through a retry.
    verify(chunk, never()).setStatus(ChunkStatus.DOWNLOAD_RETRY);
  }

  /** Holds every permit, then interrupts a task blocked on the limiter, as close() does. */
  private void assertInterruptedWhileWaitingFailsChunk(java.util.concurrent.Callable<Void> task)
      throws Exception {
    CompletableFuture<Void> ready = new CompletableFuture<>();
    when(chunk.getChunkReadyFuture()).thenReturn(ready);
    int held = ChunkDownloadLimiter.availablePermits();
    for (int i = 0; i < held; i++) {
      ChunkDownloadLimiter.acquire();
    }
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<Void> result = pool.submit(task);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (!ChunkDownloadLimiter.hasQueuedThreads() && System.nanoTime() < deadline) {
        Thread.sleep(5);
      }
      pool.shutdownNow(); // interrupts the waiting task
      assertThrows(Exception.class, () -> result.get(10, TimeUnit.SECONDS));
      assertTrue(ready.isCompletedExceptionally());
      verify(chunk, never()).downloadData(any(), any(), anyDouble());
    } finally {
      for (int i = 0; i < held; i++) {
        ChunkDownloadLimiter.release();
      }
    }
    assertEquals(held, ChunkDownloadLimiter.availablePermits());
  }
}
