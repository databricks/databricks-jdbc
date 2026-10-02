package com.databricks.jdbc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DatabricksClientConfiguratorManagerTest {
  @Test
  void initializesCollidingUuidsConcurrently() throws Exception {
    assertEquals("Aa".hashCode(), "BB".hashCode());
    IDatabricksConnectionContext slowContext = context("Aa");
    IDatabricksConnectionContext fastContext = context("BB");
    ClientConfigurator slowConfigurator = mock(ClientConfigurator.class);
    ClientConfigurator fastConfigurator = mock(ClientConfigurator.class);
    CountDownLatch slowStarted = new CountDownLatch(1);
    CountDownLatch releaseSlow = new CountDownLatch(1);
    DatabricksClientConfiguratorManager manager =
        new DatabricksClientConfiguratorManager(
            context -> {
              if (context == slowContext) {
                slowStarted.countDown();
                await(releaseSlow);
                return slowConfigurator;
              }
              return fastConfigurator;
            });
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<ClientConfigurator> slow = executor.submit(() -> manager.getConfigurator(slowContext));
      assertTrue(slowStarted.await(5, TimeUnit.SECONDS));

      Future<ClientConfigurator> fast = executor.submit(() -> manager.getConfigurator(fastContext));
      assertSame(fastConfigurator, fast.get(5, TimeUnit.SECONDS));

      releaseSlow.countDown();
      assertSame(slowConfigurator, slow.get(5, TimeUnit.SECONDS));
    } finally {
      releaseSlow.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void sharesInFlightInitializationForSameUuid() throws Exception {
    IDatabricksConnectionContext context = context("shared");
    ClientConfigurator configurator = mock(ClientConfigurator.class);
    AtomicInteger creations = new AtomicInteger();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    DatabricksClientConfiguratorManager manager =
        new DatabricksClientConfiguratorManager(
            ignored -> {
              creations.incrementAndGet();
              started.countDown();
              await(release);
              return configurator;
            });
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<ClientConfigurator> first = executor.submit(() -> manager.getConfigurator(context));
      assertTrue(started.await(5, TimeUnit.SECONDS));
      assertNull(manager.getConfiguratorOnlyIfExists(context));

      Future<ClientConfigurator> second = executor.submit(() -> manager.getConfigurator(context));
      assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));

      release.countDown();
      assertSame(configurator, first.get(5, TimeUnit.SECONDS));
      assertSame(configurator, second.get(5, TimeUnit.SECONDS));
      assertEquals(1, creations.get());
      assertSame(configurator, manager.getConfiguratorOnlyIfExists(context));
    } finally {
      release.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static IDatabricksConnectionContext context(String uuid) {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getConnectionUuid()).thenReturn(uuid);
    return context;
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("Timed out waiting for configurator initialization");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
