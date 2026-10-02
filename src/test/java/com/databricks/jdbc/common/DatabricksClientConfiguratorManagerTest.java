package com.databricks.jdbc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
    CountDownLatch secondStarted = new CountDownLatch(1);
    AtomicReference<Thread> secondThread = new AtomicReference<>();
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

      Future<ClientConfigurator> second =
          executor.submit(
              () -> {
                secondThread.set(Thread.currentThread());
                secondStarted.countDown();
                return manager.getConfigurator(context);
              });
      assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
      awaitJoin(secondThread.get());
      assertFalse(second.isDone());

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

  @Test
  void rejectsSameThreadReentryWithoutExportingTelemetry() throws Exception {
    IDatabricksConnectionContext context = context("recursive");
    ClientConfigurator configurator = mock(ClientConfigurator.class);
    AtomicReference<DatabricksClientConfiguratorManager> manager = new AtomicReference<>();
    manager.set(
        new DatabricksClientConfiguratorManager(
            ignored -> {
              try (MockedStatic<TelemetryHelper> telemetry =
                  Mockito.mockStatic(TelemetryHelper.class)) {
                IllegalStateException error =
                    assertThrows(
                        IllegalStateException.class, () -> manager.get().getConfigurator(context));
                assertTrue(error.getMessage().contains("Recursive configurator initialization"));
                telemetry.verifyNoInteractions();
              }
              return configurator;
            }));
    ExecutorService executor =
        Executors.newSingleThreadExecutor(
            task -> {
              Thread thread = new Thread(task, "configurator-reentry-test");
              thread.setDaemon(true);
              return thread;
            });
    try {
      assertSame(
          configurator,
          executor.submit(() -> manager.get().getConfigurator(context)).get(5, TimeUnit.SECONDS));
      assertSame(configurator, manager.get().getConfigurator(context));
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void retriesAfterInitializationFailure() {
    IDatabricksConnectionContext context = context("retry");
    ClientConfigurator configurator = mock(ClientConfigurator.class);
    RuntimeException original = new RuntimeException("temporary failure");
    AtomicInteger attempts = new AtomicInteger();
    DatabricksClientConfiguratorManager manager =
        new DatabricksClientConfiguratorManager(
            ignored -> {
              if (attempts.incrementAndGet() == 1) {
                throw original;
              }
              return configurator;
            });

    DatabricksDriverException error =
        assertThrows(DatabricksDriverException.class, () -> manager.getConfigurator(context));
    assertSame(original, error.getCause());
    assertNull(manager.getConfiguratorOnlyIfExists(context));
    assertSame(configurator, manager.getConfigurator(context));
    assertEquals(2, attempts.get());
  }

  @Test
  void preservesDriverExceptionWithoutReexportingTelemetry() {
    IDatabricksConnectionContext context = context("driver-error");
    try (MockedStatic<TelemetryHelper> telemetry = Mockito.mockStatic(TelemetryHelper.class)) {
      DatabricksDriverException original =
          new DatabricksDriverException(
              "invalid credentials", DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
      telemetry.clearInvocations();
      DatabricksClientConfiguratorManager manager =
          new DatabricksClientConfiguratorManager(
              ignored -> {
                throw original;
              });

      assertSame(
          original,
          assertThrows(DatabricksDriverException.class, () -> manager.getConfigurator(context)));
      telemetry.verifyNoInteractions();
    }
  }

  @Test
  void preservesValidationCauseAndErrorName() {
    IDatabricksConnectionContext context = context("validation-error");
    try (MockedStatic<TelemetryHelper> telemetry = Mockito.mockStatic(TelemetryHelper.class)) {
      DatabricksValidationException original =
          new DatabricksValidationException("invalid credentials", 1015);
      telemetry.clearInvocations();
      DatabricksClientConfiguratorManager manager =
          new DatabricksClientConfiguratorManager(
              ignored -> {
                throw original;
              });

      DatabricksDriverException error =
          assertThrows(DatabricksDriverException.class, () -> manager.getConfigurator(context));
      assertSame(original, error.getCause());
      assertEquals("HY000", original.getSQLState());
      assertEquals(1015, original.getErrorCode());
      telemetry.verify(
          () ->
              TelemetryHelper.exportFailureLog(
                  Mockito.any(),
                  Mockito.eq("INPUT_VALIDATION_ERROR"),
                  Mockito.eq(error.getMessage()),
                  Mockito.eq(TelemetryLogLevel.ERROR)));
      telemetry.verifyNoMoreInteractions();
    }
  }

  @Test
  void sharesInitializationFailureWithWaitingCaller() throws Exception {
    IDatabricksConnectionContext context = context("shared-failure");
    RuntimeException original = new RuntimeException("temporary failure");
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch secondStarted = new CountDownLatch(1);
    AtomicReference<Thread> secondThread = new AtomicReference<>();
    DatabricksClientConfiguratorManager manager =
        new DatabricksClientConfiguratorManager(
            ignored -> {
              started.countDown();
              await(release);
              throw original;
            });
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<ClientConfigurator> first = executor.submit(() -> manager.getConfigurator(context));
      assertTrue(started.await(5, TimeUnit.SECONDS));
      Future<ClientConfigurator> second =
          executor.submit(
              () -> {
                secondThread.set(Thread.currentThread());
                secondStarted.countDown();
                return manager.getConfigurator(context);
              });
      assertTrue(secondStarted.await(5, TimeUnit.SECONDS));
      awaitJoin(secondThread.get());

      release.countDown();
      Throwable firstError =
          assertThrows(ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS)).getCause();
      Throwable secondError =
          assertThrows(ExecutionException.class, () -> second.get(5, TimeUnit.SECONDS)).getCause();
      assertSame(firstError, secondError);
      assertSame(original, firstError.getCause());
      assertNull(manager.getConfiguratorOnlyIfExists(context));
    } finally {
      release.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void closesRemovedConfiguratorWhenInitializationCompletes() throws Exception {
    IDatabricksConnectionContext context = context("removed");
    ClientConfigurator removed = mock(ClientConfigurator.class);
    ClientConfigurator replacement = mock(ClientConfigurator.class);
    AtomicInteger attempts = new AtomicInteger();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    DatabricksClientConfiguratorManager manager =
        new DatabricksClientConfiguratorManager(
            ignored -> {
              if (attempts.incrementAndGet() == 1) {
                started.countDown();
                await(release);
                return removed;
              }
              return replacement;
            });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<ClientConfigurator> first = executor.submit(() -> manager.getConfigurator(context));
      assertTrue(started.await(5, TimeUnit.SECONDS));
      manager.removeInstance(context);
      verify(removed, never()).close();
      assertNull(manager.getConfiguratorOnlyIfExists(context));
      assertSame(replacement, manager.getConfigurator(context));

      release.countDown();
      assertSame(removed, first.get(5, TimeUnit.SECONDS));
      verify(removed).close();
      verify(replacement, never()).close();
      assertSame(replacement, manager.getConfiguratorOnlyIfExists(context));
    } finally {
      release.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void failedRemovedInitializationDoesNotEvictReplacement() throws Exception {
    IDatabricksConnectionContext context = context("removed-failure");
    ClientConfigurator replacement = mock(ClientConfigurator.class);
    AtomicInteger attempts = new AtomicInteger();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    DatabricksClientConfiguratorManager manager =
        new DatabricksClientConfiguratorManager(
            ignored -> {
              if (attempts.incrementAndGet() == 1) {
                started.countDown();
                await(release);
                throw new RuntimeException("temporary failure");
              }
              return replacement;
            });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<ClientConfigurator> first = executor.submit(() -> manager.getConfigurator(context));
      assertTrue(started.await(5, TimeUnit.SECONDS));
      manager.removeInstance(context);
      assertSame(replacement, manager.getConfigurator(context));
      release.countDown();
      assertThrows(ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS));
      assertSame(replacement, manager.getConfiguratorOnlyIfExists(context));
      verify(replacement, never()).close();
    } finally {
      release.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static void awaitJoin(Thread thread) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (System.nanoTime() < deadline) {
      if (thread.getState() == Thread.State.WAITING
          && Arrays.stream(thread.getStackTrace())
              .anyMatch(
                  frame ->
                      frame.getClassName().equals("java.util.concurrent.CompletableFuture")
                          && frame.getMethodName().equals("join"))) {
        return;
      }
      Thread.yield();
    }
    throw new AssertionError("Second caller did not wait for configurator initialization");
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
