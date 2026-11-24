package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

class TimeoutReviewTest {
  @Test
  void moreThan64SimultaneousTimeoutsAllAttemptCancellation() throws Exception {
    IDatabricksConnectionContext context = context();
    CountDownLatch started = new CountDownLatch(100);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch completed = new CountDownLatch(100);
    ExecutorService callers = Executors.newFixedThreadPool(100);
    CyclicBarrier barrier = new CyclicBarrier(100);
    List<Future<?>> results = new ArrayList<>();
    try {
      for (int index = 0; index < 100; index++) {
        TimeoutHandler handler =
            expired(
                context,
                () -> {
                  started.countDown();
                  try {
                    release.await(10, TimeUnit.SECONDS);
                  } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                  } finally {
                    completed.countDown();
                  }
                });
        results.add(
            callers.submit(
                () -> {
                  barrier.await(10, TimeUnit.SECONDS);
                  assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);
                  return null;
                }));
      }
      assertTrue(started.await(10, TimeUnit.SECONDS));
      release.countDown();
      for (Future<?> result : results) {
        result.get(10, TimeUnit.SECONDS);
      }
      assertEquals(0, completed.getCount(), "Every timeout must attempt its cancel");
    } finally {
      release.countDown();
      callers.shutdownNow();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void executorFailureRunsCancellationInlineAfterExpiredScope(boolean outOfMemory)
      throws Exception {
    Thread caller = Thread.currentThread();
    AtomicInteger completed = new AtomicInteger();
    try (PollingDeadline deadline = new PollingDeadline(50)) {
      assertThrows(InterruptedException.class, () -> Thread.sleep(10000));
    }
    assertFalse(Thread.currentThread().isInterrupted());
    TimeoutHandler handler = expired(null, null);
    handler.awaitCancellation(
        () -> {
          assertSame(caller, Thread.currentThread());
          assertFalse(Thread.currentThread().isInterrupted());
          try (PollingDeadline deadline = new PollingDeadline(TimeUnit.SECONDS.toMillis(30))) {
            try {
              Thread.sleep(100);
              completed.incrementAndGet();
            } catch (InterruptedException failure) {
              throw new AssertionError("Cancel backoff was interrupted", failure);
            }
          }
        },
        task -> {
          if (outOfMemory) {
            throw new OutOfMemoryError("simulated native thread exhaustion");
          }
          throw new RejectedExecutionException("simulated executor rejection");
        });
    assertEquals(1, completed.get());
    assertFalse(Thread.currentThread().isInterrupted());
  }

  @Test
  void cancellationCarriesAndClearsCallerContext() throws Exception {
    IDatabricksConnectionContext context = context();
    AtomicInteger cancelled = new AtomicInteger();
    DatabricksThreadContextHolder.setConnectionContext(context);
    DatabricksThreadContextHolder.setStatementId("statement-async");
    try (MockedStatic<TelemetryHelper> telemetry = mockStatic(TelemetryHelper.class)) {
      TimeoutHandler handler =
          expired(
              context,
              () -> {
                assertSame(context, DatabricksThreadContextHolder.getConnectionContext());
                assertEquals("statement-async", DatabricksThreadContextHolder.getStatementId());
                DatabricksThreadContextHolder.setStatementId("stale-worker-value");
                cancelled.incrementAndGet();
              });
      assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);
      assertEquals(1, cancelled.get());
      assertSame(context, DatabricksThreadContextHolder.getConnectionContext());
      assertEquals("statement-async", DatabricksThreadContextHolder.getStatementId());
      Field executor = TimeoutHandler.class.getDeclaredField("CANCELLATIONS");
      executor.setAccessible(true);
      AtomicInteger cleared = new AtomicInteger();
      handler.awaitCancellation(
          () -> {
            assertNull(DatabricksThreadContextHolder.getConnectionContext());
            assertNull(DatabricksThreadContextHolder.getStatementId());
            assertNull(Thread.currentThread().getContextClassLoader());
            cleared.incrementAndGet();
          },
          (Executor) executor.get(null));
      assertEquals(1, cleared.get());
    } finally {
      DatabricksThreadContextHolder.clearAllContext();
    }
  }

  @Test
  void timeoutIsBuiltAfterPollScopeClosesAndInterruptIsCleared() {
    AtomicInteger cancelled = new AtomicInteger();
    try (MockedStatic<TelemetryHelper> telemetry = mockStatic(TelemetryHelper.class)) {
      telemetry
          .when(() -> TelemetryHelper.exportFailureLog(any(), anyString(), anyString(), any()))
          .thenAnswer(
              call -> {
                assertFalse(PollingDeadline.isCurrentExpired());
                assertFalse(Thread.currentThread().isInterrupted());
                return null;
              });
      TimeoutHandler handler =
          new TimeoutHandler(
              1,
              "scope close",
              () -> {
                assertFalse(Thread.currentThread().isInterrupted());
                try {
                  Thread.sleep(100);
                  cancelled.incrementAndGet();
                } catch (InterruptedException failure) {
                  throw new AssertionError("Cancel backoff was interrupted", failure);
                }
              },
              DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);
      assertThrows(DatabricksTimeoutException.class, () -> handler.sleep(10000));
      assertEquals(1, cancelled.get());
      assertFalse(Thread.currentThread().isInterrupted());
    }
  }

  @Test
  void timeoutDispatchAndTelemetryAreIdempotent() throws Exception {
    Runnable cancel = mock(Runnable.class);
    try (MockedStatic<TelemetryHelper> telemetry = mockStatic(TelemetryHelper.class)) {
      TimeoutHandler handler = expired(null, cancel);
      DatabricksTimeoutException first =
          assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);
      assertSame(first, assertThrows(DatabricksTimeoutException.class, handler::checkTimeout));
      assertSame(
          first,
          assertThrows(
              DatabricksTimeoutException.class, () -> handler.executeWithTimeout(() -> "late")));
      verify(cancel, timeout(1000).times(1)).run();
      telemetry.verify(
          () ->
              TelemetryHelper.exportFailureLog(
                  any(), eq("STATEMENT_EXECUTION_TIMEOUT"), anyString(), any()),
          times(1));
      telemetry.verifyNoMoreInteractions();
    }
  }

  @Test
  void fakeServiceCancellationRunsSynchronously() throws Exception {
    Thread caller = Thread.currentThread();
    AtomicReference<Thread> cancellationThread = new AtomicReference<>();
    try (MockedStatic<DriverUtil> fake = mockStatic(DriverUtil.class)) {
      fake.when(DriverUtil::isRunningAgainstFake).thenReturn(true);
      TimeoutHandler handler = expired(null, () -> cancellationThread.set(Thread.currentThread()));
      assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);
      assertSame(caller, cancellationThread.get());
    }
  }

  @Test
  void terminalPollWithinGraceIsAcceptedButNoNewPollStarts() throws Exception {
    TimeoutHandler handler =
        new TimeoutHandler(1, "grace", null, DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);
    setStart(handler, System.currentTimeMillis() - 800);
    assertEquals(
        "terminal",
        handler.executeWithTimeout(
            () -> {
              Thread.sleep(400);
              return "terminal";
            }));
    AtomicInteger polls = new AtomicInteger();
    assertThrows(
        DatabricksTimeoutException.class, () -> handler.executeWithTimeout(polls::incrementAndGet));
    assertEquals(0, polls.get());
  }

  @Test
  void deadlineUsesMillisecondsWithoutWholeSecondRounding() throws Exception {
    TimeoutHandler handler = expired(null, null);
    setStart(handler, System.currentTimeMillis() - 1300);
    assertThrows(DatabricksTimeoutException.class, handler::checkTimeout);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void pollFailureWithinGraceKeepsOriginalFailure(boolean runtime) throws Exception {
    Exception original =
        runtime
            ? new com.databricks.sdk.core.DatabricksError(
                "UNAUTHENTICATED", "SEA authorization failed", 401)
            : new org.apache.thrift.transport.TTransportException("Thrift poll failed");
    Runnable cancel = mock(Runnable.class);
    TimeoutHandler handler =
        new TimeoutHandler(
            1, "grace failure", cancel, DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);
    setStart(handler, System.currentTimeMillis() - 800);
    Exception observed =
        assertThrows(
            Exception.class,
            () ->
                handler.executeWithTimeout(
                    () -> {
                      Thread.sleep(400);
                      throw original;
                    }));
    assertSame(original, observed);
    verifyNoInteractions(cancel);
    assertFalse(Thread.currentThread().isInterrupted());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void normalPollPreservesCallerInterrupt(boolean interrupted) throws Exception {
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    try {
      TimeoutHandler handler =
          new TimeoutHandler(
              1, "interrupt", null, DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);
      assertEquals("ok", handler.executeWithTimeout(() -> "ok"));
      assertEquals(interrupted, Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void timedOutPollPreservesCallerInterrupt(boolean interrupted) {
    AtomicInteger cancelled = new AtomicInteger();
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    try {
      TimeoutHandler handler =
          new TimeoutHandler(
              1,
              "interrupt",
              () -> {
                assertFalse(Thread.currentThread().isInterrupted());
                cancelled.incrementAndGet();
              },
              DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);
      assertThrows(
          DatabricksTimeoutException.class,
          () ->
              handler.executeWithTimeout(
                  () -> {
                    Thread.interrupted();
                    try {
                      Thread.sleep(12000);
                    } catch (InterruptedException expired) {
                      return "late terminal result";
                    }
                    return null;
                  }));
      assertEquals(1, cancelled.get());
      assertEquals(interrupted, Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void nestedDeadlineAndUnrelatedConnectionReleasePreserveOuterAbort() throws Exception {
    AtomicInteger aborted = new AtomicInteger();
    Object resource = new Object();
    try (PollingDeadline outer = new PollingDeadline(150)) {
      PollingDeadline.registerAbort(resource, aborted::incrementAndGet);
      try (PollingDeadline inner = new PollingDeadline(10000)) {
        assertFalse(PollingDeadline.isCurrentExpired());
      }
      PollingDeadline.clearAbort(new Object());
      assertThrows(InterruptedException.class, () -> Thread.sleep(10000));
      assertTrue(PollingDeadline.isCurrentExpired());
      for (int attempt = 0; attempt < 100 && aborted.get() == 0; attempt++) {
        Thread.sleep(10);
      }
      assertEquals(1, aborted.get());
    }
    assertFalse(Thread.currentThread().isInterrupted());
    assertFalse(PollingDeadline.isCurrentExpired());
  }

  @Test
  void abortRunsOutsideDeadlineMonitorAndTimerDoesNotPinClassLoader() throws Exception {
    CountDownLatch aborted = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    try (PollingDeadline deadline = new PollingDeadline(100)) {
      PollingDeadline.registerAbort(
          new Object(),
          () -> {
            try {
              assertFalse(Thread.holdsLock(deadline));
              assertNull(Thread.currentThread().getContextClassLoader());
            } catch (Throwable problem) {
              failure.set(problem);
            } finally {
              aborted.countDown();
            }
          });
      assertThrows(InterruptedException.class, () -> Thread.sleep(10000));
      assertTrue(aborted.await(5, TimeUnit.SECONDS));
      assertNull(failure.get());
    }
    Field timer = PollingDeadline.class.getDeclaredField("TIMER");
    timer.setAccessible(true);
    ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor) timer.get(null);
    assertTrue(executor.allowsCoreThreadTimeOut());
    assertTrue(executor.getKeepAliveTime(TimeUnit.SECONDS) <= 1);
  }

  private IDatabricksConnectionContext context() {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    return context;
  }

  private TimeoutHandler expired(IDatabricksConnectionContext context, Runnable cancel)
      throws Exception {
    TimeoutHandler handler =
        new TimeoutHandler(
            1,
            "Statement ID: test-statement",
            cancel,
            DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT,
            context);
    setStart(handler, System.currentTimeMillis() - 2000);
    return handler;
  }

  private void setStart(TimeoutHandler handler, long millis) throws Exception {
    Field start = TimeoutHandler.class.getDeclaredField("startTimeMillis");
    start.setAccessible(true);
    start.setLong(handler, millis);
  }
}
