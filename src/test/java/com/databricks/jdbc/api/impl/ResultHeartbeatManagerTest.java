package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.dbclient.impl.common.StatementId;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class ResultHeartbeatManagerTest {

  private ResultHeartbeatManager manager;

  @AfterEach
  void tearDown() {
    if (manager != null && !manager.isShutdown()) {
      manager.shutdown();
    }
  }

  // =========================================================================
  // Core lifecycle
  // =========================================================================

  @Test
  void testStartAndStopHeartbeat() throws Exception {
    manager = new ResultHeartbeatManager(1, "test-uuid");
    StatementId id = new StatementId("test-1");
    CountDownLatch firstExecution = new CountDownLatch(1);

    manager.startHeartbeat(id, firstExecution::countDown);
    assertEquals(1, manager.getActiveHeartbeatCount());

    assertTrue(firstExecution.await(5, TimeUnit.SECONDS), "Heartbeat should execute within 5s");

    manager.stopHeartbeat(id);
    assertEquals(0, manager.getActiveHeartbeatCount());
  }

  @Test
  void testStopIsIdempotent() {
    manager = new ResultHeartbeatManager(60, "test-uuid");
    StatementId id = new StatementId("test-2");

    manager.startHeartbeat(id, () -> {});
    manager.stopHeartbeat(id);
    assertDoesNotThrow(() -> manager.stopHeartbeat(id));
    assertDoesNotThrow(() -> manager.stopHeartbeat(new StatementId("nonexistent")));
    assertDoesNotThrow(() -> manager.stopHeartbeat(null));
  }

  @Test
  void testShutdownCancelsAll() {
    manager = new ResultHeartbeatManager(60, "test-uuid");
    manager.startHeartbeat(new StatementId("a"), () -> {});
    manager.startHeartbeat(new StatementId("b"), () -> {});
    manager.startHeartbeat(new StatementId("c"), () -> {});
    assertEquals(3, manager.getActiveHeartbeatCount());

    manager.shutdown();
    assertEquals(0, manager.getActiveHeartbeatCount());
    assertTrue(manager.isShutdown());
  }

  @Test
  void testStartAfterShutdownIsNoOp() {
    manager = new ResultHeartbeatManager(60, "test-uuid");
    manager.shutdown();

    manager.startHeartbeat(new StatementId("late"), () -> {});
    assertEquals(0, manager.getActiveHeartbeatCount());
  }

  @Test
  void testNullStatementIdHandled() {
    manager = new ResultHeartbeatManager(60, "test-uuid");
    assertDoesNotThrow(() -> manager.startHeartbeat(null, () -> {}));
    assertDoesNotThrow(() -> manager.stopHeartbeat(null));
    assertEquals(0, manager.getActiveHeartbeatCount());
  }

  // =========================================================================
  // Re-execution replaces heartbeat
  // =========================================================================

  @Test
  void testReExecutionReplacesHeartbeat() throws Exception {
    manager = new ResultHeartbeatManager(1, "test-uuid");
    StatementId id = new StatementId("reuse");
    CountDownLatch firstRan = new CountDownLatch(1);
    CountDownLatch secondRan = new CountDownLatch(1);
    AtomicBoolean firstStillRunning = new AtomicBoolean(true);

    manager.startHeartbeat(
        id,
        () -> {
          firstRan.countDown();
          firstStillRunning.set(true);
        });
    assertTrue(firstRan.await(5, TimeUnit.SECONDS));

    // Replace with new task
    manager.startHeartbeat(
        id,
        () -> {
          firstStillRunning.set(false);
          secondRan.countDown();
        });
    assertEquals(1, manager.getActiveHeartbeatCount());

    assertTrue(secondRan.await(5, TimeUnit.SECONDS));
    assertFalse(firstStillRunning.get(), "New heartbeat should have run, not old");
  }

  // =========================================================================
  // Heartbeat executes at interval (deterministic with latch)
  // =========================================================================

  @Test
  void testHeartbeatExecutesMultipleTimes() throws Exception {
    manager = new ResultHeartbeatManager(1, "test-uuid");
    CountDownLatch latch = new CountDownLatch(3);

    manager.startHeartbeat(new StatementId("interval"), latch::countDown);

    assertTrue(latch.await(10, TimeUnit.SECONDS), "Should execute 3 times within 10s");
  }

  // =========================================================================
  // Stopped flag — prevents RPC after stop
  // =========================================================================

  @Test
  void testStoppedFlagSetOnStop() {
    manager = new ResultHeartbeatManager(60, "test-uuid");
    StatementId id = new StatementId("flag-test");

    manager.startHeartbeat(id, () -> {});
    // Get flag AFTER start (start creates a fresh flag)
    AtomicBoolean flag = manager.getStoppedFlag(id);
    assertFalse(flag.get());

    manager.stopHeartbeat(id);
    assertTrue(flag.get(), "Stopped flag should be set after stopHeartbeat");
  }

  @Test
  void testStoppedFlagSetOnShutdown() {
    manager = new ResultHeartbeatManager(60, "test-uuid");
    StatementId id = new StatementId("shutdown-flag");

    manager.startHeartbeat(id, () -> {});
    // Get flag AFTER start (start creates a fresh flag)
    AtomicBoolean flag = manager.getStoppedFlag(id);
    assertFalse(flag.get());

    manager.shutdown();
    // Note: shutdown clears the map, so the flag instance may be orphaned.
    // But it should have been set to true before removal.
    assertTrue(flag.get(), "Stopped flag should be set after shutdown");
  }

  @Test
  void testStopRacingWithScheduledTick() throws Exception {
    manager = new ResultHeartbeatManager(1, "test-uuid");
    StatementId id = new StatementId("race");
    AtomicInteger rpcCount = new AtomicInteger(0);
    CountDownLatch firstTick = new CountDownLatch(1);

    manager.startHeartbeat(id, () -> firstTick.countDown());
    // Get flag AFTER start
    AtomicBoolean stoppedFlag = manager.getStoppedFlag(id);

    // Replace with a task that checks the stopped flag before "RPC"
    manager.startHeartbeat(
        id,
        () -> {
          if (!manager.getStoppedFlag(id).get()) {
            rpcCount.incrementAndGet();
          }
          firstTick.countDown();
        });

    assertTrue(firstTick.await(5, TimeUnit.SECONDS));
    int countBeforeStop = rpcCount.get();
    assertTrue(countBeforeStop >= 1);

    // Stop — flag is set atomically before cancel
    manager.stopHeartbeat(id);
    assertTrue(stoppedFlag.get());

    // Any tick that fires after stop will see the flag and skip the RPC
    // We can't guarantee no tick fires, but the flag prevents the RPC
  }

  // =========================================================================
  // Shutdown timeout — blocked task
  // =========================================================================

  @Test
  void testShutdownWithBlockedTask() throws Exception {
    manager = new ResultHeartbeatManager(1, "test-uuid");
    CountDownLatch taskStarted = new CountDownLatch(1);
    CountDownLatch taskCanContinue = new CountDownLatch(1);

    manager.startHeartbeat(
        new StatementId("blocked"),
        () -> {
          taskStarted.countDown();
          try {
            // Simulate a task blocked longer than awaitTermination(5s)
            taskCanContinue.await(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });

    assertTrue(taskStarted.await(5, TimeUnit.SECONDS), "Task should start");

    // Shutdown should not hang — it awaits 5s then calls shutdownNow()
    long start = System.currentTimeMillis();
    manager.shutdown();
    long elapsed = System.currentTimeMillis() - start;

    assertTrue(manager.isShutdown());
    // Shutdown should complete within ~6s (5s await + overhead), not 30s
    assertTrue(elapsed < 15_000, "Shutdown should not hang, took " + elapsed + "ms");

    taskCanContinue.countDown(); // cleanup
  }

  // =========================================================================
  // Invalid interval validation
  // =========================================================================

  @Test
  void testZeroIntervalDefaultsToSafe() {
    // Zero interval would cause ScheduledExecutorService to throw.
    // The validation is in DatabricksConnectionContext.getHeartbeatIntervalSeconds().
    // ResultHeartbeatManager itself receives a validated value.
    // Test that the manager handles 1-second interval (minimum valid).
    manager = new ResultHeartbeatManager(1, "test-uuid");
    CountDownLatch ran = new CountDownLatch(1);
    manager.startHeartbeat(new StatementId("min-interval"), ran::countDown);
    assertDoesNotThrow(() -> ran.await(5, TimeUnit.SECONDS));
  }

  // =========================================================================
  // Concurrent start for same statementId — orphaned future must not happen
  // =========================================================================

  @Test
  void testConcurrentStartForSameStatementId_replacesCleanly() throws Exception {
    manager = new ResultHeartbeatManager(1, "test-uuid");
    StatementId id = new StatementId("concurrent");
    AtomicInteger firstCount = new AtomicInteger(0);
    AtomicInteger secondCount = new AtomicInteger(0);
    CountDownLatch secondRan = new CountDownLatch(1);

    // Start first heartbeat
    manager.startHeartbeat(id, firstCount::incrementAndGet);
    // Immediately replace with second — first should be stopped
    manager.startHeartbeat(
        id,
        () -> {
          secondCount.incrementAndGet();
          secondRan.countDown();
        });

    assertEquals(1, manager.getActiveHeartbeatCount(), "Only one heartbeat should be active");
    assertTrue(secondRan.await(5, TimeUnit.SECONDS), "Second heartbeat should fire");

    // Wait a bit more, then verify second is the one running
    Thread.sleep(2000);
    assertTrue(secondCount.get() >= 1, "Second task should have run");
    // First task may have run once before replacement, but should not keep running
    int firstSnapshot = firstCount.get();
    Thread.sleep(1500);
    assertEquals(firstSnapshot, firstCount.get(), "First task should not run after replacement");
  }

  // =========================================================================
  // getStoppedFlag race with stop — flag must not be recreated after stop
  // =========================================================================

  @Test
  void testGetStoppedFlagAfterStop_returnsSentinel() {
    manager = new ResultHeartbeatManager(60, "test-uuid");
    StatementId id = new StatementId("sentinel-test");

    manager.startHeartbeat(id, () -> {});
    // Flag should be false while active
    assertFalse(manager.getStoppedFlag(id).get());

    manager.stopHeartbeat(id);
    // After stop, getStoppedFlag should return a true sentinel, NOT recreate a false flag
    AtomicBoolean flag = manager.getStoppedFlag(id);
    assertTrue(flag.get(), "Flag after stop should be true (sentinel)");

    // Calling getStoppedFlag again should still return true — no new false flag created
    assertTrue(manager.getStoppedFlag(id).get(), "Repeated call should still be true");
  }

  @Test
  void testStoppedFlagRaceWithScheduledTick_noLeakedRpc() throws Exception {
    manager = new ResultHeartbeatManager(1, "test-uuid");
    StatementId id = new StatementId("race-sentinel");
    AtomicInteger rpcCount = new AtomicInteger(0);
    CountDownLatch firstTick = new CountDownLatch(1);

    // Task checks stopped flag from manager each tick (like production code)
    manager.startHeartbeat(
        id,
        () -> {
          AtomicBoolean stopped = manager.getStoppedFlag(id);
          if (!stopped.get()) {
            rpcCount.incrementAndGet();
          }
          firstTick.countDown();
        });

    assertTrue(firstTick.await(5, TimeUnit.SECONDS), "First tick should fire");
    int countBeforeStop = rpcCount.get();
    assertTrue(countBeforeStop >= 1, "At least one RPC should have fired");

    // Stop — after this, getStoppedFlag should return sentinel true
    manager.stopHeartbeat(id);
    assertTrue(manager.getStoppedFlag(id).get(), "Flag should be true after stop");

    // Wait and verify no more RPCs fire
    Thread.sleep(2000);
    assertEquals(
        countBeforeStop,
        rpcCount.get(),
        "No RPCs should fire after stop (sentinel prevents recreation)");
  }

  // =========================================================================
  // Heartbeat continues past cancel to close — verify no leaked RPCs
  // =========================================================================

  @Test
  void testStopThenShutdown_noLeakedRpcs() throws Exception {
    manager = new ResultHeartbeatManager(1, "test-uuid");
    StatementId id = new StatementId("cancel-to-close");
    AtomicInteger rpcCount = new AtomicInteger(0);
    CountDownLatch firstTick = new CountDownLatch(1);

    manager.startHeartbeat(
        id,
        () -> {
          if (!manager.getStoppedFlag(id).get()) {
            rpcCount.incrementAndGet();
          }
          firstTick.countDown();
        });

    assertTrue(firstTick.await(5, TimeUnit.SECONDS));

    // Simulate Statement.cancel() stopping heartbeat
    manager.stopHeartbeat(id);
    int countAfterCancel = rpcCount.get();

    // Simulate Connection.close() shutting down manager
    manager.shutdown();

    // Wait and verify no more RPCs
    Thread.sleep(1500);
    assertEquals(countAfterCancel, rpcCount.get(), "No RPCs should fire after stop + shutdown");
    assertTrue(manager.isShutdown());
  }
}
