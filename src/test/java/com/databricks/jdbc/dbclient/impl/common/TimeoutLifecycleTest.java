package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.File;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class TimeoutLifecycleTest {
  @Test
  void cancellationWorkersAreSharedAndExpireWithoutRetainingClosedContext() throws Exception {
    String classpath =
        System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
    Path output = Files.createTempFile("timeout-lifecycle-", ".log");
    Process child = null;
    try {
      child =
          new ProcessBuilder(
                  new File(System.getProperty("java.home"), "bin/java").getPath(),
                  "-cp",
                  classpath,
                  LifecycleChild.class.getName())
              .redirectErrorStream(true)
              .redirectOutput(output.toFile())
              .start();
      boolean exited = child.waitFor(15, TimeUnit.SECONDS);
      String diagnostics = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
      assertTrue(exited, "Lifecycle child did not exit within 15 seconds:\n" + diagnostics);
      assertEquals(0, child.exitValue(), "Lifecycle child failed:\n" + diagnostics);
    } finally {
      try {
        if (child != null && child.isAlive()) {
          child.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
        }
      } finally {
        Files.deleteIfExists(output);
      }
    }
  }

  public static class LifecycleChild {
    public static void main(String[] args) throws Exception {
      Field cancellations = TimeoutHandler.class.getDeclaredField("CANCELLATIONS");
      cancellations.setAccessible(true);
      assertTrue(Modifier.isStatic(cancellations.getModifiers()));
      assertTrue(Modifier.isFinal(cancellations.getModifiers()));
      assertEquals(
          1L,
          Arrays.stream(TimeoutHandler.class.getDeclaredFields())
              .filter(
                  field ->
                      Modifier.isStatic(field.getModifiers())
                          && Executor.class.isAssignableFrom(field.getType()))
              .count());
      ThreadPoolExecutor shared = (ThreadPoolExecutor) cancellations.get(null);
      assertEquals(30L, shared.getKeepAliveTime(TimeUnit.SECONDS));
      ThreadPoolExecutor executor = TimeoutHandler.createCancellationExecutor(1);
      assertEquals(0, executor.getCorePoolSize());
      assertEquals(Integer.MAX_VALUE, executor.getMaximumPoolSize());
      assertTrue(executor.getQueue() instanceof SynchronousQueue);
      assertEquals(1L, executor.getKeepAliveTime(TimeUnit.SECONDS));
      assertEquals(0, executor.getPoolSize());

      WeakReference<IDatabricksConnectionContext> context = runTimeoutsAndClose(executor);
      long collectUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (context.get() != null && System.nanoTime() < collectUntil) {
        System.gc();
        Thread.sleep(50);
      }
      assertNull(context.get(), "Closed connection context was retained by cancellation work");

      long idleUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (executor.getPoolSize() != 0 && System.nanoTime() < idleUntil) {
        Thread.sleep(50);
      }
      assertEquals(0, executor.getPoolSize(), "Idle cancellation worker did not expire");
      assertSame(shared, cancellations.get(null));
      assertFalse(shared.isShutdown());
      executor.shutdownNow();
      System.exit(0);
    }

    private static WeakReference<IDatabricksConnectionContext> runTimeoutsAndClose(
        ThreadPoolExecutor executor) throws Exception {
      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(
              "jdbc:databricks://127.0.0.1:443/default;AuthMech=3;"
                  + "httpPath=/sql/1.0/warehouses/lifecycle",
              new Properties());
      AtomicInteger callbacks = new AtomicInteger();
      AtomicReference<Thread> worker = new AtomicReference<>();
      Field start = TimeoutHandler.class.getDeclaredField("startTimeMillis");
      start.setAccessible(true);
      for (int index = 0; index < 10; index++) {
        TimeoutHandler handler =
            new TimeoutHandler(
                1,
                "lifecycle regression",
                () -> {
                  assertTrue(Thread.currentThread().isDaemon(), "Cancel worker must be a daemon");
                  assertNull(Thread.currentThread().getContextClassLoader());
                  assertSame(context, DatabricksThreadContextHolder.getConnectionContext());
                  worker.set(Thread.currentThread());
                  callbacks.incrementAndGet();
                },
                DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT,
                context,
                executor);
        start.setLong(handler, System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(2));
        try {
          handler.checkTimeout();
          fail("Expected SQLTimeoutException");
        } catch (SQLTimeoutException expected) {
          assertEquals(
              index + 1, callbacks.get(), "Cancel must finish before the timeout is thrown");
        }
        assertTrue(executor.getPoolSize() <= 1, "Sequential cancels must reuse one worker");
        long idleUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while ((executor.getActiveCount() != 0
                || worker.get().getState() != Thread.State.TIMED_WAITING)
            && System.nanoTime() < idleUntil) {
          Thread.sleep(1);
        }
        assertEquals(0, executor.getActiveCount(), "Previous cancellation is still active");
        assertEquals(Thread.State.TIMED_WAITING, worker.get().getState());
      }
      assertEquals(1, executor.getLargestPoolSize());
      DatabricksHttpClientFactory.getInstance().closeConnection(context);
      return new WeakReference<>(context);
    }
  }
}
