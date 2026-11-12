package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.TestConstants.WAREHOUSE_JDBC_URL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.http.conn.HttpClientConnectionManager;
import org.junit.jupiter.api.Test;

public class IdleConnectionReaperTest {

  private static long reaperThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.isAlive() && IdleConnectionReaper.THREAD_NAME.equals(t.getName()))
        .count();
  }

  /** A shut-down executor's thread exits asynchronously, so give stale ones time to go. */
  private static long awaitReaperThreadsAtMost(long max) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (reaperThreads() > max && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    return reaperThreads();
  }

  @Test
  void testRegistrationsShareOneThreadThatStopsWithTheLast() throws Exception {
    // A private reaper, so registrations left open elsewhere in this JVM can't hide the lifecycle.
    List<Thread> threads = new CopyOnWriteArrayList<>();
    IdleConnectionReaper reaper =
        new IdleConnectionReaper(
            runnable -> {
              Thread thread = IdleConnectionReaper.newThread(runnable);
              threads.add(thread);
              return thread;
            });
    List<IdleConnectionReaper.Registration> registrations = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      registrations.add(reaper.add(mock(HttpClientConnectionManager.class), 1, TimeUnit.HOURS));
    }
    assertTrue(reaper.isRunning());
    assertEquals(50, reaper.registrationCount());
    assertEquals(1, threads.size());

    for (IdleConnectionReaper.Registration registration : registrations) {
      assertTrue(registration.isActive());
      registration.close();
      registration.close(); // idempotent
      assertFalse(registration.isActive());
    }
    assertEquals(0, reaper.registrationCount());
    assertFalse(reaper.isRunning());
    threads.get(0).join(10_000);
    assertFalse(threads.get(0).isAlive());

    // A later registration starts a new thread.
    HttpClientConnectionManager manager = mock(HttpClientConnectionManager.class);
    try (IdleConnectionReaper.Registration again = reaper.add(manager, 20, TimeUnit.MILLISECONDS)) {
      assertTrue(reaper.isRunning());
      assertEquals(2, threads.size());
      verify(manager, timeout(5_000).atLeastOnce()).closeExpiredConnections();
    }
    threads.get(1).join(10_000);
    assertFalse(threads.get(1).isAlive());
  }

  @Test
  void testEachManagerIsEvictedWithItsOwnIdleLimit() {
    HttpClientConnectionManager first = mock(HttpClientConnectionManager.class);
    HttpClientConnectionManager second = mock(HttpClientConnectionManager.class);
    try (IdleConnectionReaper.Registration a =
            IdleConnectionReaper.register(first, 30, TimeUnit.MILLISECONDS);
        IdleConnectionReaper.Registration b =
            IdleConnectionReaper.register(second, 50, TimeUnit.MILLISECONDS)) {
      verify(first, timeout(5_000).atLeast(2)).closeExpiredConnections();
      verify(first, timeout(5_000).atLeast(2)).closeIdleConnections(30, TimeUnit.MILLISECONDS);
      verify(second, timeout(5_000).atLeast(2)).closeExpiredConnections();
      verify(second, timeout(5_000).atLeast(2)).closeIdleConnections(50, TimeUnit.MILLISECONDS);
      verify(first, never()).closeIdleConnections(50, TimeUnit.MILLISECONDS);
      verify(second, never()).closeIdleConnections(30, TimeUnit.MILLISECONDS);
    }
  }

  @Test
  void testWithoutIdleLimitOnlyExpiredConnectionsAreClosed() {
    HttpClientConnectionManager manager = mock(HttpClientConnectionManager.class);
    try (IdleConnectionReaper.Registration registration =
        IdleConnectionReaper.register(manager, 0, TimeUnit.SECONDS)) {
      // Same 5-second interval as IdleConnectionEvictor when there is no idle limit.
      verify(manager, timeout(10_000)).closeExpiredConnections();
      verify(manager, never()).closeIdleConnections(anyLong(), any());
    }
  }

  @Test
  void testFailingManagerDoesNotStopEviction() {
    HttpClientConnectionManager failing = mock(HttpClientConnectionManager.class);
    doThrow(new IllegalStateException("pool shut down")).when(failing).closeExpiredConnections();
    HttpClientConnectionManager erroring = mock(HttpClientConnectionManager.class);
    doThrow(new NoClassDefFoundError("missing")).when(erroring).closeExpiredConnections();
    HttpClientConnectionManager healthy = mock(HttpClientConnectionManager.class);
    try (IdleConnectionReaper.Registration a =
            IdleConnectionReaper.register(failing, 20, TimeUnit.MILLISECONDS);
        IdleConnectionReaper.Registration b =
            IdleConnectionReaper.register(erroring, 20, TimeUnit.MILLISECONDS);
        IdleConnectionReaper.Registration c =
            IdleConnectionReaper.register(healthy, 20, TimeUnit.MILLISECONDS)) {
      verify(failing, timeout(5_000).atLeast(3)).closeExpiredConnections();
      verify(erroring, timeout(5_000).atLeast(3)).closeExpiredConnections();
      verify(healthy, timeout(5_000).atLeast(3)).closeIdleConnections(20, TimeUnit.MILLISECONDS);
    }
  }

  @Test
  void testClosingOneHttpClientKeepsEvictionForOthers() throws Exception {
    IDatabricksConnectionContext first =
        DatabricksConnectionContext.parse(
            WAREHOUSE_JDBC_URL + ";IdleHttpConnectionExpiry=30", new Properties());
    IDatabricksConnectionContext second =
        DatabricksConnectionContext.parse(
            WAREHOUSE_JDBC_URL + ";IdleHttpConnectionExpiry=90", new Properties());
    IdleConnectionReaper shared = IdleConnectionReaper.shared();
    int before = shared.registrationCount();
    DatabricksHttpClient firstClient = new DatabricksHttpClient(first, HttpClientType.COMMON);
    DatabricksHttpClient secondClient = new DatabricksHttpClient(second, HttpClientType.COMMON);
    try {
      assertEquals(before + 2, shared.registrationCount());
      assertEquals(1, awaitReaperThreadsAtMost(1));
      firstClient.close();
      assertFalse(firstClient.isIdleConnectionEvictionActive());
      assertTrue(secondClient.isIdleConnectionEvictionActive());
      assertTrue(shared.isRunning());
    } finally {
      firstClient.close();
      secondClient.close();
    }
    assertFalse(secondClient.isIdleConnectionEvictionActive());
    assertEquals(before, shared.registrationCount());
  }

  @Test
  void testThreadDoesNotPinTheStartingCallersContextClassLoader() throws Exception {
    Thread current = Thread.currentThread();
    ClassLoader original = current.getContextClassLoader();
    try (URLClassLoader webapp = new URLClassLoader(new URL[0], original)) {
      current.setContextClassLoader(webapp);
      Thread thread = IdleConnectionReaper.newThread(() -> {});
      assertSame(IdleConnectionReaper.class.getClassLoader(), thread.getContextClassLoader());
      assertTrue(thread.isDaemon());
      assertEquals(IdleConnectionReaper.THREAD_NAME, thread.getName());
    } finally {
      current.setContextClassLoader(original);
    }
  }

  @Test
  void testClosedRegistrationIsNoLongerEvicted() throws Exception {
    HttpClientConnectionManager manager = mock(HttpClientConnectionManager.class);
    IdleConnectionReaper.Registration registration =
        IdleConnectionReaper.register(manager, 20, TimeUnit.MILLISECONDS);
    verify(manager, timeout(5_000).atLeastOnce()).closeExpiredConnections();
    registration.close();
    Thread.sleep(100); // let a run that was already in progress finish
    clearInvocations(manager);
    Thread.sleep(200);
    verifyNoInteractions(manager);
  }
}
