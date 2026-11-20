package com.databricks.jdbc.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksDriverException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

class DatabricksClientConfiguratorManagerTest {
  // "Aa" and "BB" have the same String.hashCode, so they always share a ConcurrentHashMap bin.
  private static final String UUID_A = "Aa";
  private static final String UUID_B = "BB";

  private final DatabricksClientConfiguratorManager manager =
      DatabricksClientConfiguratorManager.getInstance();
  private IDatabricksConnectionContext contextA;
  private IDatabricksConnectionContext contextB;

  @BeforeEach
  void setUp() {
    contextA = mock(IDatabricksConnectionContext.class);
    contextB = mock(IDatabricksConnectionContext.class);
    when(contextA.getConnectionUuid()).thenReturn(UUID_A);
    when(contextB.getConnectionUuid()).thenReturn(UUID_B);
  }

  @AfterEach
  void tearDown() {
    manager.removeInstance(contextA);
    manager.removeInstance(contextB);
  }

  @Test
  void slowConfiguratorCreationDoesNotBlockOtherConnections() throws Exception {
    assertEquals(UUID_A.hashCode(), UUID_B.hashCode());
    CountDownLatch creating = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean creationFinished = new AtomicBoolean();
    ExecutorService other = Executors.newSingleThreadExecutor();
    try (MockedConstruction<ClientConfigurator> mocked =
        mockConstruction(
            ClientConfigurator.class,
            (mock, ctx) -> {
              creating.countDown();
              release.await(10, TimeUnit.SECONDS);
              creationFinished.set(true);
            })) {
      Future<Boolean> otherReturnedAfterCreation =
          other.submit(
              () -> {
                creating.await();
                manager.removeInstance(contextB);
                boolean finished = creationFinished.get();
                release.countDown();
                return finished;
              });

      ClientConfigurator configurator = manager.getConfigurator(contextA);

      assertSame(mocked.constructed().get(0), configurator);
      assertFalse(
          otherReturnedAfterCreation.get(10, TimeUnit.SECONDS),
          "removeInstance for another connection waited for this connection's configurator");
    } finally {
      release.countDown();
      other.shutdownNow();
    }
  }

  @Test
  void sameConnectionReusesConfigurator() {
    try (MockedConstruction<ClientConfigurator> mocked =
        mockConstruction(ClientConfigurator.class)) {
      ClientConfigurator first = manager.getConfigurator(contextA);

      assertSame(first, manager.getConfigurator(contextA));
      assertSame(first, manager.getConfiguratorOnlyIfExists(contextA));
      assertEquals(1, mocked.constructed().size());
    }
  }

  @Test
  void removeInstanceClosesConfiguratorAndNextCallCreatesNewOne() {
    try (MockedConstruction<ClientConfigurator> mocked =
        mockConstruction(ClientConfigurator.class)) {
      ClientConfigurator first = manager.getConfigurator(contextA);

      manager.removeInstance(contextA);

      verify(first).close();
      assertNull(manager.getConfiguratorOnlyIfExists(contextA));
      ClientConfigurator second = manager.getConfigurator(contextA);
      assertNotSame(first, second);
      assertEquals(2, mocked.constructed().size());
    }
  }

  @Test
  void detachInstanceRemovesNowAndClosesOnlyWhenRun() {
    try (MockedConstruction<ClientConfigurator> mocked =
        mockConstruction(ClientConfigurator.class)) {
      ClientConfigurator first = manager.getConfigurator(contextA);

      Runnable close = manager.detachInstance(contextA);

      assertNull(manager.getConfiguratorOnlyIfExists(contextA));
      verify(first, never()).close();
      close.run();
      verify(first).close();
      assertNull(manager.detachInstance(contextA));
    }
  }

  @Test
  void failedCreationRemovesHolderAndNextCallCanRetry() throws Exception {
    try (MockedConstruction<ClientConfigurator> mocked =
        mockConstruction(
            ClientConfigurator.class,
            (mock, ctx) -> {
              throw new IllegalStateException("Failed configurator creation");
            })) {
      assertThrows(DatabricksDriverException.class, () -> manager.getConfigurator(contextA));
    }

    Field instances = DatabricksClientConfiguratorManager.class.getDeclaredField("instances");
    instances.setAccessible(true);
    assertFalse(((Map<?, ?>) instances.get(manager)).containsKey(UUID_A));

    try (MockedConstruction<ClientConfigurator> mocked =
        mockConstruction(ClientConfigurator.class)) {
      ClientConfigurator configurator = manager.getConfigurator(contextA);
      assertSame(mocked.constructed().get(0), configurator);
      assertSame(configurator, manager.getConfiguratorOnlyIfExists(contextA));
    }
  }

  @Test
  void failedCreationDoesNotRemoveReplacementHolder() {
    ClientConfigurator replacement = mock(ClientConfigurator.class);
    try (MockedConstruction<ClientConfigurator> mocked =
        mockConstruction(
            ClientConfigurator.class,
            (mock, ctx) -> {
              manager.setConfigurator(contextA, replacement);
              throw new IllegalStateException("Failed configurator creation");
            })) {
      assertThrows(DatabricksDriverException.class, () -> manager.getConfigurator(contextA));
    }

    assertSame(replacement, manager.getConfigurator(contextA));
  }

  @Test
  void waitingCallerRetriesAfterAnotherThreadsCreationFails() throws Exception {
    CountDownLatch creating = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch waiting = new CountDownLatch(1);
    AtomicReference<Thread> waitingThread = new AtomicReference<>();
    AtomicReference<ClientConfigurator> failedConfigurator = new AtomicReference<>();
    ExecutorService callers = Executors.newFixedThreadPool(2);
    try {
      Future<?> creator =
          callers.submit(
              () -> {
                try (MockedConstruction<ClientConfigurator> mocked =
                    mockConstruction(
                        ClientConfigurator.class,
                        (mock, ctx) -> {
                          failedConfigurator.set(mock);
                          creating.countDown();
                          assertTrue(release.await(10, TimeUnit.SECONDS));
                          throw new IllegalStateException("Failed configurator creation");
                        })) {
                  assertThrows(
                      DatabricksDriverException.class, () -> manager.getConfigurator(contextA));
                }
              });
      assertTrue(creating.await(10, TimeUnit.SECONDS));
      Object failedHolder = getHolders().get(UUID_A);
      Future<ClientConfigurator> waiter =
          callers.submit(
              () -> {
                try (MockedConstruction<ClientConfigurator> mocked =
                    mockConstruction(ClientConfigurator.class)) {
                  waitingThread.set(Thread.currentThread());
                  waiting.countDown();
                  return manager.getConfigurator(contextA);
                }
              });
      assertTrue(waiting.await(10, TimeUnit.SECONDS));
      awaitBlockedOn(waitingThread.get(), failedHolder);

      release.countDown();
      creator.get(10, TimeUnit.SECONDS);
      ClientConfigurator configurator = waiter.get(10, TimeUnit.SECONDS);

      assertNotNull(configurator);
      assertNotSame(failedConfigurator.get(), configurator);
      assertNotSame(failedHolder, getHolders().get(UUID_A));
      assertSame(configurator, manager.getConfiguratorOnlyIfExists(contextA));
      verify(configurator, never()).close();
    } finally {
      release.countDown();
      callers.shutdownNow();
      assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  void waitingCallerRetriesAfterItsHolderIsRemoved() throws Exception {
    manager.setConfigurator(contextA, null);
    Object removedHolder = getHolders().get(UUID_A);
    CountDownLatch waiting = new CountDownLatch(1);
    AtomicReference<Thread> waitingThread = new AtomicReference<>();
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<ClientConfigurator> waiter;
      synchronized (removedHolder) {
        waiter =
            caller.submit(
                () -> {
                  try (MockedConstruction<ClientConfigurator> mocked =
                      mockConstruction(ClientConfigurator.class)) {
                    waitingThread.set(Thread.currentThread());
                    waiting.countDown();
                    return manager.getConfigurator(contextA);
                  }
                });
        assertTrue(waiting.await(10, TimeUnit.SECONDS));
        awaitBlockedOn(waitingThread.get(), removedHolder);

        manager.removeInstance(contextA);
      }
      ClientConfigurator configurator = waiter.get(10, TimeUnit.SECONDS);

      assertNotNull(configurator);
      assertNotSame(removedHolder, getHolders().get(UUID_A));
      assertSame(configurator, manager.getConfiguratorOnlyIfExists(contextA));
      verify(configurator, never()).close();
    } finally {
      caller.shutdownNow();
      assertTrue(caller.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private Map<?, ?> getHolders() throws Exception {
    Field instances = DatabricksClientConfiguratorManager.class.getDeclaredField("instances");
    instances.setAccessible(true);
    return (Map<?, ?>) instances.get(manager);
  }

  private static void awaitBlockedOn(Thread thread, Object monitor) throws InterruptedException {
    ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      ThreadInfo info = threads.getThreadInfo(thread.getId());
      if (info != null
          && info.getThreadState() == Thread.State.BLOCKED
          && info.getLockInfo() != null
          && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(monitor)) {
        return;
      }
      Thread.sleep(1);
    }
    fail("Caller did not wait on the expected configurator holder");
  }

  @Test
  void reentrantCreationOnSameThreadFailsInsteadOfCreatingTwice() {
    AtomicReference<Exception> reentrantError = new AtomicReference<>();
    try (MockedConstruction<ClientConfigurator> mocked =
        mockConstruction(
            ClientConfigurator.class,
            (mock, ctx) -> {
              try {
                manager.getConfigurator(contextA);
              } catch (Exception e) {
                reentrantError.set(e);
              }
            })) {
      ClientConfigurator configurator = manager.getConfigurator(contextA);

      assertSame(mocked.constructed().get(0), configurator);
      assertEquals(1, mocked.constructed().size());
      assertInstanceOf(DatabricksDriverException.class, reentrantError.get());
      assertTrue(reentrantError.get().getMessage().contains("Recursive client configurator"));
      assertSame(configurator, manager.getConfigurator(contextA));
    }
  }

  @Test
  void configuratorOnlyIfExistsIsNullBeforeCreation() {
    assertNull(manager.getConfiguratorOnlyIfExists(contextA));
  }
}
