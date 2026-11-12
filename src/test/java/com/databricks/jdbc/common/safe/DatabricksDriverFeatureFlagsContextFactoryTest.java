package com.databricks.jdbc.common.safe;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.core.DatabricksConfig;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DatabricksDriverFeatureFlagsContextFactoryTest {

  @Mock(lenient = true)
  private IDatabricksConnectionContext connectionContext1;

  @Mock(lenient = true)
  private IDatabricksConnectionContext connectionContext2;

  @Mock(lenient = true)
  private IDatabricksComputeResource computeResource1;

  @Mock(lenient = true)
  private IDatabricksComputeResource computeResource2;

  private static final String WORKSPACE_ID_1 = "workspace1";
  private static final String WORKSPACE_ID_2 = "workspace2";

  @BeforeEach
  void setUp() {
    DatabricksDriverFeatureFlagsContext.clearSnapshots();
    // Set up compute resources with different workspace IDs
    when(connectionContext1.getComputeResource()).thenReturn(computeResource1);
    when(connectionContext2.getComputeResource()).thenReturn(computeResource2);
    when(computeResource1.getWorkspaceId()).thenReturn(WORKSPACE_ID_1);
    when(computeResource2.getWorkspaceId()).thenReturn(WORKSPACE_ID_2);
    when(connectionContext1.getHost()).thenReturn("host1.databricks.com");
    when(connectionContext2.getHost()).thenReturn("host2.databricks.com");
    when(connectionContext1.getHostForOAuth()).thenReturn("host1.databricks.com");
    when(connectionContext2.getHostForOAuth()).thenReturn("host2.databricks.com");
    when(connectionContext1.getConnectionUuid()).thenReturn("uuid-1");
    when(connectionContext2.getConnectionUuid()).thenReturn("uuid-2");
  }

  @AfterEach
  void tearDown() {
    // Clean up any contexts created during tests
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext2);
    DatabricksDriverFeatureFlagsContext.clearSnapshots();
  }

  @Test
  void testGetInstanceReturnsSameContextForSameWorkspace() {
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);

    assertSame(context1, context2);
  }

  @Test
  void testGetInstanceCreatesDifferentContextForDifferentWorkspace() {
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext2);

    assertNotSame(context1, context2);
  }

  @Test
  void testMultipleCallsWithSameConnectionAreIdempotent() {
    // Multiple getInstance calls with the same connection UUID are idempotent
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);

    assertSame(
        context1, DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1));

    // A single removeInstance is sufficient to clean up one connection
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);

    // Context is gone; a new one would be created on next getInstance
    DatabricksDriverFeatureFlagsContext newContext =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    // New context was created because the old one was shut down
    // (context1 was already shut down by the single removeInstance above)
    assertNotNull(newContext);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext1);
  }

  @Test
  void testMultipleConnectionsRequireAllToClose() {
    IDatabricksConnectionContext connA =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksConnectionContext connB =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());

    lenient().when(connA.getHost()).thenReturn("shared.databricks.com");
    lenient().when(connB.getHost()).thenReturn("shared.databricks.com");
    lenient().when(connA.getHostForOAuth()).thenReturn("shared.databricks.com");
    lenient().when(connB.getHostForOAuth()).thenReturn("shared.databricks.com");
    lenient().when(connA.getConnectionUuid()).thenReturn("uuid-connA");
    lenient().when(connB.getConnectionUuid()).thenReturn("uuid-connB");

    // Both connections share the same context
    DatabricksDriverFeatureFlagsContext ctxA =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connA);
    DatabricksDriverFeatureFlagsContext ctxB =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connB);
    assertSame(ctxA, ctxB);

    // Closing one connection keeps the context alive
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connA);
    DatabricksDriverFeatureFlagsContext ctxStillAlive =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connB);
    assertSame(ctxA, ctxStillAlive);

    // Closing the last connection removes the context
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connB);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connB); // second remove is no-op
  }

  @Test
  void testConnectionContextUpdatedWhenOwnerConnectionCloses() {
    // This tests the fix for the HTTP client leak:
    // when the connection whose context is stored in the feature flags context closes,
    // the stored context should be updated to another active connection.
    IDatabricksConnectionContext connA =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksConnectionContext connB =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());

    lenient().when(connA.getHost()).thenReturn("shared2.databricks.com");
    lenient().when(connB.getHost()).thenReturn("shared2.databricks.com");
    lenient().when(connA.getHostForOAuth()).thenReturn("shared2.databricks.com");
    lenient().when(connB.getHostForOAuth()).thenReturn("shared2.databricks.com");
    lenient().when(connA.getConnectionUuid()).thenReturn("uuid-ownerA");
    lenient().when(connB.getConnectionUuid()).thenReturn("uuid-secondB");

    // connA creates the context (becomes the "owner")
    DatabricksDriverFeatureFlagsContext ctx =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connA);
    DatabricksDriverFeatureFlagsContextFactory.getInstance(connB);

    assertEquals("uuid-ownerA", ctx.getConnectionContext().getConnectionUuid());

    // When connA closes, the stored context should be updated to connB
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connA);
    assertEquals("uuid-secondB", ctx.getConnectionContext().getConnectionUuid());

    // Clean up
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connB);
  }

  @Test
  void slowInitialFetchDoesNotBlockOtherHostsInSameBin() throws Exception {
    // "Aa" and "BB" have the same String.hashCode, so they always share a ConcurrentHashMap bin.
    IDatabricksConnectionContext slowHost =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksConnectionContext otherHost =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    when(slowHost.getHost()).thenReturn("Aa");
    when(slowHost.getHostForOAuth()).thenReturn("Aa");
    when(slowHost.getConnectionUuid()).thenReturn("uuid-slow");
    when(otherHost.getHost()).thenReturn("BB");
    when(otherHost.getHostForOAuth()).thenReturn("BB");
    when(otherHost.getConnectionUuid()).thenReturn("uuid-other");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(otherHost, new HashMap<>());

    CountDownLatch fetching = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean fetchFinished = new AtomicBoolean();
    DatabricksHttpClientFactory httpClientFactory = mock(DatabricksHttpClientFactory.class);
    when(httpClientFactory.getClient(slowHost))
        .thenAnswer(
            invocation -> {
              fetching.countDown();
              release.await(10, TimeUnit.SECONDS);
              fetchFinished.set(true);
              throw new IllegalStateException("simulated fetch failure");
            });
    ExecutorService other = Executors.newSingleThreadExecutor();
    try (MockedStatic<DatabricksHttpClientFactory> mockedFactory =
        mockStatic(DatabricksHttpClientFactory.class)) {
      mockedFactory.when(DatabricksHttpClientFactory::getInstance).thenReturn(httpClientFactory);
      Future<Boolean> otherReturnedAfterFetch =
          other.submit(
              () -> {
                fetching.await();
                DatabricksDriverFeatureFlagsContextFactory.removeInstance(otherHost);
                boolean finished = fetchFinished.get();
                release.countDown();
                return finished;
              });

      assertNotNull(DatabricksDriverFeatureFlagsContextFactory.getInstance(slowHost));

      assertFalse(
          otherReturnedAfterFetch.get(10, TimeUnit.SECONDS),
          "removeInstance for another host waited for this host's feature flag fetch");
    } finally {
      release.countDown();
      other.shutdownNow();
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(slowHost);
    }
  }

  @Test
  void sameHostWaiterReadsFlagsAfterInitialFetchCompletes() throws Exception {
    when(connectionContext2.getHost()).thenReturn("host1.databricks.com");
    when(connectionContext2.getHostForOAuth()).thenReturn("host1.databricks.com");
    DatabricksDriverFeatureFlagsContext flags = publishUninitializedContext(connectionContext1);
    Field initLock = DatabricksDriverFeatureFlagsContext.class.getDeclaredField("initLock");
    initLock.setAccessible(true);
    Object monitor = initLock.get(flags);
    CountDownLatch fetching = new CountDownLatch(1);
    CountDownLatch waiting = new CountDownLatch(1);
    AtomicReference<Thread> waitingThread = new AtomicReference<>();
    DatabricksConfig config = mock(DatabricksConfig.class);
    when(config.authenticate()).thenReturn(Map.of());
    IDatabricksHttpClient httpClient = mock(IDatabricksHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
    when(response.getEntity())
        .thenReturn(
            new StringEntity("{\"flags\":[{\"name\":\"shared.flag\",\"value\":\"true\"}]}"));
    when(httpClient.execute(any(HttpGet.class)))
        .thenAnswer(
            invocation -> {
              fetching.countDown();
              assertTrue(waiting.await(10, TimeUnit.SECONDS));
              awaitBlockedOn(waitingThread.get(), monitor);
              return response;
            });
    DatabricksHttpClientFactory httpClientFactory = mock(DatabricksHttpClientFactory.class);
    when(httpClientFactory.getClient(any()))
        .thenAnswer(
            invocation -> {
              assertSame(connectionContext1, invocation.getArgument(0));
              return httpClient;
            });
    ExecutorService waiter = Executors.newSingleThreadExecutor();
    try (MockedStatic<DatabricksHttpClientFactory> mockedFactory =
            mockStatic(DatabricksHttpClientFactory.class);
        MockedConstruction<ClientConfigurator> mocked =
            mockConstruction(
                ClientConfigurator.class,
                (mock, ctx) -> when(mock.getDatabricksConfig()).thenReturn(config))) {
      mockedFactory.when(DatabricksHttpClientFactory::getInstance).thenReturn(httpClientFactory);
      Future<Boolean> firstFlagRead =
          waiter.submit(
              () -> {
                assertTrue(fetching.await(10, TimeUnit.SECONDS));
                try (MockedStatic<DatabricksHttpClientFactory> waiterFactory =
                    mockStatic(DatabricksHttpClientFactory.class)) {
                  waiterFactory
                      .when(DatabricksHttpClientFactory::getInstance)
                      .thenReturn(httpClientFactory);
                  waitingThread.set(Thread.currentThread());
                  waiting.countDown();
                  return DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext2)
                      .getBoolean("shared.flag");
                }
              });

      assertSame(flags, DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1));

      assertTrue(flags.getBoolean("shared.flag"));
      assertTrue(firstFlagRead.get(10, TimeUnit.SECONDS));
      verify(httpClient, times(1)).execute(any(HttpGet.class));
      verify(httpClientFactory, never()).getClient(connectionContext2);
    } finally {
      waiter.shutdownNow();
      assertTrue(waiter.awaitTermination(10, TimeUnit.SECONDS));
      DatabricksClientConfiguratorManager.getInstance().removeInstance(connectionContext1);
    }
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
    fail("Caller did not wait on the feature-flag initialization lock");
  }

  @Test
  void initialFetchUsesCallerWithoutWaitingForCreatorsConfigurator() throws Exception {
    when(connectionContext2.getHost()).thenReturn("host1.databricks.com");
    when(connectionContext2.getHostForOAuth()).thenReturn("host1.databricks.com");
    // Simulate the creator being preempted after publishing the holder, before initialization.
    DatabricksDriverFeatureFlagsContext flags = publishUninitializedContext(connectionContext1);

    DatabricksClientConfiguratorManager manager = DatabricksClientConfiguratorManager.getInstance();
    CountDownLatch creating = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean creationFinished = new AtomicBoolean();
    ExecutorService creator = Executors.newSingleThreadExecutor();
    Future<?> creatorResult =
        creator.submit(
            () -> {
              try (MockedConstruction<ClientConfigurator> mocked =
                  mockConstruction(
                      ClientConfigurator.class,
                      (mock, ctx) -> {
                        creating.countDown();
                        release.await(10, TimeUnit.SECONDS);
                        creationFinished.set(true);
                      })) {
                manager.getConfigurator(connectionContext1);
              }
            });
    DatabricksConfig config = mock(DatabricksConfig.class);
    when(config.authenticate()).thenReturn(Map.of());
    IDatabricksHttpClient httpClient = mock(IDatabricksHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
    when(response.getEntity())
        .thenReturn(
            new StringEntity("{\"flags\":[{\"name\":\"caller.flag\",\"value\":\"true\"}]}"));
    when(httpClient.execute(any(HttpGet.class))).thenReturn(response);
    DatabricksHttpClientFactory httpClientFactory = mock(DatabricksHttpClientFactory.class);
    when(httpClientFactory.getClient(any())).thenReturn(httpClient);
    try (MockedStatic<DatabricksHttpClientFactory> mockedFactory =
            mockStatic(DatabricksHttpClientFactory.class);
        MockedConstruction<ClientConfigurator> mocked =
            mockConstruction(
                ClientConfigurator.class,
                (mock, ctx) -> when(mock.getDatabricksConfig()).thenReturn(config))) {
      mockedFactory.when(DatabricksHttpClientFactory::getInstance).thenReturn(httpClientFactory);
      assertTrue(creating.await(10, TimeUnit.SECONDS));

      assertSame(flags, DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext2));

      assertFalse(creationFinished.get(), "The caller waited for the creator's configurator");
      assertTrue(flags.getBoolean("caller.flag"));
      verify(httpClientFactory).getClient(connectionContext2);
      assertSame(connectionContext1, flags.getConnectionContext());
    } finally {
      release.countDown();
      creatorResult.get(10, TimeUnit.SECONDS);
      creator.shutdownNow();
      manager.removeInstance(connectionContext1);
      manager.removeInstance(connectionContext2);
    }
  }

  @Test
  void lookupDuringConfiguratorCreationDefersInitialFetchUntilNextCaller() throws Exception {
    when(connectionContext2.getHost()).thenReturn("host1.databricks.com");
    when(connectionContext2.getHostForOAuth()).thenReturn("host1.databricks.com");
    DatabricksDriverFeatureFlagsContext flags = publishUninitializedContext(connectionContext1);
    DatabricksClientConfiguratorManager manager = DatabricksClientConfiguratorManager.getInstance();
    DatabricksConfig config = mock(DatabricksConfig.class);
    when(config.authenticate()).thenReturn(Map.of());
    IDatabricksHttpClient httpClient = mock(IDatabricksHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
    when(response.getEntity())
        .thenReturn(
            new StringEntity("{\"flags\":[{\"name\":\"caller.flag\",\"value\":\"true\"}]}"));
    when(httpClient.execute(any(HttpGet.class))).thenReturn(response);
    DatabricksHttpClientFactory httpClientFactory = mock(DatabricksHttpClientFactory.class);
    when(httpClientFactory.getClient(any())).thenReturn(httpClient);
    AtomicReference<DatabricksDriverFeatureFlagsContext> reentrantContext = new AtomicReference<>();
    try (MockedStatic<DatabricksHttpClientFactory> mockedFactory =
            mockStatic(DatabricksHttpClientFactory.class);
        MockedConstruction<ClientConfigurator> mocked =
            mockConstruction(
                ClientConfigurator.class,
                (mock, ctx) -> {
                  if (ctx.arguments().get(0) == connectionContext2) {
                    reentrantContext.set(
                        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext2));
                  } else {
                    when(mock.getDatabricksConfig()).thenReturn(config);
                  }
                })) {
      mockedFactory.when(DatabricksHttpClientFactory::getInstance).thenReturn(httpClientFactory);

      manager.getConfigurator(connectionContext2);

      assertSame(flags, reentrantContext.get());
      assertFalse(flags.getBoolean("caller.flag"));
      assertSame(flags, DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1));
      assertTrue(flags.getBoolean("caller.flag"));
      verify(httpClientFactory).getClient(connectionContext1);
      verify(httpClientFactory, never()).getClient(connectionContext2);
    } finally {
      manager.removeInstance(connectionContext1);
      manager.removeInstance(connectionContext2);
    }
  }

  @SuppressWarnings("unchecked")
  private DatabricksDriverFeatureFlagsContext publishUninitializedContext(
      IDatabricksConnectionContext owner) throws Exception {
    DatabricksDriverFeatureFlagsContext flags =
        DatabricksDriverFeatureFlagsContext.createUninitialized(owner);
    Field contextMap =
        DatabricksDriverFeatureFlagsContextFactory.class.getDeclaredField("contextMap");
    contextMap.setAccessible(true);
    ((Map<String, FeatureFlagsContextHolder>) contextMap.get(null))
        .put(TelemetryHelper.keyOf(owner), new FeatureFlagsContextHolder(flags, owner));
    return flags;
  }

  @Test
  void reentrantGetInstanceDuringInitialFetchDoesNotFetchAgain() {
    IDatabricksConnectionContext ctx =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    when(ctx.getHost()).thenReturn("reentrant.databricks.com");
    when(ctx.getHostForOAuth()).thenReturn("reentrant.databricks.com");
    when(ctx.getConnectionUuid()).thenReturn("uuid-reentrant");
    AtomicInteger fetches = new AtomicInteger();
    AtomicReference<DatabricksDriverFeatureFlagsContext> reentrantContext = new AtomicReference<>();
    AtomicReference<Exception> reentrantError = new AtomicReference<>();
    DatabricksHttpClientFactory httpClientFactory = mock(DatabricksHttpClientFactory.class);
    // A failed fetch exports telemetry, which looks up feature flags again on the same thread.
    when(httpClientFactory.getClient(ctx))
        .thenAnswer(
            invocation -> {
              fetches.incrementAndGet();
              try {
                reentrantContext.set(DatabricksDriverFeatureFlagsContextFactory.getInstance(ctx));
              } catch (Exception e) {
                reentrantError.set(e);
              }
              throw new IllegalStateException("simulated fetch failure");
            });
    try (MockedStatic<DatabricksHttpClientFactory> mockedFactory =
        mockStatic(DatabricksHttpClientFactory.class)) {
      mockedFactory.when(DatabricksHttpClientFactory::getInstance).thenReturn(httpClientFactory);

      DatabricksDriverFeatureFlagsContext context =
          DatabricksDriverFeatureFlagsContextFactory.getInstance(ctx);

      assertNull(reentrantError.get());
      assertSame(context, reentrantContext.get());
      assertEquals(1, fetches.get());
    } finally {
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(ctx);
    }
  }

  @Test
  void contextCreatedAfterLastCloseReusesUnexpiredSnapshot() throws Exception {
    IDatabricksConnectionContext first = hostContext("snapshot.databricks.com", "uuid-snap-1");
    IDatabricksConnectionContext second = hostContext("snapshot.databricks.com", "uuid-snap-2");
    IDatabricksHttpClient httpClient =
        httpClientReturning(
            flagsResponse(200, "[{\"name\":\"snap.flag\",\"value\":\"true\"}]", 300));
    ScheduledExecutorService scheduler = spy(Executors.newSingleThreadScheduledExecutor());
    try (MockedStatic<DatabricksHttpClientFactory> factory = mockHttpClientFactory(httpClient);
        MockedConstruction<ClientConfigurator> configurators = mockConfigurators()) {
      assertTrue(
          DatabricksDriverFeatureFlagsContextFactory.getInstance(first).getBoolean("snap.flag"));
      // Closing the only connection shuts the context down.
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(first);

      DatabricksDriverFeatureFlagsContext reopened;
      try (MockedStatic<Executors> executors = mockStatic(Executors.class, CALLS_REAL_METHODS)) {
        executors
            .when(() -> Executors.newSingleThreadScheduledExecutor(any(ThreadFactory.class)))
            .thenReturn(scheduler);
        reopened = DatabricksDriverFeatureFlagsContextFactory.getInstance(second);
      }

      assertTrue(reopened.getBoolean("snap.flag"));
      verify(httpClient, times(1)).execute(any(HttpGet.class));
      // The first refresh is due when the snapshot expires (just under 300 s from now), then every
      // server TTL.
      verify(scheduler)
          .scheduleAtFixedRate(
              any(Runnable.class),
              longThat(delay -> delay >= 290 && delay < 300),
              eq(300L),
              eq(TimeUnit.SECONDS));
    } finally {
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(second);
      scheduler.shutdownNow();
      DatabricksClientConfiguratorManager.getInstance().removeInstance(first);
    }
  }

  @Test
  void snapshotIsNotReusedForDifferentOrgIdOrUserAgent() throws Exception {
    String host = "spog.databricks.com";
    IDatabricksConnectionContext orgA = hostContext(host, "uuid-org-a");
    IDatabricksConnectionContext orgB = hostContext(host, "uuid-org-b");
    IDatabricksConnectionContext partnerInOrgA = hostContext(host, "uuid-partner");
    IDatabricksConnectionContext orgAAgain = hostContext(host, "uuid-org-a-again");
    when(orgA.getCustomHeaders()).thenReturn(Map.of("x-databricks-org-id", "111"));
    when(orgB.getCustomHeaders()).thenReturn(Map.of("x-databricks-org-id", "222"));
    when(partnerInOrgA.getCustomHeaders()).thenReturn(Map.of("x-databricks-org-id", "111"));
    when(partnerInOrgA.getCustomerUserAgent()).thenReturn("PowerBI/1.0");
    when(orgAAgain.getCustomHeaders()).thenReturn(Map.of("x-databricks-org-id", "111"));
    IDatabricksHttpClient httpClient =
        httpClientReturning(flagsResponse(200, "[{\"name\":\"f\",\"value\":\"true\"}]", 300));
    IDatabricksConnectionContext[] sequence = {orgA, orgB, partnerInOrgA, orgAAgain};
    try (MockedStatic<DatabricksHttpClientFactory> factory = mockHttpClientFactory(httpClient);
        MockedConstruction<ClientConfigurator> configurators = mockConfigurators()) {
      for (IDatabricksConnectionContext ctx : sequence) {
        DatabricksDriverFeatureFlagsContextFactory.getInstance(ctx);
        DatabricksDriverFeatureFlagsContextFactory.removeInstance(ctx);
      }

      // Only the last connection repeats an earlier request.
      verify(httpClient, times(3)).execute(any(HttpGet.class));
    } finally {
      for (IDatabricksConnectionContext ctx : sequence) {
        DatabricksClientConfiguratorManager.getInstance().removeInstance(ctx);
      }
    }
  }

  @Test
  void failedFetchIsNotReused() throws Exception {
    IDatabricksConnectionContext first = hostContext("failed.databricks.com", "uuid-failed-1");
    IDatabricksConnectionContext second = hostContext("failed.databricks.com", "uuid-failed-2");
    IDatabricksHttpClient httpClient =
        httpClientReturning(
            flagsResponse(503, null, 0),
            flagsResponse(200, "[{\"name\":\"f\",\"value\":\"true\"}]", 300));
    try (MockedStatic<DatabricksHttpClientFactory> factory = mockHttpClientFactory(httpClient);
        MockedConstruction<ClientConfigurator> configurators = mockConfigurators()) {
      assertFalse(DatabricksDriverFeatureFlagsContextFactory.getInstance(first).getBoolean("f"));
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(first);

      assertTrue(DatabricksDriverFeatureFlagsContextFactory.getInstance(second).getBoolean("f"));
      verify(httpClient, times(2)).execute(any(HttpGet.class));
    } finally {
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(second);
      DatabricksClientConfiguratorManager.getInstance().removeInstance(first);
      DatabricksClientConfiguratorManager.getInstance().removeInstance(second);
    }
  }

  @Test
  void expiredSnapshotIsNotReused() throws Exception {
    IDatabricksConnectionContext first = hostContext("expired.databricks.com", "uuid-exp-1");
    IDatabricksConnectionContext second = hostContext("expired.databricks.com", "uuid-exp-2");
    IDatabricksHttpClient httpClient =
        httpClientReturning(
            flagsResponse(200, "[{\"name\":\"f\",\"value\":\"true\"}]", 1),
            flagsResponse(200, "[{\"name\":\"f\",\"value\":\"false\"}]", 300));
    try (MockedStatic<DatabricksHttpClientFactory> factory = mockHttpClientFactory(httpClient);
        MockedConstruction<ClientConfigurator> configurators = mockConfigurators()) {
      assertTrue(DatabricksDriverFeatureFlagsContextFactory.getInstance(first).getBoolean("f"));
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(first);
      Thread.sleep(1_100);

      assertFalse(DatabricksDriverFeatureFlagsContextFactory.getInstance(second).getBoolean("f"));
      verify(httpClient, times(2)).execute(any(HttpGet.class));
    } finally {
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(second);
      DatabricksClientConfiguratorManager.getInstance().removeInstance(first);
      DatabricksClientConfiguratorManager.getInstance().removeInstance(second);
    }
  }

  @Test
  void snapshotLookupFailureFallsBackToFetch() throws Exception {
    IDatabricksConnectionContext ctx = hostContext("lookup.databricks.com", "uuid-lookup");
    // Only the snapshot lookup, which reads the headers first, sees the failure.
    when(ctx.getCustomHeaders())
        .thenThrow(new IllegalStateException("simulated lookup failure"))
        .thenReturn(Map.of());
    IDatabricksHttpClient httpClient =
        httpClientReturning(flagsResponse(200, "[{\"name\":\"f\",\"value\":\"true\"}]", 300));
    try (MockedStatic<DatabricksHttpClientFactory> factory = mockHttpClientFactory(httpClient);
        MockedConstruction<ClientConfigurator> configurators = mockConfigurators()) {
      assertTrue(DatabricksDriverFeatureFlagsContextFactory.getInstance(ctx).getBoolean("f"));
      verify(httpClient, times(1)).execute(any(HttpGet.class));
    } finally {
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(ctx);
      DatabricksClientConfiguratorManager.getInstance().removeInstance(ctx);
    }
  }

  private static IDatabricksConnectionContext hostContext(String host, String uuid) {
    IDatabricksConnectionContext ctx =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    when(ctx.getHost()).thenReturn(host);
    when(ctx.getHostForOAuth()).thenReturn(host);
    when(ctx.getConnectionUuid()).thenReturn(uuid);
    return ctx;
  }

  private static CloseableHttpResponse flagsResponse(int status, String flagsJson, int ttlSeconds)
      throws Exception {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getStatusLine())
        .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, status, "status"));
    if (flagsJson != null) {
      when(response.getEntity())
          .thenReturn(
              new StringEntity("{\"flags\":" + flagsJson + ",\"ttl_seconds\":" + ttlSeconds + "}"));
    }
    return response;
  }

  /** Returns the responses in order; the last one repeats. */
  private static IDatabricksHttpClient httpClientReturning(
      CloseableHttpResponse first, CloseableHttpResponse... rest) throws Exception {
    IDatabricksHttpClient httpClient = mock(IDatabricksHttpClient.class);
    when(httpClient.execute(any(HttpGet.class))).thenReturn(first, rest);
    return httpClient;
  }

  private static MockedStatic<DatabricksHttpClientFactory> mockHttpClientFactory(
      IDatabricksHttpClient httpClient) {
    DatabricksHttpClientFactory httpClientFactory = mock(DatabricksHttpClientFactory.class);
    when(httpClientFactory.getClient(any())).thenReturn(httpClient);
    MockedStatic<DatabricksHttpClientFactory> mocked =
        mockStatic(DatabricksHttpClientFactory.class);
    mocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(httpClientFactory);
    return mocked;
  }

  private static MockedConstruction<ClientConfigurator> mockConfigurators() {
    DatabricksConfig config = mock(DatabricksConfig.class);
    when(config.authenticate()).thenReturn(Map.of());
    return mockConstruction(
        ClientConfigurator.class,
        (mock, ctx) -> when(mock.getDatabricksConfig()).thenReturn(config));
  }

  @Test
  void testRemoveInstanceWithNullContext() {
    // Should not throw exception
    assertDoesNotThrow(() -> DatabricksDriverFeatureFlagsContextFactory.removeInstance(null));
  }

  @Test
  void testContextPersistsUntilLastRemoval() {
    // Create two distinct connections to the same host
    IDatabricksConnectionContext connX =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksConnectionContext connY =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    lenient().when(connX.getHost()).thenReturn("host1.databricks.com");
    lenient().when(connY.getHost()).thenReturn("host1.databricks.com");
    lenient().when(connX.getHostForOAuth()).thenReturn("host1.databricks.com");
    lenient().when(connY.getHostForOAuth()).thenReturn("host1.databricks.com");
    lenient().when(connX.getConnectionUuid()).thenReturn("uuid-X");
    lenient().when(connY.getConnectionUuid()).thenReturn("uuid-Y");

    // Set a feature flag
    Map<String, String> flags = new HashMap<>();
    flags.put("test.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connX, flags);

    DatabricksDriverFeatureFlagsContextFactory.getInstance(connX);
    DatabricksDriverFeatureFlagsContextFactory.getInstance(connY);

    DatabricksDriverFeatureFlagsContext context =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connX);
    assertTrue(context.getBoolean("test.flag"));

    // Remove connX — context should still exist because connY is still open
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connX);

    DatabricksDriverFeatureFlagsContext contextAfterX =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connY);
    assertTrue(contextAfterX.getBoolean("test.flag"));

    // Clean up
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connY);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connY);
  }

  @Test
  void testSetFeatureFlagsContextWorks() {
    Map<String, String> flags = new HashMap<>();
    flags.put("feature1", "true");
    flags.put("feature2", "false");

    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext1, flags);

    DatabricksDriverFeatureFlagsContext context =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);

    assertTrue(context.getBoolean("feature1"));
    assertFalse(context.getBoolean("feature2"));
  }

  @Test
  void testMultipleConnectionsToSameWorkspaceShareFlags() {
    // Create two connection contexts with same workspace ID
    IDatabricksConnectionContext conn1 =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksConnectionContext conn2 =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    IDatabricksComputeResource resource = new Warehouse(WORKSPACE_ID_1);

    lenient().when(conn1.getComputeResource()).thenReturn(resource);
    lenient().when(conn2.getComputeResource()).thenReturn(resource);
    lenient().when(conn1.getHost()).thenReturn("host.databricks.com");
    lenient().when(conn2.getHost()).thenReturn("host.databricks.com");
    lenient().when(conn1.getHostForOAuth()).thenReturn("host.databricks.com");
    lenient().when(conn2.getHostForOAuth()).thenReturn("host.databricks.com");
    lenient().when(conn1.getConnectionUuid()).thenReturn("uuid-conn1");
    lenient().when(conn2.getConnectionUuid()).thenReturn("uuid-conn2");

    // Set flags for first connection
    Map<String, String> flags = new HashMap<>();
    flags.put("shared.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(conn1, flags);

    // Get context for second connection - should share the same flags
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(conn1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(conn2);

    assertSame(context1, context2);
    assertTrue(context2.getBoolean("shared.flag"));

    // Clean up
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(conn1);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(conn2);
  }

  @Test
  void testDifferentWorkspacesHaveIsolatedFlags() {
    // Set different flags for each workspace
    Map<String, String> flags1 = new HashMap<>();
    flags1.put("workspace1.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext1, flags1);

    Map<String, String> flags2 = new HashMap<>();
    flags2.put("workspace2.flag", "true");
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(connectionContext2, flags2);

    // Get contexts
    DatabricksDriverFeatureFlagsContext context1 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext1);
    DatabricksDriverFeatureFlagsContext context2 =
        DatabricksDriverFeatureFlagsContextFactory.getInstance(connectionContext2);

    // Verify flags are isolated
    assertTrue(context1.getBoolean("workspace1.flag"));
    assertFalse(context1.getBoolean("workspace2.flag"));

    assertFalse(context2.getBoolean("workspace1.flag"));
    assertTrue(context2.getBoolean("workspace2.flag"));
  }
}
