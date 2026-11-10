package com.databricks.jdbc.common.safe;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
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
  void reentrantGetInstanceDuringInitialFetchDoesNotFetchAgain() {
    IDatabricksConnectionContext ctx =
        mock(IDatabricksConnectionContext.class, withSettings().lenient());
    when(ctx.getHost()).thenReturn("reentrant.databricks.com");
    when(ctx.getHostForOAuth()).thenReturn("reentrant.databricks.com");
    when(ctx.getConnectionUuid()).thenReturn("uuid-reentrant");
    AtomicInteger fetches = new AtomicInteger();
    DatabricksHttpClientFactory httpClientFactory = mock(DatabricksHttpClientFactory.class);
    // A failed fetch exports telemetry, which looks up feature flags again on the same thread.
    when(httpClientFactory.getClient(ctx))
        .thenAnswer(
            invocation -> {
              fetches.incrementAndGet();
              DatabricksDriverFeatureFlagsContextFactory.getInstance(ctx);
              throw new IllegalStateException("simulated fetch failure");
            });
    try (MockedStatic<DatabricksHttpClientFactory> mockedFactory =
        mockStatic(DatabricksHttpClientFactory.class)) {
      mockedFactory.when(DatabricksHttpClientFactory::getInstance).thenReturn(httpClientFactory);

      assertNotNull(DatabricksDriverFeatureFlagsContextFactory.getInstance(ctx));

      assertEquals(1, fetches.get());
    } finally {
      DatabricksDriverFeatureFlagsContextFactory.removeInstance(ctx);
    }
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
