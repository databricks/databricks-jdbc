package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.dbclient.impl.http.ClosedConnectionHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import com.databricks.jdbc.telemetry.latency.TelemetryCollectorManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for GitHub issue #1325: Leaked Socket prevents CRaC checkpointing.
 *
 * <p>Verifies that after Connection.close(), delayed telemetry flush tasks cannot re-create HTTP
 * clients or TelemetryClients that would leak TCP sockets.
 */
@ExtendWith(MockitoExtension.class)
public class TelemetryHttpClientLeakTest {

  @BeforeEach
  public void setUp() {
    TelemetryClientFactory.getInstance().reset();
    TelemetryCollectorManager.getInstance().clear();
    DatabricksHttpClientFactory.getInstance().reset();
  }

  @AfterEach
  public void tearDown() {
    TelemetryClientFactory.getInstance().reset();
    TelemetryCollectorManager.getInstance().clear();
    DatabricksHttpClientFactory.getInstance().reset();
  }

  @Test
  void testGetTelemetryClientAfterCloseReturnsNoop() throws Exception {
    String host = "leak-test-host.databricks.net";
    String uuid = "leak-test-uuid-1";

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);
      IDatabricksConnectionContext ctx = createTelemetryContext(uuid, host);

      // Register and create telemetry client

      ITelemetryClient client = TelemetryClientFactory.getInstance().getTelemetryClient(ctx);
      assertInstanceOf(TelemetryClient.class, client);

      // Close telemetry client
      TelemetryClientFactory.getInstance().closeTelemetryClient(ctx);
      assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());

      // After close, getTelemetryClient must return NoopTelemetryClient
      ITelemetryClient recreated = TelemetryClientFactory.getInstance().getTelemetryClient(ctx);
      assertInstanceOf(
          NoopTelemetryClient.class,
          recreated,
          "getTelemetryClient() after close must return NoopTelemetryClient (issue #1325)");
      assertEquals(0, TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size());
    }
  }

  @Test
  void testGetClientReturnsNullAfterCloseConnection() {
    String uuid = "leak-test-uuid-2";
    IDatabricksConnectionContext ctx = createHttpContext(uuid);

    // Close the connection
    DatabricksHttpClientFactory.getInstance().closeConnection(ctx);

    // After close, getClient should return the ClosedConnectionHttpClient sentinel
    assertInstanceOf(
        ClosedConnectionHttpClient.class,
        DatabricksHttpClientFactory.getInstance().getClient(ctx, HttpClientType.TELEMETRY),
        "getClient(TELEMETRY) should return sentinel after closeConnection (issue #1325)");
    assertInstanceOf(
        ClosedConnectionHttpClient.class,
        DatabricksHttpClientFactory.getInstance().getClient(ctx, HttpClientType.COMMON),
        "getClient(COMMON) should return sentinel after closeConnection");
  }

  @Test
  void testCloseTelemetryClientWithPendingEventsDoesNotReCreate() throws Exception {
    String host = "leak-test-host.databricks.net";
    String uuid = "leak-test-uuid-3";

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);
      IDatabricksConnectionContext ctx = createTelemetryContext(uuid, host);

      // Register and create telemetry client

      TelemetryClientFactory.getInstance().getTelemetryClient(ctx);

      // Record pending events
      TelemetryCollector collector =
          TelemetryCollectorManager.getInstance().getOrCreateCollector(ctx);
      collector.recordChunkDownloadLatency("stmt-1", 0, 100L);

      // Mock exportTelemetryLog to call getTelemetryClient(ctx) — simulating the real
      // call chain that triggered the leak before the fix.
      AtomicInteger reCreationCount = new AtomicInteger(0);
      mockedStatic
          .when(() -> TelemetryHelper.exportTelemetryLog(any(), any()))
          .thenAnswer(
              invocation -> {
                int before =
                    TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size();
                TelemetryClientFactory.getInstance().getTelemetryClient(ctx);
                int after =
                    TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size();
                if (after > before) {
                  reCreationCount.incrementAndGet();
                }
                return null;
              });

      // Close — should not re-create the client during export
      TelemetryClientFactory.getInstance().closeTelemetryClient(ctx);

      assertEquals(
          0,
          TelemetryClientFactory.getInstance().noauthTelemetryClientHolders.size(),
          "No holders should remain after close");
      assertEquals(0, reCreationCount.get(), "No TelemetryClient re-creation should have occurred");
    }
  }

  @Test
  void testConcurrentCloseAndGetClientDoesNotLeak() throws Exception {
    String host = "race-test-host.databricks.net";
    String uuid = "race-test-uuid";

    try (MockedStatic<TelemetryHelper> mockedStatic = mockStatic(TelemetryHelper.class)) {
      setupTelemetryHelperMock(mockedStatic);
      stubExportTelemetryLog(mockedStatic);
      IDatabricksConnectionContext ctx = createTelemetryContext(uuid, host);
      when(ctx.getHttpMaxConnectionsPerRoute()).thenReturn(100);

      // Register the telemetry connection (HTTP factory uses tombstones, no registration needed)

      // Create initial clients
      DatabricksHttpClientFactory.getInstance().getClient(ctx, HttpClientType.TELEMETRY);
      TelemetryClientFactory.getInstance().getTelemetryClient(ctx);

      int numThreads = 20;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(numThreads);
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);

      // Half the threads close, half simulate delayed flush (getClient)
      for (int i = 0; i < numThreads; i++) {
        final boolean isCloseThread = (i % 2 == 0);
        executor.submit(
            () -> {
              try {
                startLatch.await();
                if (isCloseThread) {
                  TelemetryClientFactory.getInstance().closeTelemetryClient(ctx);
                  DatabricksHttpClientFactory.getInstance().closeConnection(ctx);
                } else {
                  DatabricksHttpClientFactory.getInstance()
                      .getClient(ctx, HttpClientType.TELEMETRY);
                  TelemetryClientFactory.getInstance().getTelemetryClient(ctx);
                }
              } catch (Exception e) {
                // Expected in race conditions
              } finally {
                doneLatch.countDown();
              }
            });
      }

      startLatch.countDown();
      assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "Threads should complete within timeout");
      executor.shutdown();

      // After close has run, getClient should return the sentinel
      assertInstanceOf(
          ClosedConnectionHttpClient.class,
          DatabricksHttpClientFactory.getInstance().getClient(ctx, HttpClientType.TELEMETRY),
          "getClient(TELEMETRY) must return sentinel after closeConnection()");
      assertInstanceOf(
          NoopTelemetryClient.class,
          TelemetryClientFactory.getInstance().getTelemetryClient(ctx),
          "getTelemetryClient() must return NoopTelemetryClient after close");
    }
  }

  // --- Helper methods ---

  /** Minimal context for HTTP-only tests. */
  private IDatabricksConnectionContext createHttpContext(String uuid) {
    IDatabricksConnectionContext ctx = mock(IDatabricksConnectionContext.class);
    when(ctx.getConnectionUuid()).thenReturn(uuid);
    return ctx;
  }

  /**
   * Context for telemetry tests. Add getHttpMaxConnectionsPerRoute stub if creating HTTP clients.
   */
  private IDatabricksConnectionContext createTelemetryContext(String uuid, String host) {
    IDatabricksConnectionContext ctx = mock(IDatabricksConnectionContext.class);
    when(ctx.getConnectionUuid()).thenReturn(uuid);
    when(ctx.getHost()).thenReturn(host);
    when(ctx.getTelemetryBatchSize()).thenReturn(10);
    when(ctx.getTelemetryFlushIntervalInMilliseconds()).thenReturn(5000);
    return ctx;
  }

  private void setupTelemetryHelperMock(MockedStatic<TelemetryHelper> mockedStatic) {
    mockedStatic.when(() -> TelemetryHelper.keyOf(any())).thenCallRealMethod();
    mockedStatic.when(() -> TelemetryHelper.getDatabricksConfigSafely(any())).thenReturn(null);
    mockedStatic
        .when(() -> TelemetryHelper.removeConnectionParameters(anyString()))
        .thenAnswer(invocation -> null);
    mockedStatic
        .when(() -> TelemetryHelper.isTelemetryAllowedForConnection(any()))
        .thenReturn(true);
  }

  private void stubExportTelemetryLog(MockedStatic<TelemetryHelper> mockedStatic) {
    mockedStatic
        .when(() -> TelemetryHelper.exportTelemetryLog(any(), any()))
        .thenAnswer(invocation -> null);
  }
}
