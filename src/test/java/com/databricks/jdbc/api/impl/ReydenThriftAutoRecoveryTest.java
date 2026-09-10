package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.ReydenWarehouseCache;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.latency.DatabricksMetricsTimedProcessor;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for Reyden Thrift auto-recovery feature.
 *
 * <p>When a driver connects over Thrift to a Reyden (Real-Time SQL) warehouse, the SQL Gateway
 * proxy rejects the OpenSession with SQLSTATE "KP001". Instead of surfacing that as a fatal error,
 * the driver should transparently re-open the session on its own SEA (Statement Execution API)
 * path.
 */
@ExtendWith(MockitoExtension.class)
public class ReydenThriftAutoRecoveryTest {
  private static final String SESSION_ID = "session_id";
  private static final String WAREHOUSE_ID = "warehouse_id";
  private static final String HOST = "sample-host.18.azuredatabricks.net";

  private static final String WAREHOUSE_URL_THRIFT =
      "jdbc:databricks://"
          + HOST
          + ":9999/default;transportMode=http;ssl=1;"
          + "AuthMech=3;httpPath=/sql/1.0/warehouses/"
          + WAREHOUSE_ID;

  private static final String WAREHOUSE_URL_THRIFT_FORCED =
      WAREHOUSE_URL_THRIFT + ";UseThriftClient=1";

  @Mock private DatabricksSdkClient sdkClient;
  @Mock private DatabricksThriftServiceClient thriftClient;

  @BeforeEach
  public void setup() {
    // Clear the process-wide warehouse cache so tests do not leak state into each other.
    ReydenWarehouseCache.getInstance().clearCache();
  }

  /**
   * Test: KP001 detection -> marker -> fallback to SEA succeeds.
   *
   * <p>Real method called: DatabricksSession.open() calls databricksClient.createSession() which
   * throws DatabricksSQLException with SQLSTATE KP001. Stubs: Thrift client throws KP001, SEA
   * client returns success.
   */
  @Test
  public void testReydenKP001Detection_FallbackSucceeds() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(WAREHOUSE_URL_THRIFT, new Properties());
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(
        connectionContext, new HashMap<>());

    ImmutableSessionInfo successSessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();

    // Thrift OpenSession fails with KP001
    DatabricksSQLException kp001Error =
        new DatabricksSQLException(
            "Lakehouse/RT is not supported for Thrift protocol",
            "KP001",
            DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(thriftClient.createSession(any(), any(), any(), any())).thenThrow(kp001Error);

    // SEA succeeds
    when(sdkClient.createSession(eq(WAREHOUSE_COMPUTE), any(), any(), any()))
        .thenReturn(successSessionInfo);

    try (MockedStatic<DatabricksMetricsTimedProcessor> proxyMock =
        Mockito.mockStatic(DatabricksMetricsTimedProcessor.class)) {
      proxyMock
          .when(() -> DatabricksMetricsTimedProcessor.createProxy(any()))
          .thenAnswer(
              invocation -> {
                Object arg = invocation.getArgument(0);
                if (arg instanceof DatabricksSdkClient) {
                  return sdkClient;
                }
                return thriftClient;
              });

      DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);

      // Should not throw — recovery succeeds
      assertDoesNotThrow(session::open);

      // Verify session opened successfully on SEA
      assertTrue(session.isOpen());
      assertEquals(SESSION_ID, session.getSessionId());
      assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());

      // Verify warehouse is marked in cache
      assertTrue(
          ReydenWarehouseCache.getInstance().isReydenWarehouse(HOST.toLowerCase(), WAREHOUSE_ID));
    }
  }

  /**
   * Test: Pre-check uses cache to skip Thrift for known-Reyden warehouse.
   *
   * <p>Real method called: DatabricksSession.open() checks cache and skips Thrift client creation
   * if warehouse is already in cache. Stubs: Cache says warehouse is Reyden, SEA client returns
   * success.
   */
  @Test
  public void testReydenPreCheck_SkipsThrift() throws SQLException {
    // Pre-populate cache
    ReydenWarehouseCache.getInstance().markReydenWarehouse(HOST.toLowerCase(), WAREHOUSE_ID);

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(WAREHOUSE_URL_THRIFT, new Properties());
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(
        connectionContext, new HashMap<>());

    ImmutableSessionInfo successSessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();

    // SEA succeeds
    when(sdkClient.createSession(eq(WAREHOUSE_COMPUTE), any(), any(), any()))
        .thenReturn(successSessionInfo);

    try (MockedStatic<DatabricksMetricsTimedProcessor> proxyMock =
        Mockito.mockStatic(DatabricksMetricsTimedProcessor.class)) {
      // Swap only the SEA client for the mock; pass the metadata query client through unchanged
      // so it keeps its IDatabricksMetadataClient type.
      proxyMock
          .when(() -> DatabricksMetricsTimedProcessor.createProxy(any()))
          .thenAnswer(
              invocation -> {
                Object arg = invocation.getArgument(0);
                return (arg instanceof DatabricksSdkClient) ? sdkClient : arg;
              });

      DatabricksSession session = new DatabricksSession(connectionContext);
      // Before open() the client type is still the default THRIFT; the pre-check runs in open().
      assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());

      session.open();

      // The pre-check switched to SEA (Thrift OpenSession was never attempted) and the session
      // opened directly on SEA.
      assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());
      assertTrue(session.isOpen());
      assertEquals(SESSION_ID, session.getSessionId());
    }
  }

  /**
   * Test: the pre-check honors an explicit UseThriftClient=1 and does NOT switch to SEA, even when
   * the warehouse is already known to be Reyden in the cache.
   *
   * <p>Real method called: DatabricksSession.open() with the warehouse pre-marked Reyden. Stubs:
   * Thrift client returns success.
   */
  @Test
  public void testReydenPreCheck_ExplicitThriftNotSwitched() throws SQLException {
    // Warehouse is known Reyden, but the user explicitly forced Thrift.
    ReydenWarehouseCache.getInstance().markReydenWarehouse(HOST.toLowerCase(), WAREHOUSE_ID);

    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(WAREHOUSE_URL_THRIFT_FORCED, new Properties());
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(
        connectionContext, new HashMap<>());

    ImmutableSessionInfo successSessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();
    when(thriftClient.createSession(eq(WAREHOUSE_COMPUTE), any(), any(), any()))
        .thenReturn(successSessionInfo);

    DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);
    session.open();

    // The pre-check must NOT have switched to SEA -- explicit Thrift is honored.
    assertEquals(DatabricksClientType.THRIFT, connectionContext.getClientType());
    assertTrue(session.isOpen());
  }

  /**
   * Test: a Thrift-forcing metadata param (UseQueryForMetadata=0) still counts as the default path
   * (UseThriftClient unset), so a KP001 recovers to SEA. Exercises the metadata-override warn.
   */
  @Test
  public void testReydenRecovery_WithThriftMetadataParam_StillRecovers() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(
            WAREHOUSE_URL_THRIFT + ";UseQueryForMetadata=0", new Properties());
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(
        connectionContext, new HashMap<>());

    ImmutableSessionInfo successSessionInfo =
        ImmutableSessionInfo.builder()
            .sessionId(SESSION_ID)
            .computeResource(WAREHOUSE_COMPUTE)
            .build();

    DatabricksSQLException kp001Error =
        new DatabricksSQLException(
            "Lakehouse/RT is not supported for Thrift protocol",
            "KP001",
            DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(thriftClient.createSession(any(), any(), any(), any())).thenThrow(kp001Error);
    when(sdkClient.createSession(eq(WAREHOUSE_COMPUTE), any(), any(), any()))
        .thenReturn(successSessionInfo);

    try (MockedStatic<DatabricksMetricsTimedProcessor> proxyMock =
        Mockito.mockStatic(DatabricksMetricsTimedProcessor.class)) {
      // Swap only the SEA client for the mock; pass other proxied objects through unchanged.
      proxyMock
          .when(() -> DatabricksMetricsTimedProcessor.createProxy(any()))
          .thenAnswer(
              invocation -> {
                Object arg = invocation.getArgument(0);
                return (arg instanceof DatabricksSdkClient) ? sdkClient : arg;
              });

      DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);
      assertDoesNotThrow(session::open);

      // Recovery proceeds despite the metadata-forcing param (it is not a protocol choice).
      assertTrue(session.isOpen());
      assertEquals(DatabricksClientType.SEA, connectionContext.getClientType());
    }
  }

  /**
   * Test: Explicit UseThriftClient=1 is honored — no fallback on KP001.
   *
   * <p>Real method called: DatabricksSession.open() is called. If UseThriftClient=1 is explicit,
   * recovery does NOT happen even on KP001. Stubs: Thrift client throws KP001.
   */
  @Test
  public void testReydenExplicitThriftForced_NoFallback() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(WAREHOUSE_URL_THRIFT_FORCED, new Properties());
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(
        connectionContext, new HashMap<>());

    // Thrift OpenSession fails with KP001
    DatabricksSQLException kp001Error =
        new DatabricksSQLException(
            "Lakehouse/RT is not supported for Thrift protocol",
            "KP001",
            DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(thriftClient.createSession(any(), any(), any(), any())).thenThrow(kp001Error);

    DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);

    // Should throw KP001 because user explicitly forced Thrift
    DatabricksSQLException thrown = assertThrows(DatabricksSQLException.class, session::open);
    assertEquals("KP001", thrown.getSQLState());
    assertFalse(session.isOpen());
  }

  /**
   * Test: Double-failure preserves both errors in chain.
   *
   * <p>Real method called: DatabricksSession.open() fails on Thrift with KP001, attempts SEA
   * recovery, SEA also fails. Both errors should be chained. Stubs: Both Thrift and SEA throw
   * errors.
   */
  @Test
  public void testReydenDoubleFailure_PreservesBothErrors() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(WAREHOUSE_URL_THRIFT, new Properties());
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(
        connectionContext, new HashMap<>());

    // Thrift fails with KP001
    DatabricksSQLException kp001Error =
        new DatabricksSQLException(
            "Lakehouse/RT is not supported for Thrift protocol",
            "KP001",
            DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(thriftClient.createSession(any(), any(), any(), any())).thenThrow(kp001Error);

    // SEA also fails
    DatabricksSQLException seaError =
        new DatabricksSQLException(
            "SEA connection failed", "08001", DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(sdkClient.createSession(eq(WAREHOUSE_COMPUTE), any(), any(), any())).thenThrow(seaError);

    try (MockedStatic<DatabricksMetricsTimedProcessor> proxyMock =
        Mockito.mockStatic(DatabricksMetricsTimedProcessor.class)) {
      proxyMock
          .when(() -> DatabricksMetricsTimedProcessor.createProxy(any()))
          .thenAnswer(
              invocation -> {
                Object arg = invocation.getArgument(0);
                if (arg instanceof DatabricksSdkClient) {
                  return sdkClient;
                }
                return thriftClient;
              });

      DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);

      // Should throw the SEA error but with KP001 in the cause chain
      DatabricksSQLException thrown = assertThrows(DatabricksSQLException.class, session::open);
      assertTrue(thrown.getMessage().contains("SEA fallback also failed"));

      // Original KP001 should be in the suppressed or cause chain
      assertNotNull(thrown.getCause());
    }
  }

  /**
   * Test: Non-KP001 errors are NOT caught by recovery logic.
   *
   * <p>Real method called: DatabricksSession.open() with Thrift client. Stubs: Thrift throws a
   * different SQLSTATE error.
   */
  @Test
  public void testReydenNonKP001Error_PropagatesToCaller() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContext.parse(WAREHOUSE_URL_THRIFT, new Properties());
    DatabricksDriverFeatureFlagsContextFactory.setFeatureFlagsContext(
        connectionContext, new HashMap<>());

    // Thrift fails with a different error (not KP001)
    DatabricksSQLException otherError =
        new DatabricksSQLException(
            "Some other error", "42000", DatabricksDriverErrorCode.CONNECTION_ERROR);
    when(thriftClient.createSession(any(), any(), any(), any())).thenThrow(otherError);

    DatabricksSession session = new DatabricksSession(connectionContext, thriftClient);

    // Should throw the original error without attempting recovery
    DatabricksSQLException thrown = assertThrows(DatabricksSQLException.class, session::open);
    assertEquals("42000", thrown.getSQLState());
    assertFalse(session.isOpen());
  }

  /**
   * Test: Host case-insensitivity in cache key.
   *
   * <p>Real method called: ReydenWarehouseCache.isReydenWarehouse() with different casing. Stubs:
   * None.
   */
  @Test
  public void testReydenCacheKeyNormalization_HostCaseInsensitive() {
    ReydenWarehouseCache cache = ReydenWarehouseCache.getInstance();

    // Mark with lowercase
    cache.markReydenWarehouse(HOST.toLowerCase(), WAREHOUSE_ID);

    // Check with uppercase — should still be found
    assertTrue(cache.isReydenWarehouse(HOST.toUpperCase(), WAREHOUSE_ID));
    assertTrue(cache.isReydenWarehouse(HOST.toLowerCase(), WAREHOUSE_ID));
  }

  /**
   * Test: Cache key includes both host and warehouse_id.
   *
   * <p>Real method called: ReydenWarehouseCache with different hosts/warehouse IDs. Stubs: None.
   */
  @Test
  public void testReydenCacheKey_HostAndWarehouseIsolation() {
    ReydenWarehouseCache cache = ReydenWarehouseCache.getInstance();

    String host1 = "host1.cloud.databricks.com";
    String host2 = "host2.cloud.databricks.com";
    String warehouse1 = "warehouse1";
    String warehouse2 = "warehouse2";

    // Mark only (host1, warehouse1)
    cache.markReydenWarehouse(host1, warehouse1);

    // Verify isolation
    assertTrue(cache.isReydenWarehouse(host1, warehouse1));
    assertFalse(cache.isReydenWarehouse(host2, warehouse1)); // Different host
    assertFalse(cache.isReydenWarehouse(host1, warehouse2)); // Different warehouse_id
    assertFalse(cache.isReydenWarehouse(host2, warehouse2)); // Both different
  }
}
