package com.databricks.jdbc.telemetry.latency;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TelemetryCollectorManagerTest {

  private TelemetryCollectorManager manager;
  private IDatabricksConnectionContext context1;
  private IDatabricksConnectionContext context2;

  @BeforeEach
  void setUp() {
    manager = TelemetryCollectorManager.getInstance();
    manager.clear();

    // Create mock contexts with different UUIDs
    context1 = mock(IDatabricksConnectionContext.class);
    when(context1.getConnectionUuid()).thenReturn("connection-uuid-1");

    context2 = mock(IDatabricksConnectionContext.class);
    when(context2.getConnectionUuid()).thenReturn("connection-uuid-2");
  }

  @AfterEach
  void tearDown() {
    manager.clear();
  }

  @Test
  void testGetOrCreateCollectorCreatesNewInstance() {
    TelemetryCollector collector = manager.getOrCreateCollector(context1);
    assertNotNull(collector, "Collector should not be null");
  }

  @Test
  void testGetOrCreateCollectorReturnsSameInstanceForSameConnection() {
    TelemetryCollector collector1 = manager.getOrCreateCollector(context1);
    TelemetryCollector collector2 = manager.getOrCreateCollector(context1);

    assertSame(
        collector1, collector2, "Should return the same instance for the same connection UUID");
  }

  private static Stream<Arguments> provideDifferentConnectionScenarios() {
    IDatabricksConnectionContext ctx1 = mock(IDatabricksConnectionContext.class);
    when(ctx1.getConnectionUuid()).thenReturn("uuid-1");

    IDatabricksConnectionContext ctx2 = mock(IDatabricksConnectionContext.class);
    when(ctx2.getConnectionUuid()).thenReturn("uuid-2");

    IDatabricksConnectionContext ctx3 = mock(IDatabricksConnectionContext.class);
    when(ctx3.getConnectionUuid()).thenReturn("uuid-3");

    return Stream.of(
        Arguments.of("different UUIDs", ctx1, ctx2),
        Arguments.of("another pair of different UUIDs", ctx1, ctx3),
        Arguments.of("third pair of different UUIDs", ctx2, ctx3));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("provideDifferentConnectionScenarios")
  void testGetOrCreateCollectorReturnsDifferentInstancesForDifferentConnections(
      String scenarioName,
      IDatabricksConnectionContext contextA,
      IDatabricksConnectionContext contextB) {
    TelemetryCollector collector1 = manager.getOrCreateCollector(contextA);
    TelemetryCollector collector2 = manager.getOrCreateCollector(contextB);

    assertNotSame(
        collector1, collector2, "Should return different instances for scenario: " + scenarioName);
  }

  @Test
  void testPerConnectionIsolation() {
    TelemetryCollector collector1 = manager.getOrCreateCollector(context1);
    TelemetryCollector collector2 = manager.getOrCreateCollector(context2);

    // Record telemetry for connection 1
    collector1.recordChunkDownloadLatency("statement-1", 0, 100);

    // Record telemetry for connection 2
    collector2.recordChunkDownloadLatency("statement-2", 0, 200);

    // Verify connection 1 has its telemetry
    assertNotNull(
        collector1.getOrCreateTelemetryDetails("statement-1"),
        "Connection 1 should have statement-1");

    // Verify connection 2 has its telemetry
    assertNotNull(
        collector2.getOrCreateTelemetryDetails("statement-2"),
        "Connection 2 should have statement-2");

    // Verify collectors are independent by checking they return different instances
    assertNotSame(
        collector1,
        collector2,
        "Different connections should have completely independent collectors");
  }

  private static Stream<IDatabricksConnectionContext> provideContextsForRemoval() {
    IDatabricksConnectionContext normalContext = mock(IDatabricksConnectionContext.class);
    when(normalContext.getConnectionUuid()).thenReturn("test-uuid");

    IDatabricksConnectionContext nullUuidContext = mock(IDatabricksConnectionContext.class);
    when(nullUuidContext.getConnectionUuid()).thenReturn(null);

    return Stream.of(normalContext, nullUuidContext, null);
  }

  @ParameterizedTest
  @MethodSource("provideContextsForRemoval")
  void testRemoveCollectorReturnsCollector(IDatabricksConnectionContext context) {
    TelemetryCollector collector = manager.getOrCreateCollector(context);
    TelemetryCollector removed = manager.removeCollector(context);

    assertSame(collector, removed, "Should return the removed collector");
  }

  @ParameterizedTest
  @MethodSource("provideContextsForRemoval")
  void testRemoveCollectorRemovesFromManager(IDatabricksConnectionContext context) {
    TelemetryCollector original = manager.getOrCreateCollector(context);
    manager.removeCollector(context);

    // Getting collector again should create a new instance
    TelemetryCollector newCollector = manager.getOrCreateCollector(context);
    assertNotNull(newCollector, "Should create a new collector after removal");
    assertNotSame(
        original, newCollector, "Should be a different instance after removal and re-creation");
  }

  @Test
  void testRemoveCollectorDoesNotAffectOtherConnections() {
    TelemetryCollector collector1 = manager.getOrCreateCollector(context1);
    TelemetryCollector collector2 = manager.getOrCreateCollector(context2);

    // Remove connection 1
    manager.removeCollector(context1);

    // Connection 2 should still have the same collector
    TelemetryCollector stillCollector2 = manager.getOrCreateCollector(context2);
    assertSame(collector2, stillCollector2, "Connection 2's collector should be unaffected");
  }

  @ParameterizedTest
  @MethodSource("provideContextsForRemoval")
  void testRemoveNonExistentCollectorReturnsNull(IDatabricksConnectionContext context) {
    TelemetryCollector removed = manager.removeCollector(context);
    assertNull(removed, "Should return null for non-existent collector");
  }

  private static Stream<Arguments> provideNullContextScenarios() {
    IDatabricksConnectionContext contextWithNullUuid = mock(IDatabricksConnectionContext.class);
    when(contextWithNullUuid.getConnectionUuid()).thenReturn(null);

    return Stream.of(
        Arguments.of("null context and null context", null, null),
        Arguments.of("context with null UUID and null context", contextWithNullUuid, null),
        Arguments.of(
            "context with null UUID and context with null UUID",
            contextWithNullUuid,
            contextWithNullUuid));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("provideNullContextScenarios")
  void testNullContextUsesDefaultConnection(
      String scenarioName,
      IDatabricksConnectionContext context1,
      IDatabricksConnectionContext context2) {
    TelemetryCollector collector1 = manager.getOrCreateCollector(context1);
    TelemetryCollector collector2 = manager.getOrCreateCollector(context2);

    assertSame(
        collector1,
        collector2,
        "Both contexts should use the same default collector for scenario: " + scenarioName);
  }

  @Test
  void testClearRemovesAllCollectors() {
    manager.getOrCreateCollector(context1);
    manager.getOrCreateCollector(context2);

    manager.clear();

    // After clear, getting collectors should create new instances
    TelemetryCollector newCollector1 = manager.getOrCreateCollector(context1);
    TelemetryCollector newCollector2 = manager.getOrCreateCollector(context2);

    assertNotNull(newCollector1);
    assertNotNull(newCollector2);
  }

  @Test
  void testExportAndRemoveOnConnectionClose() {
    TelemetryCollector collector = manager.getOrCreateCollector(context1);

    // Record some telemetry
    StatementId statementId = new StatementId("test-statement-id");
    collector.recordChunkDownloadLatency(statementId.toSQLExecStatementId(), 0, 100);
    collector.recordChunkDownloadLatency(statementId.toSQLExecStatementId(), 1, 150);

    assertNotNull(
        collector.getOrCreateTelemetryDetails(statementId.toSQLExecStatementId()),
        "Should have telemetry details before removal");

    // Simulate connection close: export and remove
    TelemetryCollector removed = manager.removeCollector(context1);
    assertNotNull(removed, "Should return the removed collector");
    removed.exportAllPendingTelemetryDetails();

    // Verify collector was removed from manager
    TelemetryCollector newCollector = manager.getOrCreateCollector(context1);
    assertNotSame(collector, newCollector, "After removal, should get a new collector instance");
  }

  @Test
  void testMultipleConnectionsWithSameHostAreIsolated() {
    // Even if connections are to the same host, they should have separate collectors
    // because they have different connection UUIDs
    IDatabricksConnectionContext conn1 = mock(IDatabricksConnectionContext.class);
    when(conn1.getConnectionUuid()).thenReturn("uuid-1");

    IDatabricksConnectionContext conn2 = mock(IDatabricksConnectionContext.class);
    when(conn2.getConnectionUuid()).thenReturn("uuid-2");

    TelemetryCollector collector1 = manager.getOrCreateCollector(conn1);
    TelemetryCollector collector2 = manager.getOrCreateCollector(conn2);

    assertNotSame(
        collector1,
        collector2,
        "Different connections to same host should have different collectors");

    // Close connection 1
    manager.removeCollector(conn1);

    // Connection 2 should still have its collector
    TelemetryCollector stillCollector2 = manager.getOrCreateCollector(conn2);
    assertSame(
        collector2, stillCollector2, "Connection 2 should not be affected by closing connection 1");
  }
}
