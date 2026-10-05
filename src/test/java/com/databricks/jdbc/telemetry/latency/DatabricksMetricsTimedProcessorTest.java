package com.databricks.jdbc.telemetry.latency;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

class DatabricksMetricsTimedProcessorTest {

  @Mock private TelemetryCollector telemetryCollector;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  // Test interfaces and classes
  interface TestInterface {
    @DatabricksMetricsTimed
    String timedMethod(String input);

    String untimedMethod(String input);
  }

  static class TestClass implements TestInterface {
    @Override
    public String timedMethod(String input) {
      return "processed-" + input;
    }

    @Override
    public String untimedMethod(String input) {
      return "unprocessed-" + input;
    }
  }

  static class NoInterfaceClass {
    @DatabricksMetricsTimed
    public String method() {
      return "result";
    }
  }

  static class ExceptionThrowingClass implements TestInterface {
    @Override
    public String timedMethod(String input) {
      throw new RuntimeException("test exception");
    }

    @Override
    public String untimedMethod(String input) {
      throw new RuntimeException("test exception");
    }
  }

  @Test
  void createProxy_WithValidObject_CreatesProxy() {
    TestInterface original = new TestClass();
    TestInterface proxy = DatabricksMetricsTimedProcessor.createProxy(original);

    assertNotNull(proxy);
    assertNotEquals(original.getClass(), proxy.getClass());
    assertEquals("processed-test", proxy.timedMethod("test"));
  }

  @Test
  void createProxy_WithNullObject_ReturnsNull() {
    assertNull(DatabricksMetricsTimedProcessor.createProxy(null));
  }

  @Test
  void createProxy_WithNoInterfaceClass_ReturnsOriginal() {
    NoInterfaceClass original = new NoInterfaceClass();
    NoInterfaceClass proxy = DatabricksMetricsTimedProcessor.createProxy(original);

    assertNotNull(proxy);
    assertEquals(original.getClass(), proxy.getClass());
    assertSame(original, proxy);
  }

  @Test
  void proxy_TimedMethod_RecordsMetrics() {
    TestInterface proxy = DatabricksMetricsTimedProcessor.createProxy(new TestClass());
    String result = proxy.timedMethod("test");

    assertEquals("processed-test", result);
    // Note: We can't easily verify the exact timing, but we can verify the method works
  }

  @Test
  void proxy_UntimedMethod_DoesNotRecordMetrics() {
    TestInterface proxy = DatabricksMetricsTimedProcessor.createProxy(new TestClass());
    String result = proxy.untimedMethod("test");

    assertEquals("unprocessed-test", result);
  }

  @Test
  void proxy_TimedMethodThrowsException_PreservesException() {
    TestInterface proxy = DatabricksMetricsTimedProcessor.createProxy(new ExceptionThrowingClass());

    Exception exception = assertThrows(RuntimeException.class, () -> proxy.timedMethod("test"));
    assertEquals("test exception", exception.getMessage());
  }

  @Test
  void proxy_UntimedMethodThrowsException_PreservesException() {
    DatabricksMetricsTimedProcessor metricsTimedProcessor =
        new DatabricksMetricsTimedProcessor(); // coverage for constructor
    TestInterface proxy = metricsTimedProcessor.createProxy(new ExceptionThrowingClass());

    Exception exception = assertThrows(RuntimeException.class, () -> proxy.untimedMethod("test"));
    assertEquals("test exception", exception.getMessage());
  }

  /**
   * ES-1961329: when two connections are used on one thread, the timed proxy must select the
   * telemetry collector from the target client's own connection context, not from the shared
   * thread-local (which may point at a different connection).
   */
  @Test
  void proxy_TimedMethod_SelectsCollectorFromTargetContextNotThreadLocal() throws Exception {
    IDatabricksConnectionContext seaContext = mock(IDatabricksConnectionContext.class);
    when(seaContext.getConnectionUuid()).thenReturn("uuid-sea");
    IDatabricksConnectionContext thriftContext = mock(IDatabricksConnectionContext.class);
    when(thriftContext.getConnectionUuid()).thenReturn("uuid-thrift");

    // The proxied client belongs to the SEA connection.
    IDatabricksClient seaClient = mock(IDatabricksClient.class);
    when(seaClient.getConnectionContext()).thenReturn(seaContext);
    IDatabricksClient proxy = DatabricksMetricsTimedProcessor.createProxy(seaClient);

    try (MockedStatic<TelemetryCollectorManager> managerStatic =
        mockStatic(TelemetryCollectorManager.class)) {
      TelemetryCollectorManager manager = mock(TelemetryCollectorManager.class);
      managerStatic.when(TelemetryCollectorManager::getInstance).thenReturn(manager);
      when(manager.getCollectorSafely(any())).thenReturn(telemetryCollector);

      // Simulate the bug scenario: the thread-local points at the OTHER (Thrift) connection.
      DatabricksThreadContextHolder.setConnectionContext(thriftContext);
      try {
        proxy.closeStatement(new StatementId("stmt-1"));
      } finally {
        DatabricksThreadContextHolder.clearConnectionContext();
      }

      @SuppressWarnings("unchecked")
      ArgumentCaptor<Supplier<IDatabricksConnectionContext>> contextSupplier =
          ArgumentCaptor.forClass(Supplier.class);
      verify(manager).getCollectorSafely(contextSupplier.capture());
      // Must resolve to the target's own (SEA) context, not the stale thread-local (Thrift).
      assertSame(seaContext, contextSupplier.getValue().get());
      assertNotSame(thriftContext, contextSupplier.getValue().get());
      verify(telemetryCollector).recordOperationLatency(anyLong(), eq("closeStatement"));
    }
  }

  /**
   * ES-1961329: metadata clients implement {@link IDatabricksMetadataClient} (not {@link
   * IDatabricksClient}). Their timed methods must also resolve the collector from the client's own
   * connection context rather than the shared thread-local.
   */
  @Test
  void proxy_TimedMetadataMethod_SelectsCollectorFromTargetContextNotThreadLocal()
      throws Exception {
    IDatabricksConnectionContext seaContext = mock(IDatabricksConnectionContext.class);
    when(seaContext.getConnectionUuid()).thenReturn("uuid-sea");
    IDatabricksConnectionContext thriftContext = mock(IDatabricksConnectionContext.class);
    when(thriftContext.getConnectionUuid()).thenReturn("uuid-thrift");

    IDatabricksMetadataClient metadataClient = mock(IDatabricksMetadataClient.class);
    when(metadataClient.getConnectionContext()).thenReturn(seaContext);
    IDatabricksMetadataClient proxy = DatabricksMetricsTimedProcessor.createProxy(metadataClient);

    try (MockedStatic<TelemetryCollectorManager> managerStatic =
        mockStatic(TelemetryCollectorManager.class)) {
      TelemetryCollectorManager manager = mock(TelemetryCollectorManager.class);
      managerStatic.when(TelemetryCollectorManager::getInstance).thenReturn(manager);
      when(manager.getCollectorSafely(any())).thenReturn(telemetryCollector);

      DatabricksThreadContextHolder.setConnectionContext(thriftContext);
      try {
        proxy.listCatalogs(null);
      } finally {
        DatabricksThreadContextHolder.clearConnectionContext();
      }

      @SuppressWarnings("unchecked")
      ArgumentCaptor<Supplier<IDatabricksConnectionContext>> contextSupplier =
          ArgumentCaptor.forClass(Supplier.class);
      verify(manager).getCollectorSafely(contextSupplier.capture());
      assertSame(seaContext, contextSupplier.getValue().get());
      assertNotSame(thriftContext, contextSupplier.getValue().get());
      verify(telemetryCollector).recordOperationLatency(anyLong(), eq("listCatalogs"));
    }
  }
}
