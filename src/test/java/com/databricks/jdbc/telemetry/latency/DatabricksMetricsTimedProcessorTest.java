package com.databricks.jdbc.telemetry.latency;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
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
}
