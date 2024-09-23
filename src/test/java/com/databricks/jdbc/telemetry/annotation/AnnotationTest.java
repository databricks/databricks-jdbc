package com.databricks.jdbc.telemetry.annotation;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.CommandName;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@DatabricksMetricsTimedClass(
    methods = {
      @DatabricksMetricsTimedMethod(
          methodName = "annotatedMethod",
          metricName = CommandName.DEFAULT)
    })
interface TestInterface {
  void annotatedMethod();
}

class TestInterfaceImpl implements TestInterface {
  public void annotatedMethod() {}
}

class NonAnnotatedClass {}

@ExtendWith(MockitoExtension.class)
public class AnnotationTest {
  @Mock private DatabricksMetrics metricsExporter;
  @Mock private TestInterfaceImpl mockImpl;

  @Test
  void testInvoke() {
    TestInterface proxy = DatabricksMetricsTimedProcessor.createProxy(mockImpl, metricsExporter);
    proxy.annotatedMethod();

    verify(mockImpl, times(1)).annotatedMethod();
    verify(metricsExporter, times(1)).record(anyString(), anyDouble());
  }

  @Test
  void testCreateProxyWithNonAnnotatedClassThrowsException() {
    NonAnnotatedClass nonAnnotatedInstance = new NonAnnotatedClass();
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          DatabricksMetricsTimedProcessor.createProxy(nonAnnotatedInstance, metricsExporter);
        });
  }
}
