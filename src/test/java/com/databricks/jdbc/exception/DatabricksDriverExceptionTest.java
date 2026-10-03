package com.databricks.jdbc.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class DatabricksDriverExceptionTest {
  @Test
  void constructorExportsTelemetryOnce() {
    RuntimeException cause = new RuntimeException("underlying failure");
    try (MockedStatic<TelemetryHelper> telemetry = Mockito.mockStatic(TelemetryHelper.class)) {
      DatabricksDriverException error =
          new DatabricksDriverException("reason", cause, DatabricksDriverErrorCode.AUTH_ERROR);

      assertSame(cause, error.getCause());
      telemetry.verify(
          () ->
              TelemetryHelper.exportFailureLog(
                  Mockito.any(),
                  Mockito.eq("AUTH_ERROR"),
                  Mockito.eq("reason"),
                  Mockito.eq(TelemetryLogLevel.ERROR)));
      telemetry.verifyNoMoreInteractions();
    }
  }

  @Test
  void silentConstructorPreservesCauseWithoutExportingTelemetry() {
    RuntimeException cause = new RuntimeException("underlying failure");
    try (MockedStatic<TelemetryHelper> telemetry = Mockito.mockStatic(TelemetryHelper.class)) {
      DatabricksDriverException error =
          new DatabricksDriverException(
              "reason", cause, DatabricksDriverErrorCode.AUTH_ERROR, true);

      assertEquals("reason", error.getMessage());
      assertSame(cause, error.getCause());
      telemetry.verifyNoInteractions();
    }
  }
}
