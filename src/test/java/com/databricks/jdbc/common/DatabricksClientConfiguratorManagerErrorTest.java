package com.databricks.jdbc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class DatabricksClientConfiguratorManagerErrorTest {
  @Test
  void preservesUnexpectedFailureCause() {
    IDatabricksConnectionContext context = context("unexpected-error");
    RuntimeException original = new RuntimeException("temporary failure");
    DatabricksClientConfiguratorManager manager =
        new DatabricksClientConfiguratorManager(
            ignored -> {
              throw original;
            });

    DatabricksDriverException error =
        assertThrows(DatabricksDriverException.class, () -> manager.getConfigurator(context));
    assertSame(original, error.getCause());
  }

  @Test
  void preservesDriverExceptionWithoutReexportingTelemetry() {
    IDatabricksConnectionContext context = context("driver-error");
    try (MockedStatic<TelemetryHelper> telemetry = Mockito.mockStatic(TelemetryHelper.class)) {
      DatabricksDriverException original =
          new DatabricksDriverException(
              "invalid credentials", DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
      telemetry.clearInvocations();
      DatabricksClientConfiguratorManager manager =
          new DatabricksClientConfiguratorManager(
              ignored -> {
                throw original;
              });

      assertSame(
          original,
          assertThrows(DatabricksDriverException.class, () -> manager.getConfigurator(context)));
      telemetry.verifyNoInteractions();
    }
  }

  @Test
  void preservesValidationCauseAndErrorName() {
    IDatabricksConnectionContext context = context("validation-error");
    try (MockedStatic<TelemetryHelper> telemetry = Mockito.mockStatic(TelemetryHelper.class)) {
      DatabricksValidationException original =
          new DatabricksValidationException(
              "invalid credentials", DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
      telemetry.clearInvocations();
      DatabricksClientConfiguratorManager manager =
          new DatabricksClientConfiguratorManager(
              ignored -> {
                throw original;
              });

      DatabricksDriverException error =
          assertThrows(DatabricksDriverException.class, () -> manager.getConfigurator(context));
      assertSame(original, error.getCause());
      assertEquals("INPUT_VALIDATION_ERROR", original.getSQLState());
      assertEquals(1015, original.getErrorCode());
      telemetry.verify(
          () ->
              TelemetryHelper.exportFailureLog(
                  Mockito.any(),
                  Mockito.eq("INPUT_VALIDATION_ERROR"),
                  Mockito.eq(error.getMessage()),
                  Mockito.eq(TelemetryLogLevel.ERROR)));
      telemetry.verifyNoMoreInteractions();
    }
  }

  private static IDatabricksConnectionContext context(String uuid) {
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getConnectionUuid()).thenReturn(uuid);
    return context;
  }
}
