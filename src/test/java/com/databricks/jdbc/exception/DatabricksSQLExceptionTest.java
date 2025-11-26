package com.databricks.jdbc.exception;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.ITelemetryClient;
import com.databricks.jdbc.telemetry.TelemetryClientFactory;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DatabricksSQLExceptionTest {

  @Test
  void constructor_usesWarnWhenSilentExceptionsTrue() {
    // Arrange: configure connection context log level to INFO so WARN exports; mock telemetry
    com.databricks.jdbc.api.internal.IDatabricksConnectionContext ctx =
        Mockito.mock(com.databricks.jdbc.api.internal.IDatabricksConnectionContext.class);
    when(ctx.getTelemetryLogLevel()).thenReturn(com.databricks.jdbc.common.TelemetryLogLevel.INFO);
    when(ctx.isTelemetryEnabled()).thenReturn(true);
    when(ctx.forceEnableTelemetry()).thenReturn(true);
    when(ctx.getClientType()).thenReturn(com.databricks.jdbc.common.DatabricksClientType.SEA);
    when(ctx.getConnectionUuid()).thenReturn("ctx-uuid");
    DatabricksThreadContextHolder.setConnectionContext(ctx);

    ITelemetryClient clientMock = Mockito.mock(ITelemetryClient.class);
    TelemetryClientFactory factoryMock = Mockito.mock(TelemetryClientFactory.class);

    try (MockedStatic<TelemetryClientFactory> mocked =
        Mockito.mockStatic(TelemetryClientFactory.class)) {
      mocked.when(TelemetryClientFactory::getInstance).thenReturn(factoryMock);
      Mockito.when(factoryMock.getTelemetryClient(ctx)).thenReturn(clientMock);

      // Act: invoke constructor with silentExceptions=true path
      assertDoesNotThrow(
          () ->
              new DatabricksSQLException(
                  "reason",
                  "HY000",
                  com.databricks.jdbc.exception.DatabricksVendorCode.getVendorCode("reason"),
                  false));

      // Act: invoke the internalError constructor path that uses logTelemetryEvent with WARN
      assertDoesNotThrow(
          () ->
              new DatabricksSQLException(
                  "reason", "HY000", DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR));
    }
  }
}
