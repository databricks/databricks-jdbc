package com.databricks.jdbc.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mockStatic;

import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import java.sql.SQLFeatureNotSupportedException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DatabricksSQLFeatureNotImplementedExceptionTest {

  @Test
  void followsJdbcContractAndPreservesTelemetry() {
    String reason = "Not implemented";
    DatabricksSQLFeatureNotImplementedException exception;
    try (MockedStatic<TelemetryHelper> telemetryHelper = mockStatic(TelemetryHelper.class)) {
      exception = new DatabricksSQLFeatureNotImplementedException(reason);

      telemetryHelper.verify(
          () ->
              TelemetryHelper.exportFailureLog(
                  DatabricksThreadContextHolder.getConnectionContext(),
                  DatabricksDriverErrorCode.NOT_IMPLEMENTED_OPERATION.name(),
                  reason,
                  TelemetryLogLevel.ERROR));
    }

    assertInstanceOf(SQLFeatureNotSupportedException.class, exception);
    assertEquals(
        DatabricksDriverErrorCode.NOT_IMPLEMENTED_OPERATION.name(), exception.getSQLState());
    assertEquals(0, exception.getErrorCode());
  }
}
