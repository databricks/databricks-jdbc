package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.model.telemetry.SqlExecutionEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryHelperTest {
  @Mock IDatabricksConnectionContext connectionContext;

  @Test
  void testInitialTelemetryLogDoesNotThrowError() {
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    assertDoesNotThrow(() -> TelemetryHelper.exportInitialTelemetryLog(connectionContext));
  }

  @Test
  void testLatencyTelemetryLogDoesNotThrowError() {
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    SqlExecutionEvent event = new SqlExecutionEvent().setDriverStatementType(StatementType.QUERY);
    assertDoesNotThrow(() -> TelemetryHelper.exportLatencyLog(connectionContext, 150, event));
  }

  @Test
  void testErrorTelemetryLogDoesNotThrowError() {
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    SqlExecutionEvent event = new SqlExecutionEvent().setDriverStatementType(StatementType.QUERY);
    assertDoesNotThrow(
        () -> TelemetryHelper.exportFailureLog(connectionContext, TEST_STRING, TEST_STRING));
  }

  @Test
  void testGetDriverSystemConfigurationDoesNotThrowError() {
    assertDoesNotThrow(TelemetryHelper::getDriverSystemConfiguration);
  }
}
