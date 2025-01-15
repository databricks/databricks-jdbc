package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.telemetry.SqlExecutionEvent;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.ProxyConfig;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryHelperTest {
  @Mock IDatabricksConnectionContext connectionContext;

  @Test
  void testInitialTelemetryLogDoesNotThrowError() {
    when(connectionContext.getConnectionUuid()).thenReturn(UUID.randomUUID().toString());
    when(connectionContext.getUseProxy()).thenReturn(true);
    when(connectionContext.getProxyAuthType()).thenReturn(ProxyConfig.ProxyAuthType.BASIC);
    when(connectionContext.getProxyPort()).thenReturn(443);
    when(connectionContext.getProxyHost()).thenReturn(TEST_STRING);
    when(connectionContext.getUseCloudFetchProxy()).thenReturn(true);
    when(connectionContext.getCloudFetchProxyAuthType())
        .thenReturn(ProxyConfig.ProxyAuthType.BASIC);
    when(connectionContext.getCloudFetchProxyPort()).thenReturn(443);
    when(connectionContext.getCloudFetchProxyHost()).thenReturn(TEST_STRING);
    assertDoesNotThrow(() -> TelemetryHelper.exportInitialTelemetryLog(connectionContext));
  }

  @Test
  void testInitialTelemetryLogWithNullContextDoesNotThrowError() {
    TelemetryHelper telemetryHelper = new TelemetryHelper(); // To cover the constructor too
    assertDoesNotThrow(() -> telemetryHelper.exportInitialTelemetryLog(null));
  }

  @Test
  void testHostFetchThrowsErrorInTelemetryLog() throws DatabricksParsingException {
    when(connectionContext.getConnectionUuid()).thenReturn(UUID.randomUUID().toString());
    when(connectionContext.getHostUrl())
        .thenThrow(
            new DatabricksParsingException(TEST_STRING, DatabricksDriverErrorCode.INVALID_STATE));
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
