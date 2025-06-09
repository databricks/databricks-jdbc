package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.safe.FeatureFlagTestUtil.enableFeatureFlagForTesting;
import static com.databricks.jdbc.telemetry.TelemetryHelper.isTelemetryAllowedForConnection;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.telemetry.SqlExecutionEvent;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.ProxyConfig;
import java.util.Collections;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryHelperTest {
  @Mock IDatabricksConnectionContext connectionContext;

  @Mock DatabricksClientConfiguratorManager mockFactory;

  @Test
  void testInitialTelemetryLogDoesNotThrowError() {
    when(connectionContext.getConnectionUuid()).thenReturn(UUID.randomUUID().toString());
    when(connectionContext.getUseProxy()).thenReturn(true);
    when(connectionContext.getProxyAuthType()).thenReturn(ProxyConfig.ProxyAuthType.BASIC);
    when(connectionContext.getProxyPort()).thenReturn(443);
    when(connectionContext.getProxyHost()).thenReturn(TEST_STRING);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SEA);
    when(connectionContext.getUseCloudFetchProxy()).thenReturn(true);
    when(connectionContext.getCloudFetchProxyAuthType())
        .thenReturn(ProxyConfig.ProxyAuthType.BASIC);
    when(connectionContext.getCloudFetchProxyPort()).thenReturn(443);
    when(connectionContext.getCloudFetchProxyHost()).thenReturn(TEST_STRING);
    assertDoesNotThrow(() -> TelemetryHelper.exportInitialTelemetryLog(connectionContext));
  }

  @Test
  void testInitialTelemetryLogWithNullContextDoesNotThrowError() {
    assertDoesNotThrow(() -> TelemetryHelper.exportInitialTelemetryLog(null));
  }

  @Test
  void testHostFetchThrowsErrorInTelemetryLog() throws DatabricksParsingException {
    when(connectionContext.getConnectionUuid()).thenReturn(UUID.randomUUID().toString());
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SEA);
    when(connectionContext.getHostUrl())
        .thenThrow(
            new DatabricksParsingException(TEST_STRING, DatabricksDriverErrorCode.INVALID_STATE));
    assertDoesNotThrow(() -> TelemetryHelper.exportInitialTelemetryLog(connectionContext));
  }

  @Test
  void testLatencyTelemetryLogDoesNotThrowError() {
    TelemetryHelper telemetryHelper = new TelemetryHelper(); // Increasing coverage for class
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING_2);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SEA);
    SqlExecutionEvent event = new SqlExecutionEvent().setDriverStatementType(StatementType.QUERY);
    assertDoesNotThrow(
        () ->
            telemetryHelper.exportLatencyLog(
                connectionContext, 150, event, TEST_STRING, SESSION_ID));
  }

  @Test
  void testLatencyTelemetryLogDoesNotThrowErrorWithNullStatementId() {
    TelemetryHelper telemetryHelper = new TelemetryHelper(); // Increasing coverage for class
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SEA);
    SqlExecutionEvent event = new SqlExecutionEvent().setDriverStatementType(StatementType.QUERY);
    assertDoesNotThrow(
        () -> telemetryHelper.exportLatencyLog(connectionContext, 150, event, null, SESSION_ID));
  }

  @Test
  void testErrorTelemetryLogDoesNotThrowError() {
    when(connectionContext.getConnectionUuid()).thenReturn(TEST_STRING);
    assertDoesNotThrow(
        () -> TelemetryHelper.exportFailureLog(connectionContext, TEST_STRING, TEST_STRING));
  }

  @Test
  void testGetDriverSystemConfigurationDoesNotThrowError() {
    assertDoesNotThrow(TelemetryHelper::getDriverSystemConfiguration);
  }

  @Test
  void testUpdateClientAppName() {
    // Set the client app name to null first to ensure a clean state
    TelemetryHelper.updateClientAppName(null);

    // Test valid app name
    TelemetryHelper.updateClientAppName("TestApplicationName");
    assertEquals(
        "TestApplicationName", TelemetryHelper.getDriverSystemConfiguration().getClientAppName());

    // Test empty app name - should not change the existing value
    TelemetryHelper.updateClientAppName("");
    assertEquals(
        "TestApplicationName", TelemetryHelper.getDriverSystemConfiguration().getClientAppName());

    // Test null app name - should not change the existing value
    TelemetryHelper.updateClientAppName(null);
    assertEquals(
        "TestApplicationName", TelemetryHelper.getDriverSystemConfiguration().getClientAppName());
  }

  @Test
  public void testGetDatabricksConfigSafely_ReturnsNullOnError() {
    try (MockedStatic<DatabricksClientConfiguratorManager> mockedFactory =
        mockStatic(DatabricksClientConfiguratorManager.class)) {
      mockedFactory.when(DatabricksClientConfiguratorManager::getInstance).thenReturn(mockFactory);
      when(mockFactory.getConfigurator(connectionContext))
          .thenThrow(new RuntimeException("Test error"));
      DatabricksConfig result = TelemetryHelper.getDatabricksConfigSafely(connectionContext);
      assertNull(result, "Should return null when an error occurs");
    }
  }

  @Test
  public void testGetDatabricksConfigSafely_HandlesNullContext() {
    DatabricksConfig result = TelemetryHelper.getDatabricksConfigSafely(null);
    assertNull(result, "Should return null when context is null");
  }

  @Test
  public void testTelemetryNotAllowedUsecase() {
    assertFalse(() -> isTelemetryAllowedForConnection(null));
    when(connectionContext.getComputeResource()).thenReturn(WAREHOUSE_COMPUTE);
    enableFeatureFlagForTesting(connectionContext, Collections.emptyMap());
    assertFalse(() -> isTelemetryAllowedForConnection(connectionContext));
  }
}
