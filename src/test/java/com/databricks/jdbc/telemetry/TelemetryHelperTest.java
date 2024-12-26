package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryHelperTest {
  @Mock IDatabricksConnectionContext connectionContext;

  @Test
  void testInitialTelemetryLogDoesNotThrowError() {
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SEA);
    when(connectionContext.getHttpPath()).thenReturn(TEST_STRING);
    assertDoesNotThrow(() -> TelemetryHelper.exportInitialTelemetryLog(connectionContext));
  }

  @Test
  void testGetDriverSystemConfigurationDoesNotThrowError() {
    assertDoesNotThrow(TelemetryHelper::getDriverSystemConfiguration);
  }
}
