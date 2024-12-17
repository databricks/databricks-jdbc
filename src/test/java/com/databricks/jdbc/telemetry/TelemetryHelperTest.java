package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.model.telemetry.DriverMode;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class TelemetryHelperTest {
  @Mock IDatabricksConnectionContext connectionContext;

  private static Stream<Arguments> provideParametersForToDriverMode() {
    return Stream.of(
        Arguments.of(DriverMode.SEA, DatabricksClientType.SQL_EXEC),
        Arguments.of(DriverMode.TYPE_UNSPECIFIED, null),
        Arguments.of(DriverMode.THRIFT, DatabricksClientType.THRIFT));
  }

  @ParameterizedTest
  @MethodSource("provideParametersForToDriverMode")
  void testToDriverMode(DriverMode expectedDriverMode, DatabricksClientType inputClientType) {
    TelemetryHelper telemetryHelper = new TelemetryHelper();
    assertEquals(telemetryHelper.toDriverMode(inputClientType), expectedDriverMode);
  }

  @Test
  void testInitialTelemetryLogDoesNotThrowError() {
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SQL_EXEC);
    when(connectionContext.getHttpPath()).thenReturn(TEST_STRING);
    assertDoesNotThrow(() -> TelemetryHelper.exportInitialTelemetryLog(connectionContext));
  }

  @Test
  void testGetDriverSystemConfigurationDoesNotThrowError() {
    assertDoesNotThrow(TelemetryHelper::getDriverSystemConfiguration);
  }
}
