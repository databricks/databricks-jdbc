package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.model.telemetry.DriverMode;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.model.telemetry.TelemetryResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Properties;
import java.util.stream.Stream;

import static com.databricks.jdbc.TestConstants.TEST_BYTES;
import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TelemetryHelperTest {
@Mock
IDatabricksConnectionContext connectionContext;
  private static Stream<Arguments> provideParametersForToDriverMode() {
    return Stream.of(
            Arguments.of(DriverMode.SEA, DatabricksClientType.SQL_EXEC),
            Arguments.of(DriverMode.TYPE_UNSPECIFIED, null),
            Arguments.of(DriverMode.THRIFT, DatabricksClientType.THRIFT));}
  @ParameterizedTest
  @MethodSource("provideParametersForToDriverMode")
void testToDriverMode(DriverMode expectedDriverMode, DatabricksClientType inputClientType){
  assertEquals(TelemetryHelper.toDriverMode(inputClientType),expectedDriverMode);
}

@Test
void testInitialTelemetryLogDoesNotThrowError(){
when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SQL_EXEC);
when(connectionContext.getHttpPath()).thenReturn(TEST_STRING);
assertDoesNotThrow(()->TelemetryHelper.exportInitialTelemetryLog(connectionContext));
}
}
