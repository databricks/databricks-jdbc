package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.AuthFlow;
import com.databricks.jdbc.common.AuthMech;
import com.databricks.jdbc.common.DatabricksClientType;
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
    when(connectionContext.getHttpPath()).thenReturn(TEST_STRING);
    when(connectionContext.getClientType()).thenReturn(DatabricksClientType.SEA);
    when(connectionContext.getConnectionUuid()).thenReturn(UUID.randomUUID().toString());
    when(connectionContext.getUseProxy()).thenReturn(true);
    when(connectionContext.getAuthMech()).thenReturn(AuthMech.OAUTH);
    when(connectionContext.getProxyHost()).thenReturn("proxy.test.com");
    when(connectionContext.getProxyPort()).thenReturn(8080);
    when(connectionContext.getProxyAuthType()).thenReturn(ProxyConfig.ProxyAuthType.BASIC);
    when(connectionContext.getNonProxyHosts()).thenReturn("localhost|*.example.com");
    when(connectionContext.getUseSystemProxy()).thenReturn(false);
    when(connectionContext.getUseCloudFetchProxy()).thenReturn(true);
    when(connectionContext.getCloudFetchProxyAuthType())
        .thenReturn(ProxyConfig.ProxyAuthType.SPNEGO);
    when(connectionContext.getCloudFetchProxyHost()).thenReturn("cf-proxy.test.com");
    when(connectionContext.getCloudFetchProxyPort()).thenReturn(3128);
    when(connectionContext.getAuthFlow()).thenReturn(AuthFlow.CLIENT_CREDENTIALS);
    when(connectionContext.isOAuthDiscoveryModeEnabled()).thenReturn(false);
    when(connectionContext.getOAuthDiscoveryURL()).thenReturn("https://discovery.test.com");
    when(connectionContext.getSSLTrustStoreType()).thenReturn("PKCS12");
    when(connectionContext.checkCertificateRevocation()).thenReturn(true);
    when(connectionContext.acceptUndeterminedCertificateRevocation()).thenReturn(false);

    assertDoesNotThrow(() -> TelemetryHelper.exportInitialTelemetryLog(connectionContext));
  }

  @Test
  void testGetDriverSystemConfigurationDoesNotThrowError() {
    assertDoesNotThrow(TelemetryHelper::getDriverSystemConfiguration);
  }
}
