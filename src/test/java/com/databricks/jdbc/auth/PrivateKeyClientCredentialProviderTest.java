package com.databricks.jdbc.auth;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PrivateKeyClientCredentialProviderTest {
  @Mock DatabricksHttpClient httpClient;

  @Mock DatabricksConfig config;

  @Mock IDatabricksConnectionContext context;

  void setup() {
    when(context.getAuthScope()).thenReturn(TEST_SCOPE);
    when(context.getKID()).thenReturn(TEST_JWT_KID);
    when(context.getJWTKeyFile()).thenReturn(TEST_JWT_KEY_FILE);
    when(context.getJWTAlgorithm()).thenReturn(TEST_JWT_ALGORITHM);
    when(context.getJWTPassphrase()).thenReturn(null);
    when(config.getClientId()).thenReturn(TEST_CLIENT_ID);
  }

  @Test
  void testCredentialProviderWithDiscoveryMode() throws IOException {
    setup();
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(config.getOidcEndpoints()).thenReturn(TEST_OIDC_ENDPOINTS);
      PrivateKeyClientCredentialProvider customM2MClientCredentialProvider =
          new PrivateKeyClientCredentialProvider(context, config);
      JwtPrivateKeyClientCredentials clientCredentials =
          customM2MClientCredentialProvider.getClientCredentialObject(config);
      assertEquals(clientCredentials.getTokenEndpoint(), TEST_TOKEN_URL);
    }
  }

  @Test
  void testCredentialProviderWithModeEnabledButUrlNotProvided() throws IOException {
    setup();
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(httpClient);
      when(config.getOidcEndpoints())
          .thenReturn(
              new OpenIDConnectEndpoints(
                  "https://testHost/oidc/v1/token", "https://testHost/oidc/v1/authorize"));
      JwtPrivateKeyClientCredentials clientCredentialObject =
          new PrivateKeyClientCredentialProvider(context, config).getClientCredentialObject(config);
      assertEquals("https://testHost/oidc/v1/token", clientCredentialObject.getTokenEndpoint());
    }
  }

  @Test
  void testCredentialProviderWithTokenEndpointInContext() {
    setup();
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(httpClient);
      when(context.getTokenEndpoint()).thenReturn(TEST_TOKEN_URL);
      JwtPrivateKeyClientCredentials clientCredentialObject =
          new PrivateKeyClientCredentialProvider(context, config).getClientCredentialObject(config);
      assertEquals(clientCredentialObject.getTokenEndpoint(), TEST_TOKEN_URL);
    }
  }
}
