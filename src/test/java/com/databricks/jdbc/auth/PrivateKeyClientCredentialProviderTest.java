package com.databricks.jdbc.auth;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PrivateKeyClientCredentialProviderTest {
  @Mock DatabricksHttpClient httpClient;

  @Mock CloseableHttpResponse httpResponse;

  @Mock StatusLine statusLine;

  @Mock DatabricksConfig config;

  @Mock HttpEntity entity;

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
  void testCredentialProviderWithDiscoveryMode() throws DatabricksHttpException, IOException {
    setup();
    try (MockedStatic<DatabricksHttpClient> mocked = mockStatic(DatabricksHttpClient.class)) {
      mocked.when(() -> DatabricksHttpClient.getInstance(any())).thenReturn(httpClient);
      when(httpClient.execute(any())).thenReturn(httpResponse);
      when(httpResponse.getStatusLine()).thenReturn(statusLine);
      when(context.getTokenEndpoint()).thenReturn(null);
      when(statusLine.getStatusCode()).thenReturn(200);
      when(httpResponse.getEntity()).thenReturn(entity);
      when(entity.getContent())
          .thenReturn(
              new ByteArrayInputStream(TEST_OIDC_RESPONSE.getBytes()),
              new ByteArrayInputStream(TEST_OAUTH_RESPONSE.getBytes()));
      when(context.isOAuthDiscoveryModeEnabled()).thenReturn(true);
      when(context.getOAuthDiscoveryURL()).thenReturn(TEST_DISCOVERY_URL);
      PrivateKeyClientCredentialProvider customM2MClientCredentialProvider =
          new PrivateKeyClientCredentialProvider(context);
      JwtPrivateKeyClientCredentials clientCredentials =
          customM2MClientCredentialProvider.getClientCredentialObject(config);
      assertEquals(clientCredentials.getTokenEndpoint(), TEST_TOKEN_URL);
    }
  }

  @Test
  void testCredentialProviderWithModeEnabledButUrlNotProvided() {
    try (MockedStatic<DatabricksHttpClient> mocked = mockStatic(DatabricksHttpClient.class)) {
      mocked.when(() -> DatabricksHttpClient.getInstance(any())).thenReturn(httpClient);
      when(context.isOAuthDiscoveryModeEnabled()).thenReturn(true);
      when(context.getOAuthDiscoveryURL()).thenReturn(null);
      when(context.getTokenEndpoint()).thenReturn(null);
      assertThrows(
          DatabricksException.class,
          () -> new PrivateKeyClientCredentialProvider(context).getClientCredentialObject(config));
    }
  }

  @Test
  void testCredentialProviderWithTokenEndpointInContext() {
    setup();
    try (MockedStatic<DatabricksHttpClient> mocked = mockStatic(DatabricksHttpClient.class)) {
      mocked.when(() -> DatabricksHttpClient.getInstance(any())).thenReturn(httpClient);
      when(context.getTokenEndpoint()).thenReturn(TEST_TOKEN_URL);
      JwtPrivateKeyClientCredentials clientCredentialObject =
          new PrivateKeyClientCredentialProvider(context).getClientCredentialObject(config);
      assertEquals(clientCredentialObject.getTokenEndpoint(), TEST_TOKEN_URL);
    }
  }
}
