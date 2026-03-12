package com.databricks.jdbc.auth;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

  private static Path tempKeyFile;

  @BeforeAll
  public static void generateTestKeyFile() throws Exception {
    tempKeyFile = TestKeyGenerator.generateTemporaryKeyFile();
  }

  @AfterAll
  public static void cleanupTestKeyFile() throws Exception {
    TestKeyGenerator.cleanupKeyFile(tempKeyFile);
  }

  void setup() {
    lenient().when(context.getAuthScope()).thenReturn(TEST_SCOPE);
    lenient().when(context.getKID()).thenReturn(TEST_JWT_KID);
    lenient().when(context.getJWTKeyFile()).thenReturn(tempKeyFile.toString());
    lenient().when(context.getJWTAlgorithm()).thenReturn(TEST_JWT_ALGORITHM);
    lenient().when(context.getJWTPassphrase()).thenReturn(null);
    lenient().when(context.getConnectionUuid()).thenReturn("test-connection-uuid");
    lenient().when(context.getTokenCachePassPhrase()).thenReturn(null);
    lenient().when(context.getHostForOAuth()).thenReturn("https://test.databricks.com");
    lenient().when(config.getClientId()).thenReturn(TEST_CLIENT_ID);
    lenient().when(config.getHost()).thenReturn("https://test.databricks.com");
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

  @Test
  void should_ReturnCorrectAuthType() throws IOException {
    setup();
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(httpClient);
      when(config.getOidcEndpoints()).thenReturn(TEST_OIDC_ENDPOINTS);

      PrivateKeyClientCredentialProvider provider =
          new PrivateKeyClientCredentialProvider(context, config);

      assertEquals("custom-oauth-m2m", provider.authType());
    }
  }

  @Test
  void should_BuildClientCredentialsWithAllParameters() throws IOException {
    setup();
    // Don't set passphrase since our test key file is not encrypted
    when(context.getJWTPassphrase()).thenReturn(null);

    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(httpClient);
      when(config.getOidcEndpoints()).thenReturn(TEST_OIDC_ENDPOINTS);

      PrivateKeyClientCredentialProvider provider =
          new PrivateKeyClientCredentialProvider(context, config);

      JwtPrivateKeyClientCredentials clientCredentials = provider.getClientCredentialObject(config);

      assertEquals(TEST_TOKEN_URL, clientCredentials.getTokenEndpoint());
    }
  }

  @Test
  void should_ConfigureHeaderFactory_AndGenerateHeaders() throws Exception {
    setup();
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(httpClient);
      when(config.getOidcEndpoints()).thenReturn(TEST_OIDC_ENDPOINTS);

      // Mock HTTP response for token generation
      org.apache.http.HttpEntity httpEntity = mock(org.apache.http.HttpEntity.class);
      org.apache.http.client.methods.CloseableHttpResponse httpResponse =
          mock(org.apache.http.client.methods.CloseableHttpResponse.class);
      when(httpClient.execute(any())).thenReturn(httpResponse);
      when(httpResponse.getEntity()).thenReturn(httpEntity);
      when(httpEntity.getContent())
          .thenReturn(new java.io.ByteArrayInputStream(TEST_OAUTH_RESPONSE.getBytes()));

      PrivateKeyClientCredentialProvider provider =
          new PrivateKeyClientCredentialProvider(context, config);

      // Test configure method returns a HeaderFactory
      com.databricks.sdk.core.HeaderFactory headerFactory = provider.configure(config);
      assertNotNull(headerFactory, "HeaderFactory should not be null");

      // Call headers() to test the lambda and increase coverage
      java.util.Map<String, String> headers = headerFactory.headers();
      assertNotNull(headers);
      assertTrue(headers.containsKey(org.apache.http.HttpHeaders.AUTHORIZATION));
      assertTrue(headers.containsKey(org.apache.http.HttpHeaders.CONTENT_TYPE));
      assertEquals(
          "application/x-www-form-urlencoded",
          headers.get(org.apache.http.HttpHeaders.CONTENT_TYPE));
    }
  }

  @Test
  void should_BuildClientCredentialsWithJWTAlgorithm() throws IOException {
    setup();
    when(context.getJWTAlgorithm()).thenReturn("ES256");

    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(httpClient);
      when(config.getOidcEndpoints()).thenReturn(TEST_OIDC_ENDPOINTS);

      PrivateKeyClientCredentialProvider provider =
          new PrivateKeyClientCredentialProvider(context, config);

      JwtPrivateKeyClientCredentials clientCredentials = provider.getClientCredentialObject(config);

      assertEquals(TEST_TOKEN_URL, clientCredentials.getTokenEndpoint());
    }
  }

  @Test
  void should_HandleNullScope() throws IOException {
    setup();
    when(context.getAuthScope()).thenReturn(null);

    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(httpClient);
      when(config.getOidcEndpoints()).thenReturn(TEST_OIDC_ENDPOINTS);

      PrivateKeyClientCredentialProvider provider =
          new PrivateKeyClientCredentialProvider(context, config);

      JwtPrivateKeyClientCredentials clientCredentials = provider.getClientCredentialObject(config);

      assertEquals(TEST_TOKEN_URL, clientCredentials.getTokenEndpoint());
    }
  }
}
