package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OAuthEndpointResolverTest {

  @Mock IDatabricksConnectionContext context;

  @Mock DatabricksHttpClient httpClient;

  @Mock DatabricksConfig databricksConfig;

  @Mock CloseableHttpResponse httpResponse;

  @Mock StatusLine statusLine;

  @Mock HttpEntity entity;

  @Test
  void testGetTokenEndpoint_WithTokenEndpointInContext() {
    when(context.getTokenEndpoint()).thenReturn("https://token.example.com");
    OAuthEndpointResolver oAuthEndpointResolver =
        new OAuthEndpointResolver(context, databricksConfig);
    String tokenEndpoint = oAuthEndpointResolver.getTokenEndpoint();
    assertEquals("https://token.example.com", tokenEndpoint);
  }

  @Test
  void testGetTokenEndpoint_WithOAuthDiscoveryModeEnabled() throws Exception {
    when(context.isOAuthDiscoveryModeEnabled()).thenReturn(true);
    when(context.getOAuthDiscoveryURL()).thenReturn("https://discovery.example.com");

    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(httpClient);
      when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResponse);
      when(httpResponse.getStatusLine()).thenReturn(statusLine);
      when(statusLine.getStatusCode()).thenReturn(200);
      when(httpResponse.getEntity()).thenReturn(entity);
      when(entity.getContent())
          .thenReturn(
              new ByteArrayInputStream(
                  "{\"token_endpoint\": \"https://token.example.com\"}".getBytes()));

      String tokenEndpoint =
          new OAuthEndpointResolver(context, databricksConfig).getTokenEndpoint();
      assertEquals("https://token.example.com", tokenEndpoint);
    }
  }

  @Test
  void testGetTokenEndpoint_WithOAuthDiscoveryModeEnabledButUrlNotProvided() throws IOException {
    when(context.isOAuthDiscoveryModeEnabled()).thenReturn(true);
    when(context.getOAuthDiscoveryURL()).thenReturn(null);
    when(context.getTokenEndpoint()).thenReturn(null);
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(
            new OpenIDConnectEndpoints(
                "https://oauth.example.com/oidc/v1/token",
                "https://oauth.example.com/oidc/v1/authorize"));
    OAuthEndpointResolver oAuthEndpointResolver =
        spy(new OAuthEndpointResolver(context, databricksConfig));

    String expectedTokenUrl = "https://oauth.example.com/oidc/v1/token";
    String tokenEndpoint = oAuthEndpointResolver.getTokenEndpoint();

    verify(oAuthEndpointResolver, times(1)).getDefaultTokenEndpoint();
    assertEquals(expectedTokenUrl, tokenEndpoint);
  }

  @Test
  void testGetTokenEndpoint_WithOAuthDiscoveryModeAndErrorInDiscoveryEndpoint()
      throws IOException, DatabricksHttpException {
    try (MockedStatic<DatabricksHttpClientFactory> factoryMocked =
        mockStatic(DatabricksHttpClientFactory.class)) {
      DatabricksHttpClientFactory mockFactory = mock(DatabricksHttpClientFactory.class);
      factoryMocked.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockFactory);
      when(mockFactory.getClient(any())).thenReturn(httpClient);
      when(httpClient.execute(any(HttpGet.class)))
          .thenThrow(
              new DatabricksHttpException("Error fetching token endpoint from discovery endpoint"));
      when(context.isOAuthDiscoveryModeEnabled()).thenReturn(true);
      when(context.getOAuthDiscoveryURL()).thenReturn("https://fake");
      when(context.getTokenEndpoint()).thenReturn(null);
      when(databricksConfig.getOidcEndpoints())
          .thenReturn(
              new OpenIDConnectEndpoints(
                  "https://oauth.example.com/oidc/v1/token",
                  "https://oauth.example.com/oidc/v1/authorize"));
      OAuthEndpointResolver oAuthEndpointResolver =
          spy(new OAuthEndpointResolver(context, databricksConfig));

      String expectedTokenUrl = "https://oauth.example.com/oidc/v1/token";
      String tokenEndpoint = oAuthEndpointResolver.getTokenEndpoint();

      verify(oAuthEndpointResolver, times(1)).getDefaultTokenEndpoint();
      assertEquals(expectedTokenUrl, tokenEndpoint);
    }
  }

  @Test
  void testGetTokenEndpoint_WithoutOAuthDiscoveryModeAndNoTokenEndpoint() throws IOException {
    when(context.isOAuthDiscoveryModeEnabled()).thenReturn(false);
    when(context.getTokenEndpoint()).thenReturn(null);
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(
            new OpenIDConnectEndpoints(
                "https://oauth.example.com/oidc/v1/token",
                "https://oauth.example.com/oidc/v1/authorize"));
    OAuthEndpointResolver oAuthEndpointResolver =
        spy(new OAuthEndpointResolver(context, databricksConfig));

    String expectedTokenUrl = "https://oauth.example.com/oidc/v1/token";
    String tokenEndpoint = oAuthEndpointResolver.getTokenEndpoint();

    verify(oAuthEndpointResolver, times(1)).getDefaultTokenEndpoint();
    assertEquals(expectedTokenUrl, tokenEndpoint);
  }
}
