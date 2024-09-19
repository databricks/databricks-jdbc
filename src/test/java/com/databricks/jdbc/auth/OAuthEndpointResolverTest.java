package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
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
    OAuthEndpointResolver oAuthEndpointResolver = new OAuthEndpointResolver(context);
    String tokenEndpoint = oAuthEndpointResolver.getTokenEndpoint();
    assertEquals("https://token.example.com", tokenEndpoint);
  }

  @Test
  void testGetTokenEndpoint_WithOAuthDiscoveryModeEnabled() throws Exception {
    when(context.isOAuthDiscoveryModeEnabled()).thenReturn(true);
    when(context.getOAuthDiscoveryURL()).thenReturn("https://discovery.example.com");

    try (MockedStatic<DatabricksHttpClient> mocked = mockStatic(DatabricksHttpClient.class)) {
      mocked.when(() -> DatabricksHttpClient.getInstance(any())).thenReturn(httpClient);
      when(httpClient.execute(any(HttpGet.class))).thenReturn(httpResponse);
      when(httpResponse.getStatusLine()).thenReturn(statusLine);
      when(statusLine.getStatusCode()).thenReturn(200);
      when(httpResponse.getEntity()).thenReturn(entity);
      when(entity.getContent())
          .thenReturn(
              new ByteArrayInputStream(
                  "{\"token_endpoint\": \"https://token.example.com\"}".getBytes()));

      String tokenEndpoint = new OAuthEndpointResolver(context).getTokenEndpoint();
      assertEquals("https://token.example.com", tokenEndpoint);
    }
  }

  @Test
  void testGetTokenEndpoint_WithOAuthDiscoveryModeEnabledButUrlNotProvided()
      throws DatabricksParsingException, IOException {
    doReturn(true).when(context).isOAuthDiscoveryModeEnabled();
    doReturn(null).when(context).getOAuthDiscoveryURL();
    doReturn(null).when(context).getTokenEndpoint();
    OAuthEndpointResolver oAuthEndpointResolver = spy(new OAuthEndpointResolver(context));
    doReturn(databricksConfig).when(oAuthEndpointResolver).getBarebonesDatabricksConfig();
    doReturn(
            new OpenIDConnectEndpoints(
                "https://oauth.example.com/oidc/v1/token",
                "https://oauth.example.com/oidc/v1/authorize"))
        .when(databricksConfig)
        .getOidcEndpoints();

    String expectedTokenUrl = "https://oauth.example.com/oidc/v1/token";
    String tokenEndpoint = oAuthEndpointResolver.getTokenEndpoint();

    verify(oAuthEndpointResolver, times(1)).getDefaultTokenEndpoint();
    assertEquals(expectedTokenUrl, tokenEndpoint);
  }

  @Test
  void testGetTokenEndpoint_WithOAuthDiscoveryModeAndErrorInDiscoveryEndpoint()
      throws DatabricksParsingException, IOException, DatabricksHttpException {
    try (MockedStatic<DatabricksHttpClient> mocked = mockStatic(DatabricksHttpClient.class)) {
      mocked.when(() -> DatabricksHttpClient.getInstance(any())).thenReturn(httpClient);
      when(httpClient.execute(any(HttpGet.class)))
          .thenThrow(
              new DatabricksHttpException("Error fetching token endpoint from discovery endpoint"));
      //      when(context.isOAuthDiscoveryModeEnabled()).thenReturn(true);
      //      when(context.getOAuthDiscoveryURL()).thenReturn("https://fake");
      //      when(context.getTokenEndpoint()).thenReturn(null);
      //      when(context.getHostForOAuth()).thenReturn("oauth.example.com");
      doReturn(true).when(context).isOAuthDiscoveryModeEnabled();
      doReturn("http://fake").when(context).getOAuthDiscoveryURL();
      doReturn(null).when(context).getTokenEndpoint();
      //      doReturn("oauth.example.com").when(context).getHostForOAuth();

      OAuthEndpointResolver oAuthEndpointResolver = spy(new OAuthEndpointResolver(context));
      //
      // when(oAuthEndpointResolver.getBarebonesDatabricksConfig()).thenReturn(databricksConfig);'
      doReturn(databricksConfig).when(oAuthEndpointResolver).getBarebonesDatabricksConfig();
      doReturn(
              new OpenIDConnectEndpoints(
                  "https://oauth.example.com/oidc/v1/token",
                  "https://oauth.example.com/oidc/v1/authorize"))
          .when(databricksConfig)
          .getOidcEndpoints();
      //      when(databricksConfig.getOidcEndpoints())
      //              .thenReturn(
      //                      new OpenIDConnectEndpoints(
      //                              "https://oauth.example.com/oidc/v1/token",
      //                              "https://oauth.example.com/oidc/v1/authorize"));

      String expectedTokenUrl = "https://oauth.example.com/oidc/v1/token";
      String tokenEndpoint = oAuthEndpointResolver.getTokenEndpoint();

      verify(oAuthEndpointResolver, times(1)).getDefaultTokenEndpoint();
      assertEquals(expectedTokenUrl, tokenEndpoint);
    }
  }

  @Test
  void testGetTokenEndpoint_WithoutOAuthDiscoveryModeAndNoTokenEndpoint()
      throws DatabricksParsingException, IOException {
    doReturn(false).when(context).isOAuthDiscoveryModeEnabled();
    doReturn(null).when(context).getTokenEndpoint();
    //    doReturn("oauth.example.com").when(context).getHostForOAuth();
    OAuthEndpointResolver oAuthEndpointResolver = spy(new OAuthEndpointResolver(context));
    doReturn(databricksConfig).when(oAuthEndpointResolver).getBarebonesDatabricksConfig();
    doReturn(
            new OpenIDConnectEndpoints(
                "https://oauth.example.com/oidc/v1/token",
                "https://oauth.example.com/oidc/v1/authorize"))
        .when(databricksConfig)
        .getOidcEndpoints();

    String expectedTokenUrl = "https://oauth.example.com/oidc/v1/token";
    String tokenEndpoint = oAuthEndpointResolver.getTokenEndpoint();

    verify(oAuthEndpointResolver, times(1)).getDefaultTokenEndpoint();
    assertEquals(expectedTokenUrl, tokenEndpoint);
  }
}
