package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.sdk.core.DatabricksException;
import java.io.ByteArrayInputStream;
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
public class AuthUtilsTest {

  @Mock IDatabricksConnectionContext context;

  @Mock DatabricksHttpClient httpClient;

  @Mock CloseableHttpResponse httpResponse;

  @Mock StatusLine statusLine;

  @Mock HttpEntity entity;

  @Test
  void testGetTokenEndpoint_WithTokenEndpointInContext() {
    when(context.getTokenEndpoint()).thenReturn("https://token.example.com");
    String tokenEndpoint = AuthUtils.getTokenEndpoint(context);
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

      String tokenEndpoint = AuthUtils.getTokenEndpoint(context);
      assertEquals("https://token.example.com", tokenEndpoint);
    }
  }

  @Test
  void testGetTokenEndpoint_WithOAuthDiscoveryModeEnabledButUrlNotProvided() {
    when(context.isOAuthDiscoveryModeEnabled()).thenReturn(true);
    when(context.getOAuthDiscoveryURL()).thenReturn(null);

    assertThrows(DatabricksException.class, () -> AuthUtils.getTokenEndpoint(context));
  }

  @Test
  void testGetTokenEndpoint_WithOAuthDiscoveryModeAndErrorInDiscoveryEndpoint() throws Exception {
    when(context.isOAuthDiscoveryModeEnabled()).thenReturn(true);

    try (MockedStatic<DatabricksHttpClient> mocked = mockStatic(DatabricksHttpClient.class)) {
      mocked.when(() -> DatabricksHttpClient.getInstance(any())).thenReturn(httpClient);
      assertThrows(DatabricksException.class, () -> AuthUtils.getTokenEndpoint(context));
    }
  }

  @Test
  void testGetTokenEndpoint_WithoutOAuthDiscoveryModeAndNoTokenEndpoint() {
    when(context.isOAuthDiscoveryModeEnabled()).thenReturn(false);
    when(context.getTokenEndpoint()).thenReturn(null);
    when(context.getHostForOAuth()).thenReturn("oauth.example.com");

    String expectedTokenUrl = "https://oauth.example.com/oidc/v1/token";
    String tokenEndpoint = AuthUtils.getTokenEndpoint(context);

    assertEquals(expectedTokenUrl, tokenEndpoint);
  }
}
