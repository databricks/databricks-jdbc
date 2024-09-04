package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.oauth.Token;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.http.HttpHeaders;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OAuthRefreshCredentialsProviderTest {

  @Mock IDatabricksConnectionContext context;
  @Mock DatabricksConfig databricksConfig;
  @Mock CommonsHttpClient httpClient;
  @Mock Response httpResponse;

  private static final InputStream OAUTH_RESPONSE =
      new ByteArrayInputStream(
          new JSONObject()
              .put("access_token", "access-token")
              .put("token_type", "token-type")
              .put("expires_in", 360)
              .put("refresh_token", "refresh-token")
              .toString()
              .getBytes());

  private OAuthRefreshCredentialsProvider credentialsProvider;

  @BeforeEach
  void setup() throws DatabricksParsingException {
    when(context.getClientId()).thenReturn("client-id");
    when(context.getClientSecret()).thenReturn("client-secret");
    when(context.getOAuthRefreshToken()).thenReturn("refresh-token");
    credentialsProvider = new OAuthRefreshCredentialsProvider(context);
  }

  @Test
  void testRefreshThrowsExceptionWhenRefreshTokenIsNotSet() {
    when(context.getOAuthRefreshToken()).thenReturn(null);
    OAuthRefreshCredentialsProvider providerWithNullRefreshToken =
        new OAuthRefreshCredentialsProvider(context);
    DatabricksException exception =
        assertThrows(DatabricksException.class, providerWithNullRefreshToken::refresh);
    assertEquals("oauth2: token expired and refresh token is not set", exception.getMessage());
  }

  @Test
  void testRefreshSuccess() throws IOException {
    when(databricksConfig.getHttpClient()).thenReturn(httpClient);
    when(httpClient.execute(any())).thenReturn(httpResponse);
    when(httpResponse.getBody()).thenReturn(OAUTH_RESPONSE);
    HeaderFactory headerFactory = credentialsProvider.configure(databricksConfig);
    Map<String, String> headers = headerFactory.headers();
    assertNotNull(headers.get(HttpHeaders.AUTHORIZATION));
    Token refreshedToken = credentialsProvider.getToken();
    assertEquals("token-type", refreshedToken.getTokenType());
    assertEquals("access-token", refreshedToken.getAccessToken());
    assertEquals("refresh-token", refreshedToken.getRefreshToken());
    assertFalse(refreshedToken.isExpired());
  }
}
