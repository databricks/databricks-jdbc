package com.databricks.jdbc.auth;

import static com.databricks.jdbc.TestConstants.TEST_AUTH_URL;
import static com.databricks.jdbc.TestConstants.TEST_TOKEN_URL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.oauth.OAuthResponse;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.databricks.sdk.core.oauth.Token;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import org.apache.http.HttpHeaders;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OAuthRefreshCredentialsProviderTest {

  @Mock IDatabricksConnectionContext context;
  @Mock DatabricksConfig databricksConfig;
  @Mock CommonsHttpClient httpClient;
  @Mock Response httpResponse;
  @Mock OAuthResponse oAuthResponse;

  @Mock Response response;
  private OAuthRefreshCredentialsProvider credentialsProvider;
  private static final String REFRESH_TOKEN_URL_DEFAULT =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/erg6767gg;OAuthRefreshToken=refresh-token";
  private static final String REFRESH_TOKEN_URL_OVERRIDE_CLIENT_ID =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/erg6767gg;OAuthRefreshToken=refresh-token;OAuth2ClientID=client_id";
  private static final String REFRESH_TOKEN_URL_OVERRIDE_CLIENT_ID_CLIENT_SECRET =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/erg6767gg;OAuthRefreshToken=refresh-token;OAuth2ClientID=client_id;OAuth2Secret=client_secret";
  private static final String REFRESH_TOKEN_URL_OVERRIDE_TOKEN_URL =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/erg6767gg;OAuthRefreshToken=refresh-token;OAuth2TokenEndpoint=token_endpoint";
  private static final String REFRESH_TOKEN_URL_OVERRIDE_EVERYTHING =
      "jdbc:databricks://host:4423/default;transportMode=http;ssl=1;AuthMech=11;AuthFlow=0;httpPath=/sql/1.0/warehouses/erg6767gg;OAuthRefreshToken=refresh-token;OAuth2TokenEndpoint=token_endpoint;OAuth2ClientID=client_id;OAuth2Secret=client_secret";

  @Test
  void testRefreshThrowsExceptionWhenRefreshTokenIsNotSet() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(REFRESH_TOKEN_URL_DEFAULT, new Properties());
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(
            new OpenIDConnectEndpoints(
                "https://oauth.example.com/oidc/v1/token",
                "https://oauth.example.com/oidc/v1/authorize"));
    credentialsProvider = new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);
    when(context.getOAuthRefreshToken()).thenReturn(null);
    OAuthRefreshCredentialsProvider providerWithNullRefreshToken =
        new OAuthRefreshCredentialsProvider(context, databricksConfig);
    DatabricksException exception =
        assertThrows(DatabricksException.class, providerWithNullRefreshToken::refresh);
    assertEquals("oauth2: token expired and refresh token is not set", exception.getMessage());
  }

  @Test
  void testRefreshThrowsExceptionWhenOIDCFetchFails() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(REFRESH_TOKEN_URL_DEFAULT, new Properties());
    when(databricksConfig.getOidcEndpoints()).thenThrow(new IOException());
    assertThrows(
        DatabricksException.class,
        () -> new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        REFRESH_TOKEN_URL_DEFAULT,
        REFRESH_TOKEN_URL_OVERRIDE_EVERYTHING,
        REFRESH_TOKEN_URL_OVERRIDE_CLIENT_ID,
        REFRESH_TOKEN_URL_OVERRIDE_CLIENT_ID_CLIENT_SECRET,
        REFRESH_TOKEN_URL_OVERRIDE_TOKEN_URL
      })
  void testRefreshSuccess(String refreshTokenUrl) throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(refreshTokenUrl, null, null);
    boolean isDefaultEndpointPath = connectionContext.getTokenEndpoint() == null;
    if (isDefaultEndpointPath) {
      when(databricksConfig.getOidcEndpoints())
          .thenReturn(new OpenIDConnectEndpoints(TEST_TOKEN_URL, TEST_AUTH_URL));
    }
    credentialsProvider = new OAuthRefreshCredentialsProvider(connectionContext, databricksConfig);
    assertEquals("oauth-refresh", credentialsProvider.authType());
    // Reinitialize the OAUTH_RESPONSE InputStream for each test run
    String jsonResponse =
        new JSONObject()
            .put("access_token", "access-token")
            .put("token_type", "token-type")
            .put("expires_in", 360)
            .put("refresh_token", "refresh-token")
            .toString();

    when(databricksConfig.getHttpClient()).thenReturn(httpClient);
    when(httpClient.execute(any())).thenReturn(new Response(jsonResponse, new URL(TEST_TOKEN_URL)));
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
