package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.HeaderFactory;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.databricks.sdk.core.oauth.Token;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
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
    OAuthEndpointResolver oAuthEndpointResolver = spy(new OAuthEndpointResolver(connectionContext));
    when(oAuthEndpointResolver.getBarebonesDatabricksConfig()).thenReturn(databricksConfig);
    when(databricksConfig.getOidcEndpoints())
        .thenReturn(
            new OpenIDConnectEndpoints(
                "https://oauth.example.com/oidc/v1/token",
                "https://oauth.example.com/oidc/v1/authorize"));
    credentialsProvider =
        new OAuthRefreshCredentialsProvider(connectionContext, oAuthEndpointResolver);
    when(context.getOAuthRefreshToken()).thenReturn(null);
    verify(oAuthEndpointResolver, times(1)).getDefaultTokenEndpoint();
    OAuthRefreshCredentialsProvider providerWithNullRefreshToken =
        new OAuthRefreshCredentialsProvider(context, oAuthEndpointResolver);
    DatabricksException exception =
        assertThrows(DatabricksException.class, providerWithNullRefreshToken::refresh);
    assertEquals("oauth2: token expired and refresh token is not set", exception.getMessage());
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
        DatabricksConnectionContextFactory.create(refreshTokenUrl, new Properties());
    OAuthEndpointResolver oAuthEndpointResolver = spy(new OAuthEndpointResolver(connectionContext));
    boolean isDefaultEndpointPath = connectionContext.getTokenEndpoint() == null;
    if (isDefaultEndpointPath) {
      when(oAuthEndpointResolver.getBarebonesDatabricksConfig()).thenReturn(databricksConfig);
      when(databricksConfig.getOidcEndpoints())
          .thenReturn(
              new OpenIDConnectEndpoints(
                  "https://oauth.example.com/oidc/v1/token",
                  "https://oauth.example.com/oidc/v1/authorize"));
    }
    credentialsProvider =
        new OAuthRefreshCredentialsProvider(connectionContext, oAuthEndpointResolver);

    // Reinitialize the OAUTH_RESPONSE InputStream for each test run
    InputStream oauthResponse =
        new ByteArrayInputStream(
            new JSONObject()
                .put("access_token", "access-token")
                .put("token_type", "token-type")
                .put("expires_in", 360)
                .put("refresh_token", "refresh-token")
                .toString()
                .getBytes());

    when(databricksConfig.getHttpClient()).thenReturn(httpClient);
    when(httpClient.execute(any())).thenReturn(httpResponse);
    when(httpResponse.getBody()).thenReturn(oauthResponse);
    HeaderFactory headerFactory = credentialsProvider.configure(databricksConfig);
    Map<String, String> headers = headerFactory.headers();
    assertNotNull(headers.get(HttpHeaders.AUTHORIZATION));
    Token refreshedToken = credentialsProvider.getToken();
    assertEquals("token-type", refreshedToken.getTokenType());
    assertEquals("access-token", refreshedToken.getAccessToken());
    assertEquals("refresh-token", refreshedToken.getRefreshToken());
    assertFalse(refreshedToken.isExpired());
    verify(oAuthEndpointResolver, times(isDefaultEndpointPath ? 1 : 0)).getDefaultTokenEndpoint();
  }
}
