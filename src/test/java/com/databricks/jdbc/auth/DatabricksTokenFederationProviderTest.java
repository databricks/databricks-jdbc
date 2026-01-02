package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.oauth.Token;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.http.client.methods.HttpPost;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksTokenFederationProviderTest {

  @Mock private IDatabricksConnectionContext mockContext;
  @Mock private CredentialsProvider mockCredentialsProvider;
  @Mock private DatabricksConfig mockConfig;
  @Mock private IDatabricksHttpClient mockHttpClient;
  private DatabricksTokenFederationProvider databricksTokenFederationProvider;

  @BeforeEach
  public void setUp() {
    lenient().when(mockContext.getConnectionUuid()).thenReturn("test-uuid");
    lenient().when(mockContext.getHttpMaxConnectionsPerRoute()).thenReturn(100);
    databricksTokenFederationProvider =
        spy(new DatabricksTokenFederationProvider(mockContext, mockCredentialsProvider));
  }

  @Test
  public void testPassedCredentialsProviderFunctionCall() {
    when(mockCredentialsProvider.authType()).thenReturn("mock-credentials-provider");

    String authType = databricksTokenFederationProvider.authType();
    CredentialsProvider credentialsProvider =
        databricksTokenFederationProvider.getCredentialsProvider();

    assertEquals("mock-credentials-provider", authType, "Auth type should match the mock value");
    assertSame(
        mockCredentialsProvider,
        credentialsProvider,
        "CredentialsProvider should be the same as the mock");
  }

  @Test
  public void testRefreshTokenOnTokenExpiry() throws Exception {
    doReturn(testToken()).when(databricksTokenFederationProvider).getToken();
    int getTokenCount = 10;
    for (int i = 0; i < getTokenCount; i++) {
      Map<String, String> headers =
          databricksTokenFederationProvider.configure(mockConfig).headers();
    }
    // getToken should be called all 10 times
    verify(databricksTokenFederationProvider, times(10)).getToken();
  }

  @Test
  public void testConfigureFunctionCall() throws Exception {
    doReturn(testToken()).when(databricksTokenFederationProvider).getToken();

    Map<String, String> resultHeaders =
        databricksTokenFederationProvider.configure(mockConfig).headers();
    Map<String, String> expectedHeaders = new HashMap<>();
    expectedHeaders.put("Authorization", "tokenType accessToken");

    assertEquals(expectedHeaders, resultHeaders);
  }

  @Test
  public void testExchangeToken() throws Exception {
    // Use the 4-parameter test constructor that sets the config field
    when(mockConfig.getHost()).thenReturn("https://host.com");
    DatabricksTokenFederationProvider providerWithConfig =
        spy(
            new DatabricksTokenFederationProvider(
                mockContext, mockCredentialsProvider, mockConfig));
    doReturn(testToken()).when(providerWithConfig).retrieveToken(any(), any(), any(), any());
    Token exchangedToken = providerWithConfig.exchangeToken("thirdPartyToken");
    assertEquals("tokenType", exchangedToken.getTokenType());
    assertEquals("accessToken", exchangedToken.getAccessToken());
    assertEquals("refreshToken", exchangedToken.getRefreshToken());
  }

  @Test
  public void testRetrieveTokensFailure() throws Exception {

    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenThrow(
            new DatabricksHttpException(
                "Connection error", DatabricksDriverErrorCode.CONNECTION_ERROR));

    assertThrows(
        DatabricksDriverException.class,
        () ->
            databricksTokenFederationProvider.retrieveToken(
                mockHttpClient, "https://host.com", new HashMap<>(), new HashMap<>()));
  }

  @Test
  public void testTokenExchangeFailure() throws Exception {
    doThrow(DatabricksDriverException.class)
        .when(databricksTokenFederationProvider)
        .exchangeToken("accessToken");

    Optional<Token> returnedToken =
        databricksTokenFederationProvider.tryTokenExchange("accessToken", "tokenType");
    assertTrue(returnedToken.isEmpty());
  }

  @Test
  public void testSameHostNoTokenExchange() throws Exception {

    Map<String, String> testExternalHeaders =
        Map.of("Authorization", "Bearer " + testJwtTokenString());

    when(mockConfig.getHost()).thenReturn("https://host.com");
    when(mockCredentialsProvider.configure(any())).thenReturn(() -> testExternalHeaders);
    Map<String, String> headers = databricksTokenFederationProvider.configure(mockConfig).headers();

    assertEquals(testExternalHeaders, headers);
    verify(databricksTokenFederationProvider).createToken(any(), any());
  }

  private Token testToken() throws Exception {
    return new Token(
        "accessToken", "tokenType", "refreshToken", Instant.now().plus(10, ChronoUnit.MINUTES));
  }

  private String testJwtTokenString() throws Exception {
    RSAKey rsaJWK = new RSAKeyGenerator(2048).keyID("123").generate();
    RSASSASigner signer = new RSASSASigner(rsaJWK);

    JWTClaimsSet claims =
        new JWTClaimsSet.Builder()
            .issuer("https://host.com")
            .subject("user@example.com")
            .audience("https://api.host.com")
            .issueTime(new Date(1713350400L * 1000))
            .expirationTime(new Date(25340230079L * 1000))
            .build();

    SignedJWT signedJWT = new SignedJWT(new JWSHeader.Builder(JWSAlgorithm.RS256).build(), claims);
    signedJWT.sign(signer);

    return signedJWT.serialize();
  }
}
