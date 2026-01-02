package com.databricks.jdbc.auth;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.Token;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jwt.SignedJWT;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.security.interfaces.RSAPrivateKey;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JwtPrivateKeyClientCredentialsTest {
  @Mock IDatabricksHttpClient httpClient;

  @Mock CloseableHttpResponse httpResponse;

  @Mock HttpEntity httpEntity;

  @Mock RSAPrivateKey rsaPrivateKey;

  private static Path tempKeyFile;

  @BeforeAll
  public static void generateTestKeyFile() throws Exception {
    tempKeyFile = TestKeyGenerator.generateTemporaryKeyFile();
  }

  @AfterAll
  public static void cleanupTestKeyFile() throws Exception {
    TestKeyGenerator.cleanupKeyFile(tempKeyFile);
  }

  // Helper method to create test credentials (uses dynamically generated temp key file)
  private JwtPrivateKeyClientCredentials createTestCredentials() {
    return new JwtPrivateKeyClientCredentials.Builder()
        .withHttpClient(httpClient)
        .withClientId(TEST_CLIENT_ID)
        .withJwtKid(TEST_JWT_KID)
        .withJwtKeyFile(tempKeyFile.toString())
        .withJwtAlgorithm(TEST_JWT_ALGORITHM)
        .withTokenUrl(TEST_TOKEN_URL)
        .build();
  }

  @ParameterizedTest
  @CsvSource({
    "RS384,RS384",
    "RS512,RS512",
    "PS256,PS256",
    "PS384,PS384",
    "PS512,PS512",
    "RS256,RS256",
    "ES384,ES384",
    "ES512,ES512",
    "ES256,ES256",
    "null,RS256",
    "HS256,RS256", // Unsupported algorithm, should default to RS256
  })
  public void testDetermineSignatureAlgorithm(String jwtAlgorithm, JWSAlgorithm expectedAlgorithm) {
    JwtPrivateKeyClientCredentials credentials = createTestCredentials();
    JWSAlgorithm result = credentials.determineSignatureAlgorithm(jwtAlgorithm);
    assertEquals(expectedAlgorithm, result);
  }

  @Test
  public void testRetrieveTokenExceptionHandling() throws DatabricksHttpException {
    when(httpClient.execute(any()))
        .thenThrow(
            new DatabricksHttpException("Network error", DatabricksDriverErrorCode.INVALID_STATE));
    Exception exception =
        assertThrows(
            DatabricksException.class,
            () ->
                JwtPrivateKeyClientCredentials.retrieveToken(
                    httpClient, TEST_TOKEN_URL, Map.of(), Map.of()));
    assertTrue(exception.getMessage().contains("Failed to retrieve custom M2M token"));
  }

  @Test
  public void testRetrieveToken() throws DatabricksHttpException, IOException {
    when(httpClient.execute(any())).thenReturn(httpResponse);
    when(httpResponse.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent())
        .thenReturn(new ByteArrayInputStream(TEST_OAUTH_RESPONSE.getBytes()));
    Token token =
        JwtPrivateKeyClientCredentials.retrieveToken(
            httpClient, TEST_TOKEN_URL, Map.of(), Map.of());
    assertEquals(token.getAccessToken(), TEST_ACCESS_TOKEN);
    assertEquals(token.getTokenType(), "Bearer");
  }

  @Test
  void testFetchSignedJWTWithRSAKey() throws Exception {
    JwtPrivateKeyClientCredentials credentials = createTestCredentials();
    when(rsaPrivateKey.getAlgorithm()).thenReturn("RSA");
    when(rsaPrivateKey.getModulus())
        .thenReturn(new BigInteger(2048, new SecureRandom()).setBit(2047));
    when(rsaPrivateKey.getPrivateExponent()).thenReturn(new BigInteger(10, new SecureRandom()));
    SignedJWT signedJWT = credentials.fetchSignedJWT(rsaPrivateKey);
    assertNotNull(signedJWT);
    assertEquals(TEST_CLIENT_ID, signedJWT.getJWTClaimsSet().getSubject());
    assertEquals(TEST_CLIENT_ID, signedJWT.getJWTClaimsSet().getIssuer());
    assertEquals(TEST_TOKEN_URL, signedJWT.getJWTClaimsSet().getAudience().get(0));
  }
}
