package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.http.HttpClient;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.util.Date;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DatabricksAuthUtilTest {
  private DatabricksConfig mockConfig;
  private HttpClient mockHttpClient;
  private static final String NEW_ACCESS_TOKEN = "new-access-token";
  private static final String HOST_URL = "http://localhost:8080";

  @BeforeEach
  public void setUp() {
    mockConfig = mock(DatabricksConfig.class);
    mockHttpClient = mock(HttpClient.class);

    when(mockConfig.getHost()).thenReturn(HOST_URL);
    when(mockConfig.getHttpClient()).thenReturn(mockHttpClient);
  }

  @Test
  public void testInitializeConfigWithToken() {
    DatabricksConfig newConfig =
        DatabricksAuthUtil.initializeConfigWithToken(NEW_ACCESS_TOKEN, mockConfig);

    assertNotNull(newConfig);
    assertEquals(HOST_URL, newConfig.getHost());
    assertEquals(mockHttpClient, newConfig.getHttpClient());
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, newConfig.getAuthType());
    assertEquals(NEW_ACCESS_TOKEN, newConfig.getToken());
  }

  @Test
  public void testIsTokenJWT() throws Exception {
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

    String JWT = signedJWT.serialize();
    String PAT = "dapXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";

    assertEquals(true, DatabricksAuthUtil.isTokenJWT(JWT));
    assertEquals(false, DatabricksAuthUtil.isTokenJWT(PAT));
  }
}
