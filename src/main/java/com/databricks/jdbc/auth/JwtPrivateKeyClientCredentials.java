package com.databricks.jdbc.auth;

import static com.nimbusds.jose.JWSAlgorithm.*;

import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.oauth.OAuthResponse;
import com.databricks.sdk.core.oauth.RefreshableTokenSource;
import com.databricks.sdk.core.oauth.Token;
import com.google.common.annotations.VisibleForTesting;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.*;
import com.nimbusds.jwt.*;
import java.io.*;
import java.io.FileReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.Security;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

/**
 * An implementation of RefreshableTokenSource implementing the JWT client_credentials OAuth grant
 * type.
 */
public class JwtPrivateKeyClientCredentials extends RefreshableTokenSource {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(JwtPrivateKeyClientCredentials.class);

  public static class Builder {
    private String clientId;
    private String tokenUrl;
    private String jwtKeyFile;
    private String jwtKid;
    private String jwtKeyPassphrase;
    private String jwtAlgorithm;
    private IDatabricksHttpClient hc;
    private List<String> scopes = Collections.emptyList();

    public Builder withClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder withTokenUrl(String tokenUrl) {
      this.tokenUrl = tokenUrl;
      return this;
    }

    public Builder withScopes(List<String> scopes) {
      this.scopes = scopes;
      return this;
    }

    public Builder withHttpClient(IDatabricksHttpClient hc) {
      this.hc = hc;
      return this;
    }

    public Builder withJwtAlgorithm(String jwtAlgorithm) {
      this.jwtAlgorithm = jwtAlgorithm;
      return this;
    }

    public Builder withJwtKeyPassphrase(String jwtKeyPassphrase) {
      this.jwtKeyPassphrase = jwtKeyPassphrase;
      return this;
    }

    public Builder withJwtKid(String jwtKid) {
      this.jwtKid = jwtKid;
      return this;
    }

    public Builder withJwtKeyFile(String jwtKeyFile) {
      this.jwtKeyFile = jwtKeyFile;
      return this;
    }

    public JwtPrivateKeyClientCredentials build() {
      Objects.requireNonNull(this.clientId, "clientId must be specified");
      Objects.requireNonNull(this.jwtKeyFile, "JWT key file must be specified");
      Objects.requireNonNull(this.jwtKid, "JWT KID must be specified");
      return new JwtPrivateKeyClientCredentials(
          hc, clientId, jwtKeyFile, jwtKid, jwtKeyPassphrase, jwtAlgorithm, tokenUrl, scopes);
    }
  }

  private final String BOUNCY_CASTLE_PROVIDER = "BC";
  private IDatabricksHttpClient hc;
  private String clientId;
  private String tokenUrl;
  private final List<String> scopes;

  private final String jwtKeyFile;
  private final String jwtKid;
  private final String jwtKeyPassphrase;
  private final JWSAlgorithm jwtAlgorithm;

  private JwtPrivateKeyClientCredentials(
      IDatabricksHttpClient hc,
      String clientId,
      String jwtKeyFile,
      String jwtKid,
      String jwtKeyPassphrase,
      String jwtAlgorithm,
      String tokenUrl,
      List<String> scopes) {
    this.hc = hc;
    this.clientId = clientId;
    this.jwtKeyFile = jwtKeyFile;
    this.jwtKid = jwtKid;
    this.jwtKeyPassphrase = jwtKeyPassphrase;
    this.jwtAlgorithm = determineSignatureAlgorithm(jwtAlgorithm);
    this.tokenUrl = tokenUrl;
    this.scopes = scopes;
  }

  @Override
  protected Token refresh() {
    Map<String, String> params = new HashMap<>();
    params.put("grant_type", "client_credentials");
    if (scopes != null) {
      params.put("scope", String.join(" ", scopes));
    }
    params.put("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer");
    params.put("client_assertion", getSerialisedSignedJWT());
    if (DriverUtil.isRunningAgainstFake()) {
      params.put("client_assertion", "my-private-key");
    }
    return retrieveToken(hc, tokenUrl, params, new HashMap<>());
  }

  @VisibleForTesting
  protected static Token retrieveToken(
      IDatabricksHttpClient hc,
      String tokenUrl,
      Map<String, String> params,
      Map<String, String> headers) {
    try {
      URIBuilder uriBuilder = new URIBuilder(tokenUrl);
      HttpPost postRequest = new HttpPost(uriBuilder.build());
      postRequest.setEntity(
          new UrlEncodedFormEntity(
              params.entrySet().stream()
                  .map(e -> new BasicNameValuePair(e.getKey(), e.getValue()))
                  .collect(Collectors.toList()),
              StandardCharsets.UTF_8));
      headers.forEach(postRequest::setHeader);
      HttpResponse response = hc.execute(postRequest);
      OAuthResponse resp =
          JsonUtil.getMapper().readValue(response.getEntity().getContent(), OAuthResponse.class);
      LocalDateTime expiry = LocalDateTime.now().plus(resp.getExpiresIn(), ChronoUnit.SECONDS);
      return new Token(resp.getAccessToken(), resp.getTokenType(), resp.getRefreshToken(), expiry);
    } catch (IOException | URISyntaxException | DatabricksHttpException e) {
      String errorMessage = "Failed to retrieve custom M2M token: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  private String getSerialisedSignedJWT() {
    PrivateKey privateKey = getPrivateKey();
    SignedJWT signedJWT = fetchSignedJWT(privateKey);
    return signedJWT.serialize();
  }

  @VisibleForTesting
  String getTokenEndpoint() {
    return tokenUrl;
  }

  @VisibleForTesting
  JWSAlgorithm determineSignatureAlgorithm(String jwtAlgorithm) {
    if (jwtAlgorithm == null) {
      jwtAlgorithm = "RS256"; // By default, we use RS256
    }
    switch (jwtAlgorithm) {
        // Following are RSA algorithms
      case "RS384":
        return RS384;
      case "RS512":
        return RS512;
      case "PS256":
        return PS256;
      case "PS384":
        return PS384;
      case "PS512":
        return PS512;
      case "RS256":
        return RS256;
        // following are EC algorithms
      case "ES384":
        return JWSAlgorithm.ES384;
      case "ES512":
        return JWSAlgorithm.ES512;
      case "ES256":
        return JWSAlgorithm.ES256;
      default:
        LOGGER.debug("Defaulting to RS256. Provided JWT algorithm not supported " + jwtAlgorithm);
        return RS256;
    }
  }

  private PrivateKey getPrivateKey() {
    try {
      Security.addProvider(new BouncyCastleProvider());
      try (Reader reader = new FileReader(jwtKeyFile);
          PEMParser pemParser = new PEMParser(reader)) {
        Object object = pemParser.readObject();
        return convertPrivateKey(object);
      }
    } catch (DatabricksSQLException | IOException e) {
      String errorMessage = "Failed to parse private key: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  PrivateKey convertPrivateKey(Object pemObject) throws DatabricksParsingException {
    PrivateKeyInfo privateKeyInfo;
    try {
      if (jwtKeyPassphrase != null) {
        // Decrypt and process PKCS #8 keys when JWT passphrase is provided
        PKCS8EncryptedPrivateKeyInfo encryptedKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
        JceOpenSSLPKCS8DecryptorProviderBuilder decryptorProviderBuilder =
            new JceOpenSSLPKCS8DecryptorProviderBuilder();
        decryptorProviderBuilder.setProvider(BOUNCY_CASTLE_PROVIDER);
        InputDecryptorProvider decryptorProvider =
            decryptorProviderBuilder.build(jwtKeyPassphrase.toCharArray());
        privateKeyInfo = encryptedKeyInfo.decryptPrivateKeyInfo(decryptorProvider);
      } else {
        // Processing unencrypted private keys when JWT passphrase is absent
        try {
          privateKeyInfo = ((PEMKeyPair) pemObject).getPrivateKeyInfo();
        } catch (ClassCastException e) {
          privateKeyInfo = (PrivateKeyInfo) pemObject;
        }
      }
      JcaPEMKeyConverter keyConverter =
          new JcaPEMKeyConverter().setProvider(BOUNCY_CASTLE_PROVIDER);
      return keyConverter.getPrivateKey(privateKeyInfo);
    } catch (OperatorCreationException | PKCSException | PEMException e) {
      String errorMessage = "Cannot decrypt private JWT key " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new DatabricksParsingException(
          errorMessage, DatabricksDriverErrorCode.VOLUME_OPERATION_PARSING_ERROR);
    }
  }

  @VisibleForTesting
  SignedJWT fetchSignedJWT(PrivateKey privateKey) {
    try {
      JWSSigner signer;
      if (privateKey instanceof RSAPrivateKey) {
        // Use RSA Signer
        signer = new RSASSASigner(privateKey);
      } else if (privateKey instanceof ECPrivateKey) {
        // Use EC Signer
        signer = new ECDSASigner((ECPrivateKey) privateKey);
      } else {
        String errorMessage = "Unsupported private key type: " + privateKey.getClass().getName();
        LOGGER.error(errorMessage);
        throw new DatabricksException(errorMessage);
      }

      Timestamp timestamp = Timestamp.valueOf(LocalDateTime.now());
      JWTClaimsSet claimsSet =
          new JWTClaimsSet.Builder()
              .subject(clientId)
              .issuer(clientId)
              .issueTime(timestamp)
              .expirationTime(timestamp)
              .jwtID(UUID.randomUUID().toString())
              .audience(this.tokenUrl)
              .build();
      JWSHeader header = new JWSHeader.Builder(this.jwtAlgorithm).keyID(this.jwtKid).build();
      SignedJWT signedJWT = new SignedJWT(header, claimsSet);
      signedJWT.sign(signer);
      return signedJWT;
    } catch (JOSEException e) {
      String errorMessage = "Error signing the JWT: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }
}
