package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.auth.PrivateKeyClientCredentialProvider;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ClientConfiguratorTest {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ClientConfiguratorTest.class);
  @Mock private IDatabricksConnectionContext mockContext;
  private ClientConfigurator configurator;
  private static final String BASE_TRUST_STORE_PATH = "src/test/resources/";
  private static final String EMPTY_TRUST_STORE_PATH =
      BASE_TRUST_STORE_PATH + "empty-truststore.jks";
  private static final String DUMMY_TRUST_STORE_PATH =
      BASE_TRUST_STORE_PATH + "dummy-truststore.jks";
  private static final String CERTIFICATE_CN = "MinimalCertificate";
  private static final String TRUST_STORE_TYPE = "PKCS12";
  private static final String TRUST_STORE_PASSWORD = "changeit";

  @BeforeAll
  static void setup() throws Exception {
    createEmptyTrustStore();
    createDummyTrustStore();
  }

  public static void createEmptyTrustStore()
      throws KeyStoreException, CertificateException, IOException, NoSuchAlgorithmException {
    String password = TRUST_STORE_PASSWORD;
    // Create an empty JKS keystore
    KeyStore keyStore = KeyStore.getInstance(TRUST_STORE_TYPE);
    keyStore.load(null, password.toCharArray());

    // Save the empty keystore to a file
    try (FileOutputStream fos = new FileOutputStream(EMPTY_TRUST_STORE_PATH)) {
      keyStore.store(fos, password.toCharArray());
    }
  }

  public static void createDummyTrustStore() throws Exception {
    String trustStorePassword = TRUST_STORE_PASSWORD; // Password for the trust store
    String alias = "dummy-cert"; // Alias for the dummy certificate

    // Create an empty JKS keystore
    KeyStore keyStore = KeyStore.getInstance(TRUST_STORE_TYPE);
    keyStore.load(null, trustStorePassword.toCharArray());

    // Generate a key pair (public and private keys)
    KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
    keyPairGen.initialize(2048);
    KeyPair keyPair = keyPairGen.generateKeyPair();

    // Create a self-signed certificate
    X509Certificate certificate = generateBarebonesCertificate(keyPair);

    // Add the certificate to the keystore
    keyStore.setCertificateEntry(alias, certificate);

    // Save the keystore to a file
    try (FileOutputStream fos = new FileOutputStream(DUMMY_TRUST_STORE_PATH)) {
      keyStore.store(fos, trustStorePassword.toCharArray());
    }
  }

  private static X509Certificate generateBarebonesCertificate(KeyPair keyPair) throws Exception {
    // Certificate details
    X500Name issuer = new X500Name("CN=" + CERTIFICATE_CN);
    BigInteger serialNumber = BigInteger.valueOf(System.currentTimeMillis());
    Date startDate = new Date();
    Date endDate = new Date(startDate.getTime() + (365L * 24 * 60 * 60 * 1000)); // 1 year validity

    // Build the certificate
    JcaX509v3CertificateBuilder certBuilder =
        new JcaX509v3CertificateBuilder(
            issuer, serialNumber, startDate, endDate, issuer, keyPair.getPublic());

    // Sign the certificate
    ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate());
    X509CertificateHolder certHolder = certBuilder.build(signer);

    // Add BouncyCastle as a security provider
    BouncyCastleProvider provider = new BouncyCastleProvider();
    Security.addProvider(provider);
    // Convert the certificate holder to X509Certificate
    return new JcaX509CertificateConverter().setProvider(provider).getCertificate(certHolder);
  }

  @AfterAll
  static void cleanup() {
    try {
      Files.delete(Paths.get(EMPTY_TRUST_STORE_PATH));
    } catch (IOException e) {
      LOGGER.info("Failed to delete empty trust store file: " + e.getMessage());
    }
    try {
      Files.delete(Paths.get(DUMMY_TRUST_STORE_PATH));
    } catch (IOException e) {
      LOGGER.info("Failed to delete dummy trust store file: " + e.getMessage());
    }
  }

  @Test
  void getWorkspaceClient_PAT_AuthenticatesWithAccessToken() throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.PAT);
    when(mockContext.getHostUrl()).thenReturn("https://pat.databricks.com");
    when(mockContext.getToken()).thenReturn("pat-token");
    configurator = new ClientConfigurator(mockContext);

    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://pat.databricks.com", config.getHost());
    assertEquals("pat-token", config.getToken());
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void getWorkspaceClient_OAuthWithTokenPassthrough_AuthenticatesCorrectly()
      throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OAUTH);
    when(mockContext.getAuthFlow())
        .thenReturn(IDatabricksConnectionContext.AuthFlow.TOKEN_PASSTHROUGH);
    when(mockContext.getHostUrl()).thenReturn("https://oauth-token.databricks.com");
    when(mockContext.getPassThroughAccessToken()).thenReturn("oauth-token");
    configurator = new ClientConfigurator(mockContext);

    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-token.databricks.com", config.getHost());
    assertEquals("oauth-token", config.getToken());
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void getWorkspaceClient_OAuthWithClientCredentials_AuthenticatesCorrectly()
      throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OAUTH);
    when(mockContext.getAuthFlow())
        .thenReturn(IDatabricksConnectionContext.AuthFlow.CLIENT_CREDENTIALS);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-client.databricks.com");
    when(mockContext.getClientId()).thenReturn("client-id");
    when(mockContext.getClientSecret()).thenReturn("client-secret");
    configurator = new ClientConfigurator(mockContext);

    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-client.databricks.com", config.getHost());
    assertEquals("client-id", config.getClientId());
    assertEquals("client-secret", config.getClientSecret());
    assertEquals(DatabricksJdbcConstants.M2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testM2MWithJWT() throws DatabricksSQLException {
    String jdbcUrl =
        "jdbc:databricks://adb-565757575.18.azuredatabricks.net:123/default;ssl=1;port=123;AuthMech=11;"
            + "httpPath=/sql/1.0/endpoints/erg6767gg;auth_flow=1;UseJWTAssertion=1;auth_scope=test_scope;"
            + "OAuth2ClientId=test-client;auth_kid=test_kid;Auth_JWT_Key_Passphrase=test_phrase;Auth_JWT_Key_File=test_key_file;"
            + "Auth_JWT_Alg=test_algo;Oauth2TokenEndpoint=token_endpoint";
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(jdbcUrl, new Properties());
    configurator = new ClientConfigurator(connectionContext);
    DatabricksConfig config = configurator.getDatabricksConfig();
    CredentialsProvider provider = config.getCredentialsProvider();
    assertEquals("https://adb-565757575.18.azuredatabricks.net", config.getHost());
    assertEquals("test-client", config.getClientId());
    assertEquals("custom-oauth-m2m", provider.authType());
    assertEquals(DatabricksJdbcConstants.M2M_AUTH_TYPE, config.getAuthType());
    assertEquals(PrivateKeyClientCredentialProvider.class, provider.getClass());
  }

  @Test
  void getWorkspaceClient_OAuthWithBrowserBasedAuthentication_AuthenticatesCorrectly()
      throws DatabricksParsingException {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OAUTH);
    when(mockContext.getAuthFlow())
        .thenReturn(IDatabricksConnectionContext.AuthFlow.BROWSER_BASED_AUTHENTICATION);
    when(mockContext.getHostForOAuth()).thenReturn("https://oauth-browser.databricks.com");
    when(mockContext.getClientId()).thenReturn("browser-client-id");
    when(mockContext.getClientSecret()).thenReturn("browser-client-secret");
    when(mockContext.getOAuthScopesForU2M())
        .thenReturn(Arrays.asList(new String[] {"scope1", "scope2"}));
    configurator = new ClientConfigurator(mockContext);
    WorkspaceClient client = configurator.getWorkspaceClient();
    assertNotNull(client);
    DatabricksConfig config = client.config();

    assertEquals("https://oauth-browser.databricks.com", config.getHost());
    assertEquals("browser-client-id", config.getClientId());
    assertEquals("browser-client-secret", config.getClientSecret());
    assertEquals(Arrays.asList(new String[] {"scope1", "scope2"}), config.getScopes());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL, config.getOAuthRedirectUrl());
    assertEquals(DatabricksJdbcConstants.U2M_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testNonOauth() {
    when(mockContext.getAuthMech()).thenReturn(IDatabricksConnectionContext.AuthMech.OTHER);
    configurator = new ClientConfigurator(mockContext);
    DatabricksConfig config = configurator.getDatabricksConfig();
    assertEquals(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, config.getAuthType());
  }

  @Test
  void testNonProxyHostsFormatConversion() {
    String nonProxyHostsInput = ".example.com,.blabla.net,.xyz.abc";
    assertEquals(
        "*.example.com|*.blabla.net|*.xyz.abc",
        ClientConfigurator.convertNonProxyHostConfigToBeSystemPropertyCompliant(
            nonProxyHostsInput));

    String nonProxyHostsInput2 = "example.com,.blabla.net,123.xyz.abc";
    assertEquals(
        "example.com|*.blabla.net|123.xyz.abc",
        ClientConfigurator.convertNonProxyHostConfigToBeSystemPropertyCompliant(
            nonProxyHostsInput2));

    String nonProxyHostsInput3 = "staging.example.*|blabla.net|*.xyz.abc";
    assertEquals(
        "staging.example.*|blabla.net|*.xyz.abc",
        ClientConfigurator.convertNonProxyHostConfigToBeSystemPropertyCompliant(
            nonProxyHostsInput3));
  }

  @Test
  void testGetConnectionSocketFactoryRegistry() {
    when(mockContext.getSSLTrustStorePassword()).thenReturn(TRUST_STORE_PASSWORD);
    when(mockContext.getSSLTrustStoreType()).thenReturn(TRUST_STORE_TYPE);
    when(mockContext.getSSLTrustStore()).thenReturn(EMPTY_TRUST_STORE_PATH);
    assertThrows(
        DatabricksException.class,
        () -> ClientConfigurator.getConnectionSocketFactoryRegistry(mockContext),
        "the trustAnchors parameter must be non-empty");

    when(mockContext.getSSLTrustStore()).thenReturn(DUMMY_TRUST_STORE_PATH);
    Registry<ConnectionSocketFactory> registry =
        ClientConfigurator.getConnectionSocketFactoryRegistry(mockContext);
    assertInstanceOf(
        SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
    assertInstanceOf(
        PlainConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTP));
  }

  @Test
  void testGetTrustAnchorsFromTrustStore() {
    when(mockContext.getSSLTrustStorePassword()).thenReturn(TRUST_STORE_PASSWORD);
    when(mockContext.getSSLTrustStoreType()).thenReturn(TRUST_STORE_TYPE);
    when(mockContext.getSSLTrustStore()).thenReturn(DUMMY_TRUST_STORE_PATH);
    KeyStore trustStore = ClientConfigurator.loadTruststoreOrNull(mockContext);
    Set<TrustAnchor> trustAnchors = ClientConfigurator.getTrustAnchorsFromTrustStore(trustStore);
    assertTrue(
        trustAnchors.stream()
            .anyMatch(ta -> ta.getTrustedCert().getIssuerDN().toString().contains(CERTIFICATE_CN)));
  }
}
