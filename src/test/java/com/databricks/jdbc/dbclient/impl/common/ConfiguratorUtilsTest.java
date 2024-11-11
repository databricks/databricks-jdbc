package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
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
import java.util.Date;
import java.util.Set;
import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
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
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConfiguratorUtilsTest {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ConfiguratorUtilsTest.class);
  @Mock private IDatabricksConnectionContext mockContext;
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

  private static void createEmptyTrustStore()
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

  private static void createDummyTrustStore() throws Exception {
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
  void testGetConnectionSocketFactoryRegistry() {
    when(mockContext.getSSLTrustStorePassword()).thenReturn(TRUST_STORE_PASSWORD);
    when(mockContext.getSSLTrustStoreType()).thenReturn(TRUST_STORE_TYPE);
    when(mockContext.getSSLTrustStore()).thenReturn(EMPTY_TRUST_STORE_PATH);
    assertThrows(
        DatabricksException.class,
        () -> ConfiguratorUtils.getConnectionSocketFactoryRegistry(mockContext),
        "the trustAnchors parameter must be non-empty");

    when(mockContext.getSSLTrustStore()).thenReturn(DUMMY_TRUST_STORE_PATH);
    Registry<ConnectionSocketFactory> registry =
        ConfiguratorUtils.getConnectionSocketFactoryRegistry(mockContext);
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
    KeyStore trustStore = ConfiguratorUtils.loadTruststoreOrNull(mockContext);
    Set<TrustAnchor> trustAnchors = ConfiguratorUtils.getTrustAnchorsFromTrustStore(trustStore);
    assertTrue(
        trustAnchors.stream()
            .anyMatch(ta -> ta.getTrustedCert().getIssuerDN().toString().contains(CERTIFICATE_CN)));
  }

  @Test
  void testGetBaseConnectionManager_NoSSLTrustStoreAndRevocationCheckEnabled() {
    // Define behavior for mock context to meet conditions for not calling
    // getConnectionSocketFactoryRegistry
    when(mockContext.getSSLTrustStore()).thenReturn(null);
    when(mockContext.checkCertificateRevocation()).thenReturn(true);
    when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(false);

    try (MockedStatic<ConfiguratorUtils> configuratorUtils = mockStatic(ConfiguratorUtils.class)) {
      configuratorUtils
          .when(() -> ConfiguratorUtils.getBaseConnectionManager(mockContext))
          .thenCallRealMethod();
      // Call getBaseConnectionManager with the mock context
      PoolingHttpClientConnectionManager connManager =
          ConfiguratorUtils.getBaseConnectionManager(mockContext);

      // Assert that getConnectionSocketFactoryRegistry was NOT called
      configuratorUtils.verify(
          () -> ConfiguratorUtils.getConnectionSocketFactoryRegistry(mockContext), never());

      // Ensure the returned connection manager is not null
      assertNotNull(connManager);
    }
  }

  @Test
  void testGetBaseConnectionManager_WithSSLTrustStore() {
    // Define behavior for mock context where SSLTrustStore is set
    when(mockContext.getSSLTrustStore()).thenReturn(DUMMY_TRUST_STORE_PATH);

    try (MockedStatic<ConfiguratorUtils> configuratorUtils = mockStatic(ConfiguratorUtils.class)) {
      configuratorUtils
          .when(() -> ConfiguratorUtils.getBaseConnectionManager(mockContext))
          .thenCallRealMethod();
      configuratorUtils
          .when(() -> ConfiguratorUtils.getConnectionSocketFactoryRegistry(mockContext))
          .thenReturn(mock(Registry.class));
      // Call getBaseConnectionManager with the mock context
      PoolingHttpClientConnectionManager connManager =
          ConfiguratorUtils.getBaseConnectionManager(mockContext);

      // Assert that getConnectionSocketFactoryRegistry was called
      configuratorUtils.verify(
          () -> ConfiguratorUtils.getConnectionSocketFactoryRegistry(mockContext), times(1));

      // Ensure the returned connection manager is not null
      assertNotNull(connManager);
    }
  }
}
