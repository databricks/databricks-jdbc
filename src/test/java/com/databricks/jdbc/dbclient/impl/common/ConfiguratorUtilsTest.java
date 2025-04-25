package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
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
    // Create an empty JKS keystore
    KeyStore keyStore = KeyStore.getInstance(TRUST_STORE_TYPE);
    keyStore.load(null, TRUST_STORE_PASSWORD.toCharArray());

    // Save the empty keystore to a file
    try (FileOutputStream fos = new FileOutputStream(EMPTY_TRUST_STORE_PATH)) {
      keyStore.store(fos, TRUST_STORE_PASSWORD.toCharArray());
    }
  }

  private static void createDummyTrustStore() throws Exception {
    // Create an empty JKS keystore
    KeyStore keyStore = KeyStore.getInstance(TRUST_STORE_TYPE);
    keyStore.load(null, TRUST_STORE_PASSWORD.toCharArray());

    // Generate a key pair (public and private keys)
    KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
    keyPairGen.initialize(2048);
    KeyPair keyPair = keyPairGen.generateKeyPair();

    // Create a self-signed certificate
    X509Certificate certificate = generateBarebonesCertificate(keyPair);
    keyStore.setCertificateEntry("dummy-cert", certificate);
    try (FileOutputStream fos = new FileOutputStream(DUMMY_TRUST_STORE_PATH)) {
      keyStore.store(fos, TRUST_STORE_PASSWORD.toCharArray());
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
      Files.delete(Path.of(EMPTY_TRUST_STORE_PATH));
    } catch (IOException e) {
      LOGGER.info("Failed to delete empty trust store file: " + e.getMessage());
    }
    try {
      Files.delete(Path.of(DUMMY_TRUST_STORE_PATH));
    } catch (IOException e) {
      LOGGER.info("Failed to delete dummy trust store file: " + e.getMessage());
    }
  }

  @Test
  void testGetBaseConnectionManager_NoSSLTrustStoreAndRevocationCheckEnabled()
      throws DatabricksHttpException {
    // Define behavior for mock context
    when(mockContext.checkCertificateRevocation()).thenReturn(true);
    when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(false);
    when(mockContext.useSystemTrustStore()).thenReturn(false);

    try (MockedStatic<ConfiguratorUtils> configuratorUtils =
        mockStatic(ConfiguratorUtils.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))) {

      // Call getBaseConnectionManager with the mock context
      PoolingHttpClientConnectionManager connManager =
          ConfiguratorUtils.getBaseConnectionManager(mockContext);

      configuratorUtils.verify(
          () -> ConfiguratorUtils.createConnectionSocketFactoryRegistry(any()), times(1));

      // Ensure the returned connection manager is not null
      assertNotNull(connManager);
    }
  }

  @Test
  void testGetBaseConnectionManager_WithSSLTrustStore() throws DatabricksHttpException {
    try (MockedStatic<ConfiguratorUtils> configuratorUtils = mockStatic(ConfiguratorUtils.class)) {
      configuratorUtils
          .when(() -> ConfiguratorUtils.getBaseConnectionManager(mockContext))
          .thenCallRealMethod();
      configuratorUtils
          .when(() -> ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext))
          .thenReturn(mock(Registry.class));
      // Call getBaseConnectionManager with the mock context
      PoolingHttpClientConnectionManager connManager =
          ConfiguratorUtils.getBaseConnectionManager(mockContext);

      // Assert that getConnectionSocketFactoryRegistry was called
      configuratorUtils.verify(
          () -> ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext), times(1));

      // Ensure the returned connection manager is not null
      assertNotNull(connManager);
    }
  }

  @Test
  void testUseSystemTrustStoreFalse_NoCustomTrustStore() throws DatabricksHttpException {
    // Scenario: useSystemTrustStore=false and no custom trust store provided
    // Should use JDK default trust store and ignore system property

    when(mockContext.useSystemTrustStore()).thenReturn(false);
    when(mockContext.checkCertificateRevocation()).thenReturn(false);

    Registry<ConnectionSocketFactory> registry =
        ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);
    assertNotNull(registry);
    assertInstanceOf(
        SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
  }

  @Test
  void testCustomTrustStore_WithRevocationChecking() throws DatabricksHttpException {
    // Scenario: Custom trust store with certificate revocation checking

    when(mockContext.checkCertificateRevocation()).thenReturn(true);
    when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(true);

    Registry<ConnectionSocketFactory> registry =
        ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);

    assertNotNull(registry);
    assertInstanceOf(
        SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
  }

  @Test
  void testCreateRegistryWithSystemPropertyTrustStore() throws DatabricksHttpException {
    // Save original system properties to restore later
    String originalTrustStore = System.getProperty("javax.net.ssl.trustStore");
    String originalPassword = System.getProperty("javax.net.ssl.trustStorePassword");
    String originalType = System.getProperty("javax.net.ssl.trustStoreType");

    try {
      // Set system properties to use the dummy trust store
      System.setProperty("javax.net.ssl.trustStore", DUMMY_TRUST_STORE_PATH);
      System.setProperty("javax.net.ssl.trustStorePassword", TRUST_STORE_PASSWORD);
      System.setProperty("javax.net.ssl.trustStoreType", TRUST_STORE_TYPE);
      when(mockContext.useSystemTrustStore()).thenReturn(true);
      when(mockContext.checkCertificateRevocation()).thenReturn(false);

      Registry<ConnectionSocketFactory> registry =
          ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);

      assertNotNull(registry);
      assertInstanceOf(
          SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
    } finally {
      // Restore original system properties
      if (originalTrustStore != null) {
        System.setProperty("javax.net.ssl.trustStore", originalTrustStore);
      } else {
        System.clearProperty("javax.net.ssl.trustStore");
      }

      if (originalPassword != null) {
        System.setProperty("javax.net.ssl.trustStorePassword", originalPassword);
      } else {
        System.clearProperty("javax.net.ssl.trustStorePassword");
      }

      if (originalType != null) {
        System.setProperty("javax.net.ssl.trustStoreType", originalType);
      } else {
        System.clearProperty("javax.net.ssl.trustStoreType");
      }
    }
  }

  @Test
  void testCreateRegistryWithSystemPropertyTrustStore_WithRevocationChecking()
      throws DatabricksHttpException {
    // Save original system properties to restore later
    String originalTrustStore = System.getProperty("javax.net.ssl.trustStore");
    String originalPassword = System.getProperty("javax.net.ssl.trustStorePassword");
    String originalType = System.getProperty("javax.net.ssl.trustStoreType");

    try {
      // Set system properties to use the dummy trust store
      System.setProperty("javax.net.ssl.trustStore", DUMMY_TRUST_STORE_PATH);
      System.setProperty("javax.net.ssl.trustStorePassword", TRUST_STORE_PASSWORD);
      System.setProperty("javax.net.ssl.trustStoreType", TRUST_STORE_TYPE);

      when(mockContext.useSystemTrustStore()).thenReturn(true);
      when(mockContext.checkCertificateRevocation()).thenReturn(true);
      when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(true);

      Registry<ConnectionSocketFactory> registry =
          ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);

      assertNotNull(registry);
      assertInstanceOf(
          SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
    } finally {
      // Restore original system properties
      if (originalTrustStore != null) {
        System.setProperty("javax.net.ssl.trustStore", originalTrustStore);
      } else {
        System.clearProperty("javax.net.ssl.trustStore");
      }

      if (originalPassword != null) {
        System.setProperty("javax.net.ssl.trustStorePassword", originalPassword);
      } else {
        System.clearProperty("javax.net.ssl.trustStorePassword");
      }

      if (originalType != null) {
        System.setProperty("javax.net.ssl.trustStoreType", originalType);
      } else {
        System.clearProperty("javax.net.ssl.trustStoreType");
      }
    }
  }

  @Test
  void testCreateTrustManagers_WithAndWithoutRevocationChecking() throws Exception {
    // Load a real trust store to test with
    when(mockContext.checkCertificateRevocation()).thenReturn(true);
    when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(false);
    Registry<ConnectionSocketFactory> revocationCheckingRegistry =
        ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);
    assertNotNull(revocationCheckingRegistry);

    // Test with revocation checking disabled
    when(mockContext.checkCertificateRevocation()).thenReturn(false);
    Registry<ConnectionSocketFactory> noRevocationCheckingRegistry =
        ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);
    assertNotNull(noRevocationCheckingRegistry);
  }

  @Test
  void testFindX509TrustManager() throws Exception {
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init((KeyStore) null);
    TrustManager[] trustManagers = tmf.getTrustManagers();

    // Verify we have at least one trust manager
    assertNotNull(trustManagers);
    assertTrue(trustManagers.length > 0);

    // Verify at least one is an X509TrustManager
    boolean foundX509TrustManager = false;
    for (TrustManager tm : trustManagers) {
      if (tm instanceof X509TrustManager) {
        foundX509TrustManager = true;
        break;
      }
    }
    assertTrue(foundX509TrustManager, "Should find at least one X509TrustManager");
  }

  @Test
  void testEmptyTrustAnchorsException() {
    // Test the behavior when trust anchors are empty
    Set<TrustAnchor> emptyTrustAnchors = Collections.emptySet();

    DatabricksHttpException exception =
        assertThrows(
            DatabricksHttpException.class,
            () -> ConfiguratorUtils.buildTrustManagerParameters(emptyTrustAnchors, true, false));

    assertTrue(
        exception.getMessage().contains("parameter must be non-empty"),
        "Exception should mention empty parameter");
  }

  @Test
  void testCreateSocketFactoryRegistry() throws Exception {
    // Test using a real trust manager
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init((KeyStore) null);

    // Create a registry with the system default trust managers
    when(mockContext.checkCertificateRevocation()).thenReturn(false);
    when(mockContext.useSystemTrustStore()).thenReturn(false);

    Registry<ConnectionSocketFactory> registry =
        ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);

    assertNotNull(registry);
    assertInstanceOf(
        SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
    assertInstanceOf(
        PlainConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTP));
  }
}
