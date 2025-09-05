package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksSSLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
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
  private static final String EMPTY_KEY_STORE_PATH = BASE_TRUST_STORE_PATH + "empty-keystore.jks";
  private static final String DUMMY_KEY_STORE_PATH = BASE_TRUST_STORE_PATH + "dummy-keystore.jks";
  private static final String CERTIFICATE_CN = "MinimalCertificate";
  private static final String TRUST_STORE_TYPE = "PKCS12";
  private static final String TRUST_STORE_PASSWORD = "changeit";
  private static final String KEY_STORE_TYPE = "PKCS12";
  private static final String KEY_STORE_PASSWORD = "changeit";

  @BeforeAll
  static void setup() throws Exception {
    createEmptyStore(EMPTY_TRUST_STORE_PATH, TRUST_STORE_TYPE, TRUST_STORE_PASSWORD);
    createEmptyStore(EMPTY_KEY_STORE_PATH, KEY_STORE_TYPE, KEY_STORE_PASSWORD);
    createDummyStore(
        DUMMY_TRUST_STORE_PATH, TRUST_STORE_TYPE, TRUST_STORE_PASSWORD, "dummy-cert", false);
    createDummyStore(DUMMY_KEY_STORE_PATH, KEY_STORE_TYPE, KEY_STORE_PASSWORD, "client-cert", true);
  }

  /**
   * Creates an empty keystore/truststore file.
   *
   * @param filePath The path where the store will be saved
   * @param storeType The type of store (e.g., "PKCS12", "JKS")
   * @param password The password for the store
   */
  private static void createEmptyStore(String filePath, String storeType, String password)
      throws KeyStoreException, CertificateException, IOException, NoSuchAlgorithmException {
    KeyStore keyStore = KeyStore.getInstance(storeType);
    keyStore.load(null, password.toCharArray());

    // Save the empty keystore to a file
    try (FileOutputStream fos = new FileOutputStream(filePath)) {
      keyStore.store(fos, password.toCharArray());
    }
  }

  /**
   * Creates a keystore/truststore with a test certificate.
   *
   * @param filePath The path where the store will be saved
   * @param storeType The type of store (e.g., "PKCS12", "JKS")
   * @param password The password for the store
   * @param alias The alias for the certificate entry
   * @param isKeyStore Whether this is a keystore (with private key) or truststore (cert only)
   */
  public static void createDummyStore(
      String filePath, String storeType, String password, String alias, boolean isKeyStore)
      throws Exception {
    KeyStore keyStore = KeyStore.getInstance(storeType);
    keyStore.load(null, password.toCharArray());

    // Generate a key pair (public and private keys)
    KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance("RSA");
    keyPairGen.initialize(2048);
    KeyPair keyPair = keyPairGen.generateKeyPair();

    // Create a self-signed certificate
    X509Certificate certificate = generateBarebonesCertificate(keyPair);

    // For keystore, add the private key with certificate chain
    // For truststore, add just the certificate
    if (isKeyStore) {
      keyStore.setKeyEntry(
          alias, keyPair.getPrivate(), password.toCharArray(), new X509Certificate[] {certificate});
    } else {
      keyStore.setCertificateEntry(alias, certificate);
    }

    // Save the keystore to a file
    try (FileOutputStream fos = new FileOutputStream(filePath)) {
      keyStore.store(fos, password.toCharArray());
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
      Files.delete(java.nio.file.Paths.get(EMPTY_TRUST_STORE_PATH));
    } catch (IOException e) {
      LOGGER.info("Failed to delete empty trust store file: " + e.getMessage());
    }
    try {
      Files.delete(java.nio.file.Paths.get(DUMMY_TRUST_STORE_PATH));
    } catch (IOException e) {
      LOGGER.info("Failed to delete dummy trust store file: " + e.getMessage());
    }
    try {
      Files.delete(java.nio.file.Paths.get(EMPTY_KEY_STORE_PATH));
    } catch (IOException e) {
      LOGGER.info("Failed to delete empty key store file: " + e.getMessage());
    }
    try {
      Files.delete(java.nio.file.Paths.get(DUMMY_KEY_STORE_PATH));
    } catch (IOException e) {
      LOGGER.info("Failed to delete dummy key store file: " + e.getMessage());
    }
  }

  @Test
  void testGetConnectionSocketFactoryRegistry() throws DatabricksSSLException {
    when(mockContext.getSSLTrustStorePassword()).thenReturn(TRUST_STORE_PASSWORD);
    when(mockContext.getSSLTrustStoreType()).thenReturn(TRUST_STORE_TYPE);
    when(mockContext.getSSLTrustStore()).thenReturn(EMPTY_TRUST_STORE_PATH);
    assertThrows(
        DatabricksSSLException.class,
        () -> ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext),
        "the trustAnchors parameter must be non-empty");

    when(mockContext.getSSLTrustStore()).thenReturn(DUMMY_TRUST_STORE_PATH);
    Registry<ConnectionSocketFactory> registry =
        ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);
    assertInstanceOf(
        SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
    assertInstanceOf(
        PlainConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTP));
  }

  @Test
  void testGetTrustAnchorsFromTrustStore() throws DatabricksSSLException {
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
  void testGetBaseConnectionManager_NoSSLTrustStoreAndRevocationCheckEnabled()
      throws DatabricksSSLException {
    // Define behavior for mock context
    when(mockContext.getSSLTrustStore()).thenReturn(null);
    when(mockContext.checkCertificateRevocation()).thenReturn(true);
    when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(false);

    try (MockedStatic<ConfiguratorUtils> configuratorUtils =
        mockStatic(ConfiguratorUtils.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))) {

      // Call getBaseConnectionManager with the mock context
      PoolingHttpClientConnectionManager connManager =
          ConfiguratorUtils.getBaseConnectionManager(mockContext);

      // Ensure the returned connection manager is not null
      assertNotNull(connManager);
    }
  }

  @Test
  void testGetBaseConnectionManager_WithSSLTrustStore() throws DatabricksSSLException {
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
  void testUseSystemTrustStoreFalse_NoCustomTrustStore() throws DatabricksSSLException {
    // Scenario: useSystemTrustStore=false and no custom trust store provided
    // Should use JDK default trust store and ignore system property

    when(mockContext.getSSLTrustStore()).thenReturn(null);
    when(mockContext.useSystemTrustStore()).thenReturn(false);
    when(mockContext.checkCertificateRevocation()).thenReturn(false);

    try {
      Registry<ConnectionSocketFactory> registry =
          ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);
      assertNotNull(registry);
      assertInstanceOf(
          SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
    } catch (Exception e) {
      fail(
          "Should not throw exception when useSystemTrustStore=false and no custom trust store: "
              + e.getMessage());
    }
  }

  @Test
  void testAllowSelfSignedCerts() throws DatabricksSSLException {
    // Scenario: allowSelfSignedCerts=true
    // Should use trust-all socket factory

    when(mockContext.allowSelfSignedCerts()).thenReturn(true);

    PoolingHttpClientConnectionManager connManager =
        ConfiguratorUtils.getBaseConnectionManager(mockContext);

    assertNotNull(connManager);
  }

  @Test
  void testCustomTrustStore_WithRevocationChecking() throws DatabricksSSLException {
    // Scenario: Custom trust store with certificate revocation checking

    when(mockContext.getSSLTrustStore()).thenReturn(DUMMY_TRUST_STORE_PATH);
    when(mockContext.getSSLTrustStorePassword()).thenReturn(TRUST_STORE_PASSWORD);
    when(mockContext.getSSLTrustStoreType()).thenReturn(TRUST_STORE_TYPE);
    when(mockContext.checkCertificateRevocation()).thenReturn(true);
    when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(true);

    Registry<ConnectionSocketFactory> registry =
        ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);

    assertNotNull(registry);
    assertInstanceOf(
        SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
  }

  @Test
  void testCreateRegistryWithSystemPropertyTrustStore() throws DatabricksSSLException {
    // Save original system properties to restore later
    String originalTrustStore = System.getProperty("javax.net.ssl.trustStore");
    String originalPassword = System.getProperty("javax.net.ssl.trustStorePassword");
    String originalType = System.getProperty("javax.net.ssl.trustStoreType");

    try {
      // Set system properties to use the dummy trust store
      System.setProperty("javax.net.ssl.trustStore", DUMMY_TRUST_STORE_PATH);
      System.setProperty("javax.net.ssl.trustStorePassword", TRUST_STORE_PASSWORD);
      System.setProperty("javax.net.ssl.trustStoreType", TRUST_STORE_TYPE);

      when(mockContext.getSSLTrustStore()).thenReturn(null);
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
      throws DatabricksSSLException {
    // Save original system properties to restore later
    String originalTrustStore = System.getProperty("javax.net.ssl.trustStore");
    String originalPassword = System.getProperty("javax.net.ssl.trustStorePassword");
    String originalType = System.getProperty("javax.net.ssl.trustStoreType");

    try {
      // Set system properties to use the dummy trust store
      System.setProperty("javax.net.ssl.trustStore", DUMMY_TRUST_STORE_PATH);
      System.setProperty("javax.net.ssl.trustStorePassword", TRUST_STORE_PASSWORD);
      System.setProperty("javax.net.ssl.trustStoreType", TRUST_STORE_TYPE);

      when(mockContext.getSSLTrustStore()).thenReturn(null);
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
  void testNonExistentTrustStore() {
    // Create a mock with lenient verification since this test only expects an exception
    IDatabricksConnectionContext mockContextLocal = mock(IDatabricksConnectionContext.class);

    String nonExistentPath = "/path/to/nonexistent/truststore.jks";
    when(mockContextLocal.getSSLTrustStore()).thenReturn(nonExistentPath);

    DatabricksSSLException exception =
        assertThrows(
            DatabricksSSLException.class,
            () -> ConfiguratorUtils.loadTruststoreOrNull(mockContextLocal));

    assertTrue(
        exception.getMessage().contains("does not exist"),
        "Exception should mention that the trust store does not exist");
  }

  @Test
  void testCreateTrustManagers_WithAndWithoutRevocationChecking() throws Exception {
    // Load a real trust store to test with
    when(mockContext.getSSLTrustStore()).thenReturn(DUMMY_TRUST_STORE_PATH);
    when(mockContext.getSSLTrustStorePassword()).thenReturn(TRUST_STORE_PASSWORD);
    when(mockContext.getSSLTrustStoreType()).thenReturn(TRUST_STORE_TYPE);

    KeyStore trustStore = ConfiguratorUtils.loadTruststoreOrNull(mockContext);
    Set<TrustAnchor> trustAnchors = ConfiguratorUtils.getTrustAnchorsFromTrustStore(trustStore);

    // We're testing a private method, so we'll verify the public method behavior that uses it
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
    // Test instance method rather than using reflection on the private static method
    // First test that we can create a trust manager factory
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

    DatabricksSSLException exception =
        assertThrows(
            DatabricksSSLException.class,
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
    when(mockContext.getSSLTrustStore()).thenReturn(null);
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

  @Test
  void testLoadKeystoreOrNull() throws DatabricksSSLException {
    when(mockContext.getSSLKeyStorePassword()).thenReturn(KEY_STORE_PASSWORD);
    when(mockContext.getSSLKeyStoreType()).thenReturn(KEY_STORE_TYPE);
    when(mockContext.getSSLKeyStore()).thenReturn(DUMMY_KEY_STORE_PATH);

    KeyStore keyStore = ConfiguratorUtils.loadKeystoreOrNull(mockContext);
    assertNotNull(keyStore, "Keystore should be loaded successfully");

    try {
      assertTrue(
          keyStore.containsAlias("client-cert"), "Keystore should contain the client-cert alias");
      assertTrue(keyStore.isKeyEntry("client-cert"), "Alias should be a key entry");
    } catch (KeyStoreException e) {
      fail("Exception checking keystore: " + e.getMessage());
    }
  }

  @Test
  void testNonExistentKeyStore() {
    when(mockContext.getSSLKeyStore()).thenReturn("non-existent-keystore.jks");
    assertThrows(
        DatabricksSSLException.class,
        () -> ConfiguratorUtils.loadKeystoreOrNull(mockContext),
        "Should throw an exception for non-existent keystore");
  }

  @Test
  void testEmptyKeyStore() throws DatabricksSSLException {
    when(mockContext.getSSLKeyStorePassword()).thenReturn(KEY_STORE_PASSWORD);
    when(mockContext.getSSLKeyStoreType()).thenReturn(KEY_STORE_TYPE);
    when(mockContext.getSSLKeyStore()).thenReturn(EMPTY_KEY_STORE_PATH);

    KeyStore keyStore = ConfiguratorUtils.loadKeystoreOrNull(mockContext);
    assertNotNull(keyStore, "Empty keystore should load successfully");

    // Verify the keystore has no key entries
    try {
      boolean hasKeyEntry = false;
      for (String alias : Collections.list(keyStore.aliases())) {
        if (keyStore.isKeyEntry(alias)) {
          hasKeyEntry = true;
          break;
        }
      }
      assertFalse(hasKeyEntry, "Empty keystore should not have key entries");
    } catch (KeyStoreException e) {
      fail("Exception checking empty keystore: " + e.getMessage());
    }
  }

  @Test
  void testClientCertificateAuthentication() throws DatabricksSSLException {
    // Set up the mock context for both trust store and key store
    when(mockContext.getSSLTrustStorePassword()).thenReturn(TRUST_STORE_PASSWORD);
    when(mockContext.getSSLTrustStoreType()).thenReturn(TRUST_STORE_TYPE);
    when(mockContext.getSSLTrustStore()).thenReturn(DUMMY_TRUST_STORE_PATH);
    when(mockContext.getSSLKeyStorePassword()).thenReturn(KEY_STORE_PASSWORD);
    when(mockContext.getSSLKeyStoreType()).thenReturn(KEY_STORE_TYPE);
    when(mockContext.getSSLKeyStore()).thenReturn(DUMMY_KEY_STORE_PATH);
    when(mockContext.checkCertificateRevocation()).thenReturn(false);

    // Create registry with both trust store and key store configured
    Registry<ConnectionSocketFactory> registry =
        ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);

    // Verify registry was created successfully
    assertNotNull(registry, "Registry should be created successfully with client certificate");
    assertInstanceOf(
        SSLConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTPS));
    assertInstanceOf(
        PlainConnectionSocketFactory.class, registry.lookup(DatabricksJdbcConstants.HTTP));
  }

  @Test
  void testMissingKeyStorePassword() {
    when(mockContext.getSSLKeyStore()).thenReturn(DUMMY_KEY_STORE_PATH);
    when(mockContext.getSSLKeyStorePassword()).thenReturn(null);

    assertThrows(
        DatabricksSSLException.class,
        () -> ConfiguratorUtils.loadKeystoreOrNull(mockContext),
        "Should throw an exception when key store password is missing");
  }

  @Test
  void testLoadTrustStoreWithProvider() throws Exception {
    String providerName = "TestProvider";
    when(mockContext.getSSLTrustStore()).thenReturn(DUMMY_TRUST_STORE_PATH);
    when(mockContext.getSSLTrustStorePassword()).thenReturn(TRUST_STORE_PASSWORD);
    when(mockContext.getSSLTrustStoreType()).thenReturn(TRUST_STORE_TYPE);
    when(mockContext.getSSLTrustStoreProvider()).thenReturn(providerName);

    try (MockedStatic<KeyStore> keyStoreStatic = mockStatic(KeyStore.class)) {
      KeyStore mockKeyStore = mock(KeyStore.class);
      keyStoreStatic
          .when(() -> KeyStore.getInstance(TRUST_STORE_TYPE, providerName))
          .thenReturn(mockKeyStore);

      // Call the method under test
      ConfiguratorUtils.loadTruststoreOrNull(mockContext);

      keyStoreStatic.verify(() -> KeyStore.getInstance(TRUST_STORE_TYPE, providerName));
      verify(mockKeyStore).load(any(), eq(TRUST_STORE_PASSWORD.toCharArray()));
    }
  }
}
