package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.IS_JDBC_TEST_ENV;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.util.SocketFactoryUtil;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.*;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/** This class contains the utility functions for configuring a client. */
public class ConfiguratorUtils {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ConfiguratorUtils.class);

  private static final String JAVA_TRUST_STORE_PATH_PROPERTY = "javax.net.ssl.trustStore";
  private static final String JAVA_TRUST_STORE_PASSWORD_PROPERTY =
      "javax.net.ssl.trustStorePassword";
  private static final String JAVA_TRUST_STORE_TYPE_PROPERTY = "javax.net.ssl.trustStoreType";

  private static boolean isJDBCTestEnv() {
    return Boolean.parseBoolean(System.getenv(IS_JDBC_TEST_ENV));
  }

  /**
   * Creates and configures the connection manager based on the connection context.
   *
   * @param connectionContext The connection context to use for configuration.
   * @return A configured PoolingHttpClientConnectionManager.
   * @throws DatabricksHttpException If there is an error during configuration.
   */
  public static PoolingHttpClientConnectionManager getBaseConnectionManager(
      IDatabricksConnectionContext connectionContext) throws DatabricksHttpException {

    if (connectionContext.getSSLTrustStore() == null
        && connectionContext.checkCertificateRevocation()
        && !connectionContext.acceptUndeterminedCertificateRevocation()
        && !connectionContext.useSystemTrustStore()
        && !connectionContext.allowSelfSignedCerts()) {
      return new PoolingHttpClientConnectionManager();
    }

    // For test environments, use a trust-all socket factory
    if (isJDBCTestEnv()) {
      LOGGER.info("Using trust-all socket factory for JDBC test environment");
      return new PoolingHttpClientConnectionManager(
          SocketFactoryUtil.getTrustAllSocketFactoryRegistry());
    }

    // If self-signed certificates are allowed, use a trust-all socket factory
    if (connectionContext.allowSelfSignedCerts()) {
      LOGGER.warn(
          "Self-signed certificates are allowed. Please only use this parameter (AllowSelfSignedCerts) when you're sure of what you're doing. This is not recommended for production use.");
      return new PoolingHttpClientConnectionManager(
          SocketFactoryUtil.getTrustAllSocketFactoryRegistry());
    }

    // For standard SSL configuration, create a custom socket factory registry
    Registry<ConnectionSocketFactory> socketFactoryRegistry =
        createConnectionSocketFactoryRegistry(connectionContext);
    return new PoolingHttpClientConnectionManager(socketFactoryRegistry);
  }

  /**
   * Creates a registry of connection socket factories based on the connection context.
   *
   * @param connectionContext The connection context to use for configuration.
   * @return A configured Registry of ConnectionSocketFactory.
   * @throws DatabricksHttpException If there is an error during configuration.
   */
  public static Registry<ConnectionSocketFactory> createConnectionSocketFactoryRegistry(
      IDatabricksConnectionContext connectionContext) throws DatabricksHttpException {

    // First check if a custom trust store is specified
    if (connectionContext.getSSLTrustStore() != null) {
      return createRegistryWithCustomTrustStore(connectionContext);
    } else {
      return createRegistryWithSystemOrDefaultTrustStore(connectionContext);
    }
  }

  /**
   * Creates a socket factory registry using a custom trust store.
   *
   * @param connectionContext The connection context containing the trust store information.
   * @return A registry of connection socket factories.
   * @throws DatabricksHttpException If there is an error setting up the trust store.
   */
  private static Registry<ConnectionSocketFactory> createRegistryWithCustomTrustStore(
      IDatabricksConnectionContext connectionContext) throws DatabricksHttpException {

    try {
      KeyStore trustStore = loadTruststoreOrNull(connectionContext);
      if (trustStore == null) {
        String errorMessage =
            "Specified trust store could not be loaded: " + connectionContext.getSSLTrustStore();
        handleError(errorMessage, new IOException(errorMessage));
      }

      // Get trust anchors from custom store
      Set<TrustAnchor> trustAnchors = getTrustAnchorsFromTrustStore(trustStore);
      if (trustAnchors.isEmpty()) {
        String errorMessage =
            "Custom trust store contains no trust anchors. Certificate validation will fail.";
        handleError(errorMessage, new CertificateException(errorMessage));
      }

      LOGGER.info("Using custom trust store: " + connectionContext.getSSLTrustStore());

      return createRegistryFromTrustAnchors(
          trustAnchors,
          connectionContext,
          "custom trust store: " + connectionContext.getSSLTrustStore());
    } catch (Exception e) {
      handleError(
          "Error while setting up custom trust store: " + connectionContext.getSSLTrustStore(), e);
    }
    return null;
  }

  /**
   * Creates a socket factory registry using either the system property trust store or JDK default.
   *
   * @param connectionContext The connection context for configuration.
   * @return A registry of connection socket factories.
   * @throws DatabricksHttpException If there is an error during setup.
   */
  private static Registry<ConnectionSocketFactory> createRegistryWithSystemOrDefaultTrustStore(
      IDatabricksConnectionContext connectionContext) throws DatabricksHttpException {

    // Check if we should use the system property trust store based on useSystemTrustStore
    String sysTrustStore = null;
    if (connectionContext.useSystemTrustStore()) {
      // When useSystemTrustStore=true, check for javax.net.ssl.trustStore system property
      sysTrustStore = System.getProperty(JAVA_TRUST_STORE_PATH_PROPERTY);
    }

    // If system property is set and useSystemTrustStore=true, use that trust store
    if (sysTrustStore != null && !sysTrustStore.isEmpty()) {
      return createRegistryWithSystemPropertyTrustStore(connectionContext, sysTrustStore);
    }
    // No system property set or useSystemTrustStore=false, use JDK's default trust store (cacerts)
    else {
      return createRegistryWithJdkDefaultTrustStore(connectionContext);
    }
  }

  /**
   * Creates a socket factory registry using the trust store specified by system property.
   *
   * @param connectionContext The connection context for configuration.
   * @param sysTrustStore The path to the system property trust store.
   * @return A registry of connection socket factories.
   * @throws DatabricksHttpException If there is an error during setup.
   */
  private static Registry<ConnectionSocketFactory> createRegistryWithSystemPropertyTrustStore(
      IDatabricksConnectionContext connectionContext, String sysTrustStore)
      throws DatabricksHttpException {

    try {
      LOGGER.info(
          "Using system property javax.net.ssl.trustStore: "
              + sysTrustStore
              + " (This overrides the JDK's default cacerts store)");

      // Load the system property trust store
      File trustStoreFile = new File(sysTrustStore);
      if (!trustStoreFile.exists()) {
        String errorMessage = "System property trust store file does not exist: " + sysTrustStore;
        handleError(errorMessage, new IOException(errorMessage));
      }

      // Load the system property trust store
      KeyStore trustStore =
          KeyStore.getInstance(System.getProperty(JAVA_TRUST_STORE_TYPE_PROPERTY, "JKS"));
      char[] password = null;
      String passwordProp = System.getProperty(JAVA_TRUST_STORE_PASSWORD_PROPERTY);
      if (passwordProp != null) {
        password = passwordProp.toCharArray();
      }

      try (FileInputStream fis = new FileInputStream(sysTrustStore)) {
        trustStore.load(fis, password);
      }

      // Get trust anchors and create trust managers
      Set<TrustAnchor> trustAnchors = getTrustAnchorsFromTrustStore(trustStore);
      return createRegistryFromTrustAnchors(
          trustAnchors, connectionContext, "system property trust store: " + sysTrustStore);
    } catch (DatabricksHttpException
        | KeyStoreException
        | NoSuchAlgorithmException
        | CertificateException
        | IOException e) {
      handleError("Error while setting up system property trust store: " + sysTrustStore, e);
    }
    return null;
  }

  /**
   * Creates a socket factory registry using the JDK's default trust store (cacerts).
   *
   * @param connectionContext The connection context for configuration.
   * @return A registry of connection socket factories.
   * @throws DatabricksHttpException If there is an error during setup.
   */
  private static Registry<ConnectionSocketFactory> createRegistryWithJdkDefaultTrustStore(
      IDatabricksConnectionContext connectionContext) throws DatabricksHttpException {

    try {
      if (connectionContext.useSystemTrustStore()) {
        LOGGER.info(
            "No system property trust store found, using JDK default trust store (cacerts)");
      } else {
        LOGGER.info(
            "UseSystemTrustStore=false, using JDK default trust store (cacerts) and ignoring system properties");
      }

      Set<TrustAnchor> systemTrustAnchors = getTrustAnchorsFromTrustStore(null);
      return createRegistryFromTrustAnchors(
          systemTrustAnchors, connectionContext, "JDK default trust store (cacerts)");
    } catch (DatabricksHttpException e) {
      handleError("Error while setting up JDK default trust store", e);
    }
    return null;
  }

  /**
   * Creates a socket factory registry from trust anchors and client keystore if available.
   *
   * @param trustAnchors The trust anchors for server certificate validation.
   * @param connectionContext The connection context for configuration.
   * @param sourceDescription A description of the trust store source for logging.
   * @return A registry of connection socket factories.
   * @throws DatabricksHttpException If there is an error during setup.
   */
  private static Registry<ConnectionSocketFactory> createRegistryFromTrustAnchors(
      Set<TrustAnchor> trustAnchors,
      IDatabricksConnectionContext connectionContext,
      String sourceDescription)
      throws DatabricksHttpException {
    if (trustAnchors == null || trustAnchors.isEmpty()) {
      throw new DatabricksHttpException(
          sourceDescription + " contains no trust anchors",
          DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }

    try {
      // Create trust managers for server certificate validation
      TrustManager[] trustManagers =
          createTrustManagers(
              trustAnchors,
              connectionContext.checkCertificateRevocation(),
              connectionContext.acceptUndeterminedCertificateRevocation());

      // Load client certificate keystore if available
      KeyStore keyStore = loadKeystoreOrNull(connectionContext);

      if (keyStore != null) {
        LOGGER.info("Client certificate authentication enabled");

        // Get password for the keystore
        char[] keyStorePassword = null;
        if (connectionContext.getSSLKeyStorePassword() != null) {
          keyStorePassword = connectionContext.getSSLKeyStorePassword().toCharArray();
        }

        // Create key managers for client certificate authentication
        javax.net.ssl.KeyManager[] keyManagers = createKeyManagers(keyStore, keyStorePassword);

        return createSocketFactoryRegistry(trustManagers, keyManagers);
      } else {
        LOGGER.info("No client keystore configured, server certificate validation only");
        return createSocketFactoryRegistry(trustManagers);
      }
    } catch (Exception e) {
      handleError("Error setting up SSL socket factory for " + sourceDescription, e);
    }
    return null;
  }

  /**
   * Creates a socket factory registry with the provided trust managers.
   *
   * @param trustManagers The trust managers to use.
   * @return A registry of connection socket factories.
   * @throws NoSuchAlgorithmException If there is an error during SSL context creation.
   * @throws KeyManagementException If there is an error during SSL context creation.
   */
  private static Registry<ConnectionSocketFactory> createSocketFactoryRegistry(
      TrustManager[] trustManagers) throws NoSuchAlgorithmException, KeyManagementException {
    return createSocketFactoryRegistry(trustManagers, null);
  }

  /**
   * Creates key managers from the provided key store.
   *
   * @param keyStore The KeyStore containing client certificates and private keys.
   * @param keyStorePassword The password for the key store.
   * @return An array of key managers, or null if the key store is null.
   * @throws NoSuchAlgorithmException If the algorithm for the key manager factory is not available.
   * @throws KeyStoreException If there is an error accessing the key store.
   * @throws DatabricksHttpException If there is an error creating the key managers.
   */
  private static javax.net.ssl.KeyManager[] createKeyManagers(
      KeyStore keyStore, char[] keyStorePassword)
      throws NoSuchAlgorithmException, KeyStoreException, DatabricksHttpException {
    if (keyStore == null) {
      return null;
    }

    try {
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, keyStorePassword);
      LOGGER.info("Successfully initialized key managers for client certificate authentication");
      return kmf.getKeyManagers();
    } catch (UnrecoverableKeyException e) {
      String errorMessage = "Failed to initialize key managers: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new DatabricksHttpException(
          errorMessage, e, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
  }

  /**
   * Creates a socket factory registry with the provided trust managers and key managers.
   *
   * @param trustManagers The trust managers to use.
   * @param keyManagers The key managers to use for client authentication.
   * @return A registry of connection socket factories.
   * @throws NoSuchAlgorithmException If there is an error during SSL context creation.
   * @throws KeyManagementException If there is an error during SSL context creation.
   */
  private static Registry<ConnectionSocketFactory> createSocketFactoryRegistry(
      TrustManager[] trustManagers, javax.net.ssl.KeyManager[] keyManagers)
      throws NoSuchAlgorithmException, KeyManagementException {

    SSLContext sslContext = SSLContext.getInstance(DatabricksJdbcConstants.TLS);
    sslContext.init(keyManagers, trustManagers, new SecureRandom());
    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);

    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register(DatabricksJdbcConstants.HTTPS, sslSocketFactory)
        .register(DatabricksJdbcConstants.HTTP, new PlainConnectionSocketFactory())
        .build();
  }

  /**
   * Creates trust managers based on the provided trust anchors and settings.
   *
   * @param trustAnchors The trust anchors to use.
   * @param checkCertificateRevocation Whether to check certificate revocation.
   * @param acceptUndeterminedCertificateRevocation Whether to accept undetermined revocation
   *     status.
   * @return An array of trust managers.
   * @throws NoSuchAlgorithmException If there is an error during trust manager creation.
   * @throws InvalidAlgorithmParameterException If there is an error during trust manager creation.
   */
  private static TrustManager[] createTrustManagers(
      Set<TrustAnchor> trustAnchors,
      boolean checkCertificateRevocation,
      boolean acceptUndeterminedCertificateRevocation)
      throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, DatabricksHttpException {

    // Always use the custom trust manager with trust anchors
    CertPathTrustManagerParameters trustManagerParams =
        buildTrustManagerParameters(
            trustAnchors, checkCertificateRevocation, acceptUndeterminedCertificateRevocation);

    TrustManagerFactory customTmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    customTmf.init(trustManagerParams);

    LOGGER.info("Certificate revocation check: " + checkCertificateRevocation);
    return customTmf.getTrustManagers();
  }

  /**
   * Finds the X509TrustManager in an array of TrustManager objects.
   *
   * @param trustManagers Array of TrustManager objects to search.
   * @return The X509TrustManager if found, null otherwise.
   */
  private static X509TrustManager findX509TrustManager(TrustManager[] trustManagers) {
    if (trustManagers == null) {
      return null;
    }

    for (TrustManager tm : trustManagers) {
      if (tm instanceof X509TrustManager) {
        return (X509TrustManager) tm;
      }
    }

    return null;
  }

  /**
   * Loads a trust store from the path specified in the connection context.
   *
   * @param connectionContext The connection context containing trust store configuration.
   * @return The loaded KeyStore or null if it could not be loaded.
   * @throws DatabricksHttpException If there is an error during loading.
   */
  public static KeyStore loadTruststoreOrNull(IDatabricksConnectionContext connectionContext)
      throws DatabricksHttpException {
    String trustStorePath = connectionContext.getSSLTrustStore();
    if (trustStorePath == null) {
      return null;
    }

    // If the specified file doesn't exist, throw a specific error
    File trustStoreFile = new File(trustStorePath);
    if (!trustStoreFile.exists()) {
      String errorMessage = "Specified trust store file does not exist: " + trustStorePath;
      LOGGER.error(errorMessage);
      throw new DatabricksHttpException(
          errorMessage, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }

    char[] password = null;
    if (connectionContext.getSSLTrustStorePassword() != null) {
      password = connectionContext.getSSLTrustStorePassword().toCharArray();
    }

    String trustStoreType = connectionContext.getSSLTrustStoreType();

    try (FileInputStream trustStoreStream = new FileInputStream(trustStorePath)) {
      LOGGER.info("Loading trust store as type: " + trustStoreType);
      KeyStore trustStore = KeyStore.getInstance(trustStoreType);
      trustStore.load(trustStoreStream, password);
      LOGGER.info("Successfully loaded trust store: " + trustStorePath);
      return trustStore;
    } catch (Exception e) {
      String errorMessage =
          "Failed to load trust store: "
              + trustStorePath
              + " with type "
              + trustStoreType
              + ": "
              + e.getMessage();
      LOGGER.error(errorMessage);
      throw new DatabricksHttpException(
          errorMessage, e, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
  }

  /**
   * Loads a key store from the path specified in the connection context. The key store contains the
   * client's private key and certificate for client authentication.
   *
   * @param connectionContext The connection context containing key store configuration.
   * @return The loaded KeyStore or null if no key store was specified or it could not be loaded.
   * @throws DatabricksHttpException If there is an error during loading.
   */
  public static KeyStore loadKeystoreOrNull(IDatabricksConnectionContext connectionContext)
      throws DatabricksHttpException {
    String keyStorePath = connectionContext.getSSLKeyStore();
    if (keyStorePath == null) {
      return null;
    }

    // If the specified file doesn't exist, throw a specific error
    File keyStoreFile = new File(keyStorePath);
    if (!keyStoreFile.exists()) {
      String errorMessage = "Specified key store file does not exist: " + keyStorePath;
      LOGGER.error(errorMessage);
      throw new DatabricksHttpException(
          errorMessage, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }

    // Check if keystore password is provided, which is required for accessing private keys
    String keyStorePassword = connectionContext.getSSLKeyStorePassword();
    if (keyStorePassword == null) {
      String errorMessage =
          "Key store password is required when a key store is specified: " + keyStorePath;
      LOGGER.error(errorMessage);
      throw new DatabricksHttpException(
          errorMessage, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
    char[] password = keyStorePassword.toCharArray();

    String keyStoreType = connectionContext.getSSLKeyStoreType();
    String keyStoreProvider = connectionContext.getSSLKeyStoreProvider();

    try {
      LOGGER.info("Loading key store as type: " + keyStoreType);

      // Create KeyStore instance, with provider if specified
      KeyStore keyStore;
      if (keyStoreProvider != null && !keyStoreProvider.isEmpty()) {
        LOGGER.info("Using key store provider: " + keyStoreProvider);
        keyStore = KeyStore.getInstance(keyStoreType, keyStoreProvider);
      } else {
        keyStore = KeyStore.getInstance(keyStoreType);
      }

      // Load the KeyStore with password
      try (FileInputStream keyStoreStream = new FileInputStream(keyStorePath)) {
        keyStore.load(keyStoreStream, password);
      }

      // Verify that the keystore contains at least one private key entry
      boolean hasKeyEntry = false;
      try {
        for (Enumeration<String> aliases = keyStore.aliases(); aliases.hasMoreElements(); ) {
          String alias = aliases.nextElement();
          if (keyStore.isKeyEntry(alias)) {
            hasKeyEntry = true;
            LOGGER.debug("Found key entry with alias: " + alias);
            break;
          }
        }
      } catch (KeyStoreException e) {
        // Log but don't fail - we'll still return the keystore
        LOGGER.warn("Unable to verify key entries in keystore: " + e.getMessage());
      }

      if (!hasKeyEntry) {
        LOGGER.warn(
            "Key store does not contain any private key entries. "
                + "Client authentication may fail.");
      }

      LOGGER.info("Successfully loaded key store: " + keyStorePath);
      return keyStore;
    } catch (Exception e) {
      String errorMessage =
          "Failed to load key store: "
              + keyStorePath
              + " with type "
              + keyStoreType
              + ": "
              + e.getMessage();
      LOGGER.error(errorMessage);
      throw new DatabricksHttpException(
          errorMessage, e, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
  }

  /**
   * Extracts trust anchors from a KeyStore.
   *
   * @param trustStore The KeyStore from which to extract trust anchors.
   * @return A Set of TrustAnchor objects extracted from the KeyStore.
   * @throws DatabricksHttpException If there is an error during extraction.
   */
  public static Set<TrustAnchor> getTrustAnchorsFromTrustStore(KeyStore trustStore)
      throws DatabricksHttpException {
    try {
      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(trustStore);

      // Get the trust managers
      TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
      X509TrustManager x509TrustManager = findX509TrustManager(trustManagers);

      if (x509TrustManager == null || x509TrustManager.getAcceptedIssuers().length == 0) {
        // No trust anchors found
        return Collections.emptySet();
      }

      return Arrays.stream(x509TrustManager.getAcceptedIssuers())
          .map(cert -> new TrustAnchor(cert, null))
          .collect(Collectors.toSet());
    } catch (KeyStoreException | NoSuchAlgorithmException e) {
      handleError("Error while getting trust anchors from trust store: " + e.getMessage(), e);
    }
    return Collections.emptySet();
  }

  /**
   * Builds trust manager parameters for certificate path validation including certificate
   * revocation checking.
   *
   * @param trustAnchors The trust anchors to use in the trust manager.
   * @param checkCertificateRevocation Whether to check certificate revocation.
   * @param acceptUndeterminedCertificateRevocation Whether to accept undetermined certificate
   *     revocation status.
   * @return The trust manager parameters based on the input parameters.
   * @throws DatabricksHttpException If there is an error during configuration.
   */
  public static CertPathTrustManagerParameters buildTrustManagerParameters(
      Set<TrustAnchor> trustAnchors,
      boolean checkCertificateRevocation,
      boolean acceptUndeterminedCertificateRevocation)
      throws DatabricksHttpException {
    try {
      PKIXBuilderParameters pkixBuilderParameters =
          new PKIXBuilderParameters(trustAnchors, new X509CertSelector());
      pkixBuilderParameters.setRevocationEnabled(checkCertificateRevocation);

      if (checkCertificateRevocation) {
        CertPathValidator certPathValidator =
            CertPathValidator.getInstance(DatabricksJdbcConstants.PKIX);
        PKIXRevocationChecker revocationChecker =
            (PKIXRevocationChecker) certPathValidator.getRevocationChecker();

        if (acceptUndeterminedCertificateRevocation) {
          revocationChecker.setOptions(
              Set.of(
                  PKIXRevocationChecker.Option.SOFT_FAIL,
                  PKIXRevocationChecker.Option.NO_FALLBACK,
                  PKIXRevocationChecker.Option.PREFER_CRLS));
        }
        LOGGER.info(
            "Certificate revocation enabled. Undetermined revocation accepted: "
                + acceptUndeterminedCertificateRevocation);

        pkixBuilderParameters.addCertPathChecker(revocationChecker);
      }

      return new CertPathTrustManagerParameters(pkixBuilderParameters);
    } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
      handleError("Error while building trust manager parameters: " + e.getMessage(), e);
    }
    return null;
  }

  /**
   * Centralized error handling method for logging and throwing exceptions.
   *
   * @param errorMessage The error message to log.
   * @param e The exception to log and throw.
   * @throws DatabricksHttpException The wrapped exception.
   */
  private static void handleError(String errorMessage, Exception e) throws DatabricksHttpException {
    LOGGER.error(errorMessage, e);
    throw new DatabricksHttpException(
        errorMessage, e, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
  }
}
