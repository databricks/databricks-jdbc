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
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.*;
import java.util.Arrays;
import java.util.Collections;
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
        getConnectionSocketFactoryRegistry(connectionContext);
    return new PoolingHttpClientConnectionManager(socketFactoryRegistry);
  }

  /**
   * Creates a registry of connection socket factories based on the connection context.
   *
   * @param connectionContext The connection context to use for configuration.
   * @return A configured Registry of ConnectionSocketFactory.
   * @throws DatabricksHttpException If there is an error during configuration.
   */
  public static Registry<ConnectionSocketFactory> getConnectionSocketFactoryRegistry(
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
        LOGGER.error(errorMessage);
        throw new DatabricksHttpException(
            errorMessage, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
      }

      // Get trust anchors from custom store
      Set<TrustAnchor> trustAnchors = getTrustAnchorsFromTrustStore(trustStore);
      if (trustAnchors.isEmpty()) {
        String errorMessage =
            "Custom trust store contains no trust anchors. Certificate validation will fail.";
        LOGGER.error(errorMessage);
        throw new DatabricksHttpException(
            errorMessage, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
      }

      LOGGER.info("Using custom trust store: " + connectionContext.getSSLTrustStore());

      // Create trust managers from trust store
      TrustManager[] trustManagers =
          createTrustManagers(
              trustStore,
              trustAnchors,
              connectionContext.checkCertificateRevocation(),
              connectionContext.acceptUndeterminedCertificateRevocation());

      // Create socket factory registry
      return createSocketFactoryRegistry(trustManagers);
    } catch (Exception e) {
      String errorMessage =
          "Error while setting up custom trust store: " + connectionContext.getSSLTrustStore();
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(
          errorMessage, e, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
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
      sysTrustStore = System.getProperty("javax.net.ssl.trustStore");
    }

    // If system property is set and useSystemTrustStore=true, use that trust store
    if (sysTrustStore != null && !sysTrustStore.isEmpty()) {
      return createRegistryWithSystemPropertyTrustStore(sysTrustStore);
    }
    // No system property set or useSystemTrustStore=false, use JDK's default trust store (cacerts)
    else {
      return createRegistryWithJdkDefaultTrustStore(connectionContext);
    }
  }

  /**
   * Creates a socket factory registry using the trust store specified by system property.
   *
   * @param sysTrustStore The path to the system property trust store.
   * @return A registry of connection socket factories.
   * @throws DatabricksHttpException If there is an error during setup.
   */
  private static Registry<ConnectionSocketFactory> createRegistryWithSystemPropertyTrustStore(
      String sysTrustStore) throws DatabricksHttpException {

    try {
      LOGGER.info(
          "Using system property javax.net.ssl.trustStore: "
              + sysTrustStore
              + " (This overrides the JDK's default cacerts store)");
      // Let the default SSLContext handle this since it respects system properties
      SSLContext sslContext = SSLContext.getInstance(DatabricksJdbcConstants.TLS);
      sslContext.init(null, null, null);
      SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);

      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register(DatabricksJdbcConstants.HTTPS, sslSocketFactory)
          .register(DatabricksJdbcConstants.HTTP, new PlainConnectionSocketFactory())
          .build();
    } catch (Exception e) {
      String errorMessage = "Error while setting up system property trust store: " + sysTrustStore;
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(
          errorMessage, e, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
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

      // Explicitly initialize with default trust managers from JDK's cacerts
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init((KeyStore) null); // null uses the JDK's default trust store (cacerts)

      // Configure certificate revocation checking if enabled
      TrustManager[] trustManagers = tmf.getTrustManagers();
      if (connectionContext.checkCertificateRevocation()) {
        trustManagers =
            configureCertificateRevocationForDefaultTrustStore(
                trustManagers, connectionContext.acceptUndeterminedCertificateRevocation());
      }

      return createSocketFactoryRegistry(trustManagers);
    } catch (Exception e) {
      String errorMessage = "Error while setting up JDK default trust store";
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(
          errorMessage, e, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
  }

  /**
   * Creates a socket factory registry with the provided trust managers.
   *
   * @param trustManagers The trust managers to use.
   * @return A registry of connection socket factories.
   * @throws Exception If there is an error during SSL context creation.
   */
  private static Registry<ConnectionSocketFactory> createSocketFactoryRegistry(
      TrustManager[] trustManagers) throws Exception {

    SSLContext sslContext = SSLContext.getInstance(DatabricksJdbcConstants.TLS);
    sslContext.init(null, trustManagers, null);
    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);

    return RegistryBuilder.<ConnectionSocketFactory>create()
        .register(DatabricksJdbcConstants.HTTPS, sslSocketFactory)
        .register(DatabricksJdbcConstants.HTTP, new PlainConnectionSocketFactory())
        .build();
  }

  /**
   * Creates trust managers based on the provided trust store and settings.
   *
   * @param trustStore The trust store to use.
   * @param trustAnchors The trust anchors from the trust store.
   * @param checkCertificateRevocation Whether to check certificate revocation.
   * @param acceptUndeterminedRevocation Whether to accept undetermined revocation status.
   * @return An array of trust managers.
   * @throws Exception If there is an error during trust manager creation.
   */
  private static TrustManager[] createTrustManagers(
      KeyStore trustStore,
      Set<TrustAnchor> trustAnchors,
      boolean checkCertificateRevocation,
      boolean acceptUndeterminedRevocation)
      throws Exception {

    if (checkCertificateRevocation) {
      // Configure with certificate revocation checking
      CertPathTrustManagerParameters trustManagerParams =
          buildTrustManagerParameters(
              trustAnchors, checkCertificateRevocation, acceptUndeterminedRevocation);

      TrustManagerFactory customTmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      customTmf.init(trustManagerParams);
      LOGGER.info("Certificate revocation checking enabled with custom trust store");
      return customTmf.getTrustManagers();
    } else {
      // Standard trust manager without revocation checking
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
      return tmf.getTrustManagers();
    }
  }

  /**
   * Configures certificate revocation checking for the JDK default trust store.
   *
   * @param trustManagers The trust managers from the JDK default trust store.
   * @param acceptUndeterminedRevocation Whether to accept undetermined revocation status.
   * @return An array of trust managers configured for certificate revocation checking.
   * @throws Exception If there is an error during configuration.
   */
  private static TrustManager[] configureCertificateRevocationForDefaultTrustStore(
      TrustManager[] trustManagers, boolean acceptUndeterminedRevocation) throws Exception {

    try {
      // Get trust anchors from JDK's default trust store
      X509TrustManager x509TrustManager = findX509TrustManager(trustManagers);
      if (x509TrustManager != null) {
        Set<TrustAnchor> systemTrustAnchors =
            Arrays.stream(x509TrustManager.getAcceptedIssuers())
                .map(cert -> new TrustAnchor(cert, null))
                .collect(Collectors.toSet());

        // Build trust manager parameters with revocation checking
        CertPathTrustManagerParameters trustManagerParams =
            buildTrustManagerParameters(systemTrustAnchors, true, acceptUndeterminedRevocation);

        TrustManagerFactory customTmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        customTmf.init(trustManagerParams);
        LOGGER.info("Certificate revocation checking enabled with JDK default trust store");
        return customTmf.getTrustManagers();
      }
      return trustManagers;
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to set up certificate revocation checking with JDK default trust store: "
              + e.getMessage());
      // Fall back to default trust managers if revocation checking setup fails
      if (!acceptUndeterminedRevocation) {
        throw new Exception(
            "Certificate revocation checking failed to initialize and strict checking is enabled");
      }
      return trustManagers;
    }
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
   * Loads a trust store from the path specified in the connection context. Tries multiple formats
   * in this order: 1. The format specified in the connection context 2. PKCS12 3. JKS
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

    // Define the types to try, in order of preference
    String[] typesToTry =
        new String[] {
          connectionContext.getSSLTrustStoreType(), // User-specified type (might be null/empty)
          "PKCS12", // Standard PKCS12 format
          "JKS" // Java KeyStore format
        };

    // Skip the first type if it's null or empty
    int startIndex = (typesToTry[0] == null || typesToTry[0].isEmpty()) ? 1 : 0;

    for (int i = startIndex; i < typesToTry.length; i++) {
      String trustStoreType = typesToTry[i];
      try (FileInputStream trustStoreStream = new FileInputStream(trustStorePath)) {
        LOGGER.info("Attempting to load trust store as type: " + trustStoreType);
        KeyStore trustStore = KeyStore.getInstance(trustStoreType);
        trustStore.load(trustStoreStream, password);
        LOGGER.info("Successfully loaded trust store as type: " + trustStoreType);
        return trustStore;
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to load trust store as type "
                + trustStoreType
                + (i < typesToTry.length - 1 ? ", will try next type" : ""));
      }
    }

    String errorMessage = "Failed to load trust store using any of the supported types";
    LOGGER.error(errorMessage);
    throw new DatabricksHttpException(errorMessage, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
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
      if (trustStore == null) {
        return Collections.emptySet();
      }

      // Create a trust manager factory
      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(trustStore);

      // Get the trust managers
      TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
      if (trustManagers == null || trustManagers.length == 0) {
        LOGGER.warn("No trust managers found in the trust store");
        return Collections.emptySet();
      }

      // Find the X509TrustManager
      X509TrustManager x509TrustManager = findX509TrustManager(trustManagers);
      if (x509TrustManager == null) {
        LOGGER.warn("No X509TrustManager found in the trust store");
        return Collections.emptySet();
      }

      // Get the accepted issuers (trust anchors)
      X509Certificate[] acceptedIssuers = x509TrustManager.getAcceptedIssuers();
      if (acceptedIssuers == null || acceptedIssuers.length == 0) {
        LOGGER.warn("No accepted issuers found in the X509TrustManager");
        return Collections.emptySet();
      }

      // Convert certificates to trust anchors
      Set<TrustAnchor> trustAnchors =
          Arrays.stream(acceptedIssuers)
              .map(cert -> new TrustAnchor(cert, null))
              .collect(Collectors.toSet());

      LOGGER.info("Found " + trustAnchors.size() + " trust anchors in the trust store");
      return trustAnchors;
    } catch (Exception e) {
      String errorMessage = "Error while getting trust anchors from trust store";
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(
          errorMessage, e, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
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

        pkixBuilderParameters.addCertPathChecker(revocationChecker);
      }

      return new CertPathTrustManagerParameters(pkixBuilderParameters);
    } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
      String errorMessage = "Error while building trust manager parameters";
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(
          errorMessage, e, DatabricksDriverErrorCode.SSL_HANDSHAKE_ERROR);
    }
  }
}
