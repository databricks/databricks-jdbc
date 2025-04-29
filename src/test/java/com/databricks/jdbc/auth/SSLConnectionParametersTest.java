package com.databricks.jdbc.auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.SocketFactoryUtil;
import com.databricks.jdbc.dbclient.impl.common.ConfiguratorUtils;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.security.cert.X509Certificate;
import java.util.Properties;
import javax.net.ssl.X509TrustManager;
import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class SSLConnectionParametersTest {

  @Mock private IDatabricksConnectionContext mockContext;

  private Properties properties;

  @BeforeEach
  public void setUp() {
    mockContext = mock(IDatabricksConnectionContext.class);
    properties = new Properties();
  }

  @Test
  public void testGetBaseConnectionManagerWithDefaultSettings() throws DatabricksHttpException {
    when(mockContext.allowSelfSignedCerts()).thenReturn(false);
    when(mockContext.useSystemTrustStore()).thenReturn(false);
    when(mockContext.getSSLTrustStore()).thenReturn(null);
    when(mockContext.checkCertificateRevocation()).thenReturn(true);
    when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(false);

    PoolingHttpClientConnectionManager manager =
        ConfiguratorUtils.getBaseConnectionManager(mockContext);

    assertNotNull(manager, "Connection manager should not be null");
  }

  @Test
  public void testGetBaseConnectionManagerWithSelfSignedCerts() throws DatabricksHttpException {
    when(mockContext.allowSelfSignedCerts()).thenReturn(true);

    PoolingHttpClientConnectionManager manager =
        ConfiguratorUtils.getBaseConnectionManager(mockContext);

    assertNotNull(manager, "Connection manager should not be null");
  }

  @Test
  public void testGetBaseConnectionManagerWithCustomTrustStore() {
    when(mockContext.allowSelfSignedCerts()).thenReturn(false);
    when(mockContext.useSystemTrustStore()).thenReturn(false);
    when(mockContext.getSSLTrustStore()).thenReturn("/path/to/truststore.jks");
    when(mockContext.getSSLTrustStorePassword()).thenReturn("password");
    when(mockContext.getSSLTrustStoreType()).thenReturn("JKS");
    when(mockContext.checkCertificateRevocation()).thenReturn(true);
    when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(false);

    try {
      ConfiguratorUtils.getBaseConnectionManager(mockContext);
      fail("Should throw exception for non-existent trust store");
    } catch (DatabricksHttpException e) {
      assertTrue(
          e.getMessage()
              .contains("Error while setting up custom trust store: /path/to/truststore.jks"),
          "Exception should mention that there is an error while setting up custom trust store");
    }
  }

  @Test
  public void testGetTrustAllSocketFactoryRegistry() {
    Registry<ConnectionSocketFactory> registry =
        SocketFactoryUtil.getTrustAllSocketFactoryRegistry();

    assertNotNull(registry, "Trust-all socket factory registry should not be null");
    assertNotNull(registry.lookup("https"), "Registry should have entry for https");
    assertNotNull(registry.lookup("http"), "Registry should have entry for http");
  }

  @Test
  public void testGetConnectionSocketFactoryRegistryWithSelfSignedCerts()
      throws DatabricksHttpException {
    when(mockContext.allowSelfSignedCerts()).thenReturn(true);

    Registry<ConnectionSocketFactory> registry =
        ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);

    assertNotNull(registry, "Socket factory registry should not be null");
  }

  @Test
  public void testGetConnectionSocketFactoryRegistryWithSystemTrustStore() {
    when(mockContext.allowSelfSignedCerts()).thenReturn(false);
    when(mockContext.useSystemTrustStore()).thenReturn(true);
    when(mockContext.getSSLTrustStore()).thenReturn(null);
    when(mockContext.checkCertificateRevocation()).thenReturn(false);
    when(mockContext.acceptUndeterminedCertificateRevocation()).thenReturn(false);

    try {
      Registry<ConnectionSocketFactory> registry =
          ConfiguratorUtils.createConnectionSocketFactoryRegistry(mockContext);

      assertNotNull(registry, "Socket factory registry should not be null");
    } catch (Exception e) {
      fail("Should not throw exception with valid configuration: " + e.getMessage());
    }
  }

  @Test
  public void testAllPermutationsOfParameters() throws DatabricksSQLException {
    // Case 1: AllowSelfSignedCerts=false, UseSystemTrustStore=true (default)
    Properties props = new Properties();
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            "jdbc:databricks://hostname:443/default;httpPath=/sql/1.0/warehouses/123", props);
    assertFalse(context.allowSelfSignedCerts());
    assertFalse(context.useSystemTrustStore());

    // Case 2: AllowSelfSignedCerts=true, UseSystemTrustStore=false
    props = new Properties();
    props.setProperty("AllowSelfSignedCerts", "1");
    context =
        DatabricksConnectionContext.parse(
            "jdbc:databricks://hostname:443/default;httpPath=/sql/1.0/warehouses/123", props);
    assertTrue(context.allowSelfSignedCerts());
    assertFalse(context.useSystemTrustStore());

    // Case 3: AllowSelfSignedCerts=false, UseSystemTrustStore=false
    props = new Properties();
    props.setProperty("UseSystemTrustStore", "0");
    context =
        DatabricksConnectionContext.parse(
            "jdbc:databricks://hostname:443/default;httpPath=/sql/1.0/warehouses/123", props);
    assertFalse(context.allowSelfSignedCerts());
    assertFalse(context.useSystemTrustStore());

    // Case 4: AllowSelfSignedCerts=true, UseSystemTrustStore=false
    props = new Properties();
    props.setProperty("AllowSelfSignedCerts", "1");
    props.setProperty("UseSystemTrustStore", "0");
    context =
        DatabricksConnectionContext.parse(
            "jdbc:databricks://hostname:443/default;httpPath=/sql/1.0/warehouses/123", props);
    assertTrue(context.allowSelfSignedCerts());
    assertFalse(context.useSystemTrustStore());
  }

  @Test
  public void testTrustAllTrustManagerAcceptsAnyCertificate()
      throws NoSuchFieldException, IllegalAccessException {
    when(mockContext.allowSelfSignedCerts()).thenReturn(true);

    Registry<ConnectionSocketFactory> registry =
        SocketFactoryUtil.getTrustAllSocketFactoryRegistry();

    assertNotNull(registry, "Trust-all socket factory registry should not be null");
    assertNotNull(registry.lookup("https"), "Registry should have entry for https");

    X509TrustManager trustAllManager =
        (X509TrustManager) SocketFactoryUtil.getTrustManagerThatTrustsAllCertificates()[0];

    assertArrayEquals(
        trustAllManager.getAcceptedIssuers(),
        new X509Certificate[0],
        "Trust-all manager should return no accepted issuer");

    try {
      trustAllManager.checkServerTrusted(null, "RSA");
    } catch (Exception e) {
      fail("Trust-all manager should not validate certificates");
    }
  }
}
