package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.http.config.Registry;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SocketFactoryUtilTest {

  @Test
  void testGetTrustAllSocketFactoryRegistrySuccess() {
    Registry<ConnectionSocketFactory> registry =
        SocketFactoryUtil.getTrustAllSocketFactoryRegistry();

    assertNotNull(registry, "Registry should not be null");

    // Verify HTTP socket factory
    ConnectionSocketFactory httpFactory = registry.lookup("http");
    assertNotNull(httpFactory, "HTTP factory should not be null");
    assertInstanceOf(
        PlainConnectionSocketFactory.class,
        httpFactory,
        "HTTP factory should be instance of PlainConnectionSocketFactory");

    // Verify HTTPS socket factory
    ConnectionSocketFactory httpsFactory = registry.lookup("https");
    assertNotNull(httpsFactory, "HTTPS factory should not be null");
    assertInstanceOf(
        SSLConnectionSocketFactory.class,
        httpsFactory,
        "HTTPS factory should be instance of SSLConnectionSocketFactory");
  }
}
