package com.databricks.jdbc.client.http;

import com.databricks.jdbc.client.impl.helper.JdbcSSLSocketFactoryHandler;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;

import java.util.Optional;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class JdbcSSLSocketFactoryHandlerTest {
  IDatabricksConnectionContext connectionContext = Mockito.mock(IDatabricksConnectionContext.class);

  @Test
  public void testHttpGetAndCheckCertificate() throws Exception {
    when(connectionContext.checkCertificateRevocation()).thenReturn(true);
    when(connectionContext.acceptUndeterminedCertificateRevocation()).thenReturn(true);
    // Create an SSLContext with our existing CustomX509TrustManager
    JdbcSSLSocketFactoryHandler handler = new JdbcSSLSocketFactoryHandler(connectionContext);

    Optional<SSLConnectionSocketFactory> sslSocketFactory = handler.getCustomSSLSocketFactory();

    HttpClientBuilder httpClientBuilder = HttpClients.custom();
    sslSocketFactory.ifPresent(httpClientBuilder::setSSLSocketFactory);
    CloseableHttpClient httpClient = httpClientBuilder.build();

    HttpGet httpGet = new HttpGet("https://revoked.badssl.com/");
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      System.out.println(response.toString());
      assertNotNull(response);
      assertTrue(response.getStatusLine().getStatusCode() == 200);

      // Validate that the certificate was used
      // Note: HttpClient does not provide direct access to the server's certificate
      // during a request, so you need to validate this indirectly
    }
  }
}
