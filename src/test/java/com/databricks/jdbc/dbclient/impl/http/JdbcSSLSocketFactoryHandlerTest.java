package com.databricks.jdbc.client.http;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.JdbcSslSocketFactoryHandler;
import java.util.Optional;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class JdbcSSLSocketFactoryHandlerTest {
  IDatabricksConnectionContext connectionContext = Mockito.mock(IDatabricksConnectionContext.class);

  @Test
  public void testHttpGetAndCheckCertificate() throws Exception {
    when(connectionContext.checkCertificateRevocation()).thenReturn(false);
    when(connectionContext.acceptUndeterminedCertificateRevocation()).thenReturn(true);
    // Create an SSLContext with our existing CustomX509TrustManager
    JdbcSslSocketFactoryHandler handler = new JdbcSslSocketFactoryHandler(connectionContext);

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
