package com.databricks.jdbc.driver;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DatabricksCommonHttpClientTest {

  @Mock private SSLContext sslContext;

  @Mock private SSLSocketFactory mockedSSLSocketFactory;

  @Mock private CloseableHttpClient mockedHttpClient;

  @Mock private CloseableHttpResponse mockedResponse;

  @Mock private StatusLine statusLine;

  @Mock private HttpEntity entity;

  private DatabricksCommonHttpClient httpClientWithSSL;
  private DatabricksCommonHttpClient httpClientWithoutSSL;

  @BeforeEach
  void setUp() throws Exception {
    // Common mock configurations
    when(sslContext.getSocketFactory()).thenReturn(mockedSSLSocketFactory);

    // Create instances for testing
    httpClientWithoutSSL = new DatabricksCommonHttpClient(10);
    httpClientWithSSL = new DatabricksCommonHttpClient(10, sslContext);
  }

  @Test
  void testConstructorWithoutSSL() {
    // Assertions to ensure the httpClientWithoutSSL was configured without SSL (not easily directly
    // testable without reflection or breaking encapsulation)
    assertNotNull(httpClientWithoutSSL, "HttpClient instance should be created");
  }

  @Test
  void testConstructorWithSSL() {
    // Assertions to ensure the httpClientWithSSL was configured with SSL (similarly challenging to
    // test directly)
    assertNotNull(httpClientWithSSL, "HttpClient instance should be created with SSL context");
  }

  @Test
  void testExecuteGetRequest() throws Exception {
    when(mockedResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(statusLine.getReasonPhrase()).thenReturn("OK");
    when(mockedResponse.getEntity()).thenReturn(entity);
    when(entity.getContent())
        .thenReturn(new ByteArrayInputStream("response body".getBytes(StandardCharsets.UTF_8)));
    when(mockedResponse.getAllHeaders())
        .thenReturn(new BasicHeader[] {new BasicHeader("Content-Type", "application/json")});

    // Setup HttpClient to use our mocked response
    CloseableHttpClient closeableHttpClient = mock(CloseableHttpClient.class);
    when(closeableHttpClient.execute(any())).thenReturn(mockedResponse);

    // Use reflection or a factory method for testing to inject the mocked CloseableHttpClient
    setInternalState(httpClientWithoutSSL, "hc", closeableHttpClient);

    // Prepare and execute the request
    Request request = new Request("GET", "http://test.com");
    Response response = httpClientWithoutSSL.execute(request);

    // Assertions
    assertNotNull(response, "Response should not be null");
    assertEquals(200, response.getStatusCode(), "Status code should be 200");
    assertEquals("OK", response.getStatus(), "Status text should be OK");
    verify(closeableHttpClient, times(1)).execute(any());
  }

  // Utility method to set private fields for the purpose of injecting mocks
  private void setInternalState(Object target, String fieldName, Object value) {
    try {
      var field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
