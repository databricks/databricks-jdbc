package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksHttpTTransportTest {
  @Mock DatabricksHttpClient mockedHttpClient;
  @Mock CloseableHttpResponse mockResponse;
  @Mock StatusLine statusLine;
  private final String testUrl = "http://localhost:8080";

  @Test
  public void isOpen_AlwaysReturnsTrue() {
    DatabricksHttpTTransport transport = new DatabricksHttpTTransport(mockedHttpClient, testUrl);
    assertTrue(transport.isOpen());
  }

  @Test
  public void close_ClosesInputStreamWithoutError() {
    DatabricksHttpTTransport transport = new DatabricksHttpTTransport(mockedHttpClient, testUrl);
    transport.close();
    assertNull(transport.getInputStream());
    assertDoesNotThrow(() -> transport.open());
  }

  @Test
  public void setCustomHeaders_SetsHeadersCorrectly() throws TTransportException {
    DatabricksHttpTTransport transport = new DatabricksHttpTTransport(mockedHttpClient, testUrl);
    Map<String, String> headers = new HashMap<>();
    headers.put("Authorization", "Bearer abc123");
    transport.setCustomHeaders(headers);
    assertEquals("Bearer abc123", transport.getCustomHeaders().get("Authorization"));
    transport.setCustomHeaders(null);
    assertEquals(0, transport.getCustomHeaders().size());
    assertNull(transport.getConfiguration());
    assertDoesNotThrow(() -> transport.updateKnownMessageSize(0));
    assertDoesNotThrow(() -> transport.checkReadBytesAvailable(0));
  }

  @Test
  public void writeAndRead_ValidatesDataIntegrity() throws TTransportException {
    DatabricksHttpTTransport transport = new DatabricksHttpTTransport(mockedHttpClient, testUrl);
    byte[] testData = TEST_STRING.getBytes();
    transport.write(testData, 0, testData.length);
    transport.setInputStream(new ByteArrayInputStream(testData));
    byte[] buffer = new byte[testData.length];
    int bytesRead = transport.read(buffer, 0, buffer.length);
    assertEquals(testData.length, bytesRead);
    assertArrayEquals(testData, buffer);
    transport.close();
    assertNull(transport.getInputStream());
  }

  @Test
  public void flush_SendsDataCorrectly()
      throws DatabricksHttpException, IOException, TTransportException {
    DatabricksHttpTTransport transport = new DatabricksHttpTTransport(mockedHttpClient, testUrl);
    byte[] testData = TEST_STRING.getBytes();
    transport.write(testData, 0, testData.length);
    HttpEntity mockEntity = mock(HttpEntity.class);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(testData));
    when(mockedHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);

    transport.flush();
    ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);
    verify(mockedHttpClient).execute(requestCaptor.capture());
    HttpPost capturedRequest = requestCaptor.getValue();

    assertNotNull(capturedRequest.getEntity());
    assertTrue(capturedRequest.getEntity() instanceof ByteArrayEntity);
    assertEquals(testUrl, capturedRequest.getURI().toString());
    assertTrue(capturedRequest.containsHeader("Content-Type"));
  }
}
