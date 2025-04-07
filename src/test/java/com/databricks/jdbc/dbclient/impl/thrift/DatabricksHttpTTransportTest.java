package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.dbclient.impl.common.TracingUtil;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.sdk.core.DatabricksConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
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

  private static final String NEW_ACCESS_TOKEN = "new-access-token";
  private static final String testUrl = "http://localhost:8080";
  @Mock DatabricksHttpClient mockedHttpClient;
  @Mock CloseableHttpResponse mockResponse;
  @Mock StatusLine mockStatusLine;
  @Mock DatabricksConfig mockDatabricksConfig;
  @Mock IDatabricksConnectionContext mockConnectionContext;

  @Test
  public void isOpen_AlwaysReturnsTrue() {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            mockedHttpClient, testUrl, mockDatabricksConfig, mockConnectionContext);
    assertTrue(transport.isOpen());
  }

  @Test
  public void close_ClosesInputStreamWithoutError() {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            mockedHttpClient, testUrl, mockDatabricksConfig, mockConnectionContext);
    transport.close();
    assertDoesNotThrow(transport::open);
  }

  @Test
  public void writeAndRead_ValidatesDataIntegrity() throws TTransportException {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            mockedHttpClient, testUrl, mockDatabricksConfig, mockConnectionContext);
    byte[] testData = TEST_STRING.getBytes();
    transport.write(testData, 0, testData.length);
    transport.setResponseBuffer(new ByteArrayInputStream(testData));
    byte[] buffer = new byte[testData.length];
    int bytesRead = transport.read(buffer, 0, buffer.length);
    assertEquals(testData.length, bytesRead);
    assertArrayEquals(testData, buffer);
    transport.close();
  }

  @Test
  public void flush_SendsDataCorrectly()
      throws DatabricksHttpException, IOException, TTransportException {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            mockedHttpClient, testUrl, mockDatabricksConfig, mockConnectionContext);
    byte[] testData = TEST_STRING.getBytes();
    transport.write(testData, 0, testData.length);
    HttpEntity mockEntity = mock(HttpEntity.class);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(testData));
    when(mockedHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockConnectionContext.isRequestTracingEnabled()).thenReturn(false);

    transport.flush();
    ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);
    verify(mockedHttpClient).execute(requestCaptor.capture());
    HttpPost capturedRequest = requestCaptor.getValue();

    assertNotNull(capturedRequest.getEntity());
    assertInstanceOf(ByteArrayEntity.class, capturedRequest.getEntity());
    assertEquals(testUrl, capturedRequest.getURI().toString());
    assertTrue(capturedRequest.containsHeader("Content-Type"));
    assertFalse(capturedRequest.containsHeader(TracingUtil.TRACE_HEADER));
  }

  @Test
  public void flush_SendsDataCorrectly_tracingEnabled()
      throws DatabricksHttpException, IOException, TTransportException {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            mockedHttpClient, testUrl, mockDatabricksConfig, mockConnectionContext);
    byte[] testData = TEST_STRING.getBytes();
    transport.write(testData, 0, testData.length);
    HttpEntity mockEntity = mock(HttpEntity.class);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(testData));
    when(mockedHttpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
    when(mockConnectionContext.isRequestTracingEnabled()).thenReturn(true);

    transport.flush();
    ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor.forClass(HttpPost.class);
    verify(mockedHttpClient).execute(requestCaptor.capture());
    HttpPost capturedRequest = requestCaptor.getValue();

    assertNotNull(capturedRequest.getEntity());
    assertInstanceOf(ByteArrayEntity.class, capturedRequest.getEntity());
    assertEquals(testUrl, capturedRequest.getURI().toString());
    assertTrue(capturedRequest.containsHeader("Content-Type"));
    assertTrue(capturedRequest.containsHeader(TracingUtil.TRACE_HEADER));
  }

  @Test
  public void resetAccessToken_UpdatesConfigCorrectly() {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            mockedHttpClient, testUrl, mockDatabricksConfig, mockConnectionContext);

    when(mockDatabricksConfig.getHost()).thenReturn(testUrl);

    transport.resetAccessToken(NEW_ACCESS_TOKEN);

    assertEquals(NEW_ACCESS_TOKEN, transport.databricksConfig.getToken());
    assertEquals(testUrl, transport.databricksConfig.getHost());
    assertEquals(
        DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE, transport.databricksConfig.getAuthType());
    assertNotNull(transport.databricksConfig.getHttpClient());
  }
}
