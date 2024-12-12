package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import java.io.File;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class VolumeOperationProcessorDirectTest {

  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;useFileSystemAPI=1";
  private static final String operationUrl = "http://example.com/upload";
  private VolumeOperationProcessorDirect processor;
  @Mock IDatabricksHttpClient mockHttpClient;
  @Mock CloseableHttpResponse mockResponse;
  @Mock StatusLine mockStatusLine;
  @Mock DatabricksHttpClientFactory mockClientFactory;
  private MockedStatic<DatabricksHttpClientFactory> mockedFactory;
  private String localFilePath;
  private IDatabricksConnectionContext connectionContext;

  @BeforeEach
  void setUp() throws Exception {
    // Mock static methods
    mockedFactory = mockStatic(DatabricksHttpClientFactory.class);
    mockedFactory.when(DatabricksHttpClientFactory::getInstance).thenReturn(mockClientFactory);
    when(mockClientFactory.getClient(any())).thenReturn(mockHttpClient);

    java.util.Properties info = new java.util.Properties();
    connectionContext = DatabricksConnectionContextFactory.create(JDBC_URL, info);
  }

  @AfterEach
  void tearDown() {
    mockedFactory.close();
  }

  @Nested
  class PutOperationTest {
    @BeforeEach
    void setUpFile() throws Exception {
      // Create a temporary file to simulate the local file
      File tempFile = File.createTempFile("testfile", ".txt");
      localFilePath = tempFile.getAbsolutePath();

      // Initialize the processor
      processor =
          new VolumeOperationProcessorDirect(operationUrl, localFilePath, connectionContext);
    }

    @AfterEach
    void tearDownFile() throws Exception {
      // Delete the temporary file
      new File(localFilePath).delete();
    }

    @Test
    void testExecutePutOperation_Success() throws Exception {
      when(mockHttpClient.execute(any(HttpPut.class))).thenReturn(mockResponse);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(200); // HTTP 200 OK

      processor.executePutOperation();

      ArgumentCaptor<HttpPut> httpPutCaptor = ArgumentCaptor.forClass(HttpPut.class);
      verify(mockHttpClient).execute(httpPutCaptor.capture());

      HttpPut capturedHttpPut = httpPutCaptor.getValue();
      assertEquals(operationUrl, capturedHttpPut.getURI().toString());
      assertInstanceOf(FileEntity.class, capturedHttpPut.getEntity());
    }

    @Test
    void testExecutePutOperation_HttpFailure() throws Exception {
      when(mockHttpClient.execute(any(HttpPut.class))).thenReturn(mockResponse);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(500); // HTTP 500 Internal Server Error

      processor.executePutOperation();

      verify(mockHttpClient).execute(any(HttpPut.class));
    }

    @Test
    void testExecutePutOperation_IOException() throws Exception {
      when(mockHttpClient.execute(any(HttpPut.class)))
          .thenThrow(new DatabricksHttpException("IO error"));

      DatabricksVolumeOperationException exception =
          assertThrows(
              DatabricksVolumeOperationException.class, () -> processor.executePutOperation());

      assertTrue(exception.getMessage().contains("IO error"));
    }

    @Test
    void testExecutePutOperation_DatabricksHttpException() throws Exception {
      when(mockHttpClient.execute(any(HttpPut.class)))
          .thenThrow(new DatabricksHttpException("HTTP error"));

      DatabricksVolumeOperationException exception =
          assertThrows(
              DatabricksVolumeOperationException.class, () -> processor.executePutOperation());

      assertTrue(exception.getMessage().contains("HTTP error"));
    }
  }

  @Nested
  class GetOperationTest {

    @BeforeEach
    void setUpFile() throws Exception {
      localFilePath = "testfile.txt";
      // Initialize the processor
      processor =
          new VolumeOperationProcessorDirect(operationUrl, localFilePath, connectionContext);
    }

    @Test
    void testExecuteGetOperation_Success() throws Exception {
      when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(200); // HTTP 200 OK

      processor.executeGetOperation();

      ArgumentCaptor<HttpGet> httpGetCaptor = ArgumentCaptor.forClass(HttpGet.class);
      verify(mockHttpClient).execute(httpGetCaptor.capture());

      HttpGet capturedHttpGet = httpGetCaptor.getValue();
      assertEquals(operationUrl, capturedHttpGet.getURI().toString());
    }

    @Test
    void testExecuteGetOperation_HttpFailure() throws Exception {
      when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(500); // HTTP 500 Internal Server Error

      assertThrows(DatabricksVolumeOperationException.class, () -> processor.executeGetOperation());
    }
  }

  @Nested
  class DeleteOperationTest {

    @BeforeEach
    void setUpFile() throws Exception {
      processor = new VolumeOperationProcessorDirect(operationUrl, null, connectionContext);
    }

    @Test
    void testExecuteDeleteOperation_Success() throws Exception {
      when(mockHttpClient.execute(any(HttpDelete.class))).thenReturn(mockResponse);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(200); // HTTP 200 OK

      processor.executeDeleteOperation();

      ArgumentCaptor<HttpDelete> httpDeleteCaptor = ArgumentCaptor.forClass(HttpDelete.class);
      verify(mockHttpClient).execute(httpDeleteCaptor.capture());

      HttpDelete capturedHttpDelete = httpDeleteCaptor.getValue();
      assertEquals(operationUrl, capturedHttpDelete.getURI().toString());
    }

    @Test
    void testExecuteDeleteOperation_HttpFailure() throws Exception {
      when(mockHttpClient.execute(any(HttpDelete.class))).thenReturn(mockResponse);
      when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      when(mockStatusLine.getStatusCode()).thenReturn(500); // HTTP 500 Internal Server Error

      assertThrows(
          DatabricksVolumeOperationException.class, () -> processor.executeDeleteOperation());
    }
  }
}
