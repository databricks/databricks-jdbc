package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.model.client.filesystem.*;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import java.io.InputStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DBFSVolumeClientTest {
  private static final String PRE_SIGNED_URL = "http://example.com/upload";

  @Mock private VolumeOperationProcessorDirect mockProcessor;
  @Mock private WorkspaceClient mockWorkSpaceClient;
  @Mock private ApiClient mockAPIClient;
  private DBFSVolumeClient client;

  @BeforeEach
  void setup() {
    client = new DBFSVolumeClient(mockWorkSpaceClient);
    client = spy(client);
  }

  @Test
  void testPrefixExists() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              client.prefixExists("catalog", "schema", "volume", "prefix", true);
            });
    assertEquals(
        "prefixExists function is unsupported in DBFSVolumeClient", exception.getMessage());
  }

  @Test
  void testObjectExists() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              client.objectExists("catalog", "schema", "volume", "objectPath", true);
            });
    assertEquals(
        "objectExists function is unsupported in DBFSVolumeClient", exception.getMessage());
  }

  @Test
  void testVolumeExists() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              client.volumeExists("catalog", "schema", "volumeName", true);
            });
    assertEquals(
        "volumeExists function is unsupported in DBFSVolumeClient", exception.getMessage());
  }

  @Test
  void testListObjects() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              client.listObjects("catalog", "schema", "volume", "prefix", true);
            });
    assertEquals("listObjects function is unsupported in DBFSVolumeClient", exception.getMessage());
  }

  @Test
  void testGetObjectWithLocalPath() throws Exception {
    CreateDownloadUrlResponse mockResponse = mock(CreateDownloadUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateDownloadUrlResponse(any());
    doReturn(mockProcessor)
        .when(client)
        .getVolumeOperationProcessorDirect(anyString(), anyString());

    boolean result = client.getObject("catalog", "schema", "volume", "objectPath", "localPath");

    assertTrue(result);
    verify(mockProcessor).executeGetOperation();
  }

  @Test
  void testGetObject_getCreateDownloadUrlResponseException() throws Exception {
    DatabricksVolumeOperationException mockException =
        new DatabricksVolumeOperationException("Mocked Exception");
    doThrow(mockException).when(client).getCreateDownloadUrlResponse(any());
    assertThrows(
        DatabricksVolumeOperationException.class,
        () -> client.getObject("catalog", "schema", "volume", "objectPath", "localPath"));
  }

  @Test
  void testGetCreateDownloadUrlResponse() throws Exception {
    CreateDownloadUrlResponse mockResponse = new CreateDownloadUrlResponse();

    when(client.workspaceClient.apiClient()).thenReturn(mockAPIClient);
    when(mockAPIClient.POST(anyString(), any(), eq(CreateDownloadUrlResponse.class), anyMap()))
        .thenReturn(mockResponse);

    CreateDownloadUrlResponse response = client.getCreateDownloadUrlResponse("path");
    assertEquals(response, mockResponse);
  }

  @Test
  void testGetCreateUploadUrlResponse() throws Exception {
    CreateUploadUrlResponse mockResponse = new CreateUploadUrlResponse();

    when(client.workspaceClient.apiClient()).thenReturn(mockAPIClient);
    when(mockAPIClient.POST(anyString(), any(), eq(CreateUploadUrlResponse.class), anyMap()))
        .thenReturn(mockResponse);

    CreateUploadUrlResponse response = client.getCreateUploadUrlResponse("path");
    assertEquals(response, mockResponse);
  }

  @Test
  void testGetCreateDeleteUrlResponse() throws Exception {
    CreateDeleteUrlResponse mockResponse = new CreateDeleteUrlResponse();

    when(client.workspaceClient.apiClient()).thenReturn(mockAPIClient);
    when(mockAPIClient.POST(anyString(), any(), eq(CreateDeleteUrlResponse.class), anyMap()))
        .thenReturn(mockResponse);

    CreateDeleteUrlResponse response = client.getCreateDeleteUrlResponse("path");
    assertEquals(response, mockResponse);
  }

  @Test
  void testGetObjectReturningInputStreamEntity() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              client.getObject("catalog", "schema", "volume", "objectPath");
            });
    assertEquals(
        "getObject returning InputStreamEntity function is unsupported in DBFSVolumeClient",
        exception.getMessage());
  }

  @Test
  void testPutObjectWithLocalPath() throws Exception {
    CreateUploadUrlResponse mockResponse = mock(CreateUploadUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateUploadUrlResponse(any());
    doReturn(mockProcessor)
        .when(client)
        .getVolumeOperationProcessorDirect(anyString(), anyString());

    boolean result =
        client.putObject("catalog", "schema", "volume", "objectPath", "localPath", true);

    assertTrue(result);
    verify(mockProcessor).executePutOperation();
  }

  @Test
  void testPutObjectWithLocalPath_getCreateUploadUrlResponseException() throws Exception {
    DatabricksVolumeOperationException mockException =
        new DatabricksVolumeOperationException("Mocked Exception");
    doThrow(mockException).when(client).getCreateUploadUrlResponse(any());
    assertThrows(
        DatabricksVolumeOperationException.class,
        () -> client.putObject("catalog", "schema", "volume", "objectPath", "localPath", true));
  }

  @Test
  void testPutObjectWithInputStream() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              client.putObject(
                  "catalog", "schema", "volume", "objectPath", mock(InputStream.class), 100L, true);
            });
    assertEquals(
        "putObject for InputStream function is unsupported in DBFSVolumeClient",
        exception.getMessage());
  }

  @Test
  void testDeleteObject() throws Exception {
    CreateDeleteUrlResponse mockResponse = mock(CreateDeleteUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateDeleteUrlResponse(any());
    doReturn(mockProcessor).when(client).getVolumeOperationProcessorDirect(anyString(), isNull());

    boolean result = client.deleteObject("catalog", "schema", "volume", "objectPath");

    assertTrue(result);
    verify(mockProcessor).executeDeleteOperation();
  }

  @Test
  void testDeleteObject_getCreateDeleteUrlResponseException() throws Exception {
    DatabricksVolumeOperationException mockException =
        new DatabricksVolumeOperationException("Mocked Exception");
    doThrow(mockException).when(client).getCreateDeleteUrlResponse(any());
    assertThrows(
        DatabricksVolumeOperationException.class,
        () -> client.deleteObject("catalog", "schema", "volume", "objectPath"));
  }
}
