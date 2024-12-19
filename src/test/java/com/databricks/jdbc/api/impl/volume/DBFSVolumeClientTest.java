package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.common.util.HttpUtil;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.model.client.filesystem.*;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DBFSVolumeClientTest {
  private static final String PRE_SIGNED_URL = "http://example.com/upload";

  @Mock private VolumeOperationProcessor mockProcessor;
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
  void testListObjects() throws Exception {
    ListResponse mockResponse = mock(ListResponse.class);
    FileInfo file1 = mock(FileInfo.class);
    FileInfo file2 = mock(FileInfo.class);

    // Stub the behavior of FileInfo::getPath
    when(file1.getPath()).thenReturn("/path/to/file1");
    when(file2.getPath()).thenReturn("/path/to/file2");

    doReturn(mockResponse).when(client).getListResponse(anyString());
    when(mockResponse.getFiles()).thenReturn(Arrays.asList(file1, file2));

    List<String> result = client.listObjects("catalog", "schema", "volume", "prefix", true);

    assertEquals(Arrays.asList("/path/to/file1", "/path/to/file2"), result);

    Mockito.verify(mockResponse).getFiles();
    Mockito.verify(file1).getPath();
    Mockito.verify(file2).getPath();
  }

  @Test
  void testGetObjectWithLocalPath() throws Exception {
    CreateDownloadUrlResponse mockResponse = mock(CreateDownloadUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateDownloadUrlResponse(any());

    doReturn(mockProcessor)
        .when(client)
        .getVolumeOperationProcessor(
            eq(HttpUtil.VOLUME_OPERATION_TYPE_GET),
            eq(PRE_SIGNED_URL),
            anyMap(),
            eq("localPath"),
            any(),
            eq(false),
            eq(null),
            any(),
            eq(null));

    doReturn(true).when(client).checkVolumeOperationStatus(any());

    boolean result = client.getObject("catalog", "schema", "volume", "objectPath", "localPath");

    assertTrue(result);
    verify(mockProcessor).process();
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
  void testPutObjectWithLocalPath() throws Exception {
    CreateUploadUrlResponse mockResponse = mock(CreateUploadUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateUploadUrlResponse(any());

    doReturn(mockProcessor)
        .when(client)
        .getVolumeOperationProcessor(
            eq(HttpUtil.VOLUME_OPERATION_TYPE_PUT),
            eq(PRE_SIGNED_URL),
            anyMap(),
            eq("localPath"),
            any(),
            eq(false),
            eq(null),
            any(),
            eq(null));

    doReturn(true).when(client).checkVolumeOperationStatus(any());

    boolean result =
        client.putObject("catalog", "schema", "volume", "objectPath", "localPath", true);

    assertTrue(result);
    verify(mockProcessor).process();
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
  void testPutObjectWithInputStream() throws Exception {
    CreateUploadUrlResponse mockResponse = mock(CreateUploadUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateUploadUrlResponse(any());

    doReturn(mockProcessor)
        .when(client)
        .getVolumeOperationProcessor(
            eq(HttpUtil.VOLUME_OPERATION_TYPE_PUT),
            eq(PRE_SIGNED_URL),
            anyMap(),
            eq(null),
            eq(null),
            eq(true),
            any(),
            any(),
            eq(null));

    doReturn(true).when(client).checkVolumeOperationStatus(any());

    File file = new File("/tmp/dbfs_test_put.txt");

    boolean result = false;
    try {
      Files.writeString(file.toPath(), "test-put-stream");
      System.out.println("File created");

      result =
          client.putObject(
              "catalog",
              "schema",
              "volume",
              "objectPath",
              new FileInputStream(file),
              file.length(),
              true);

    } finally {
      file.delete();
    }

    assertTrue(result);
    verify(mockProcessor).process();
  }

  @Test
  void testDeleteObject() throws Exception {
    CreateDeleteUrlResponse mockResponse = mock(CreateDeleteUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateDeleteUrlResponse(any());

    doReturn(mockProcessor)
        .when(client)
        .getVolumeOperationProcessor(
            eq(HttpUtil.VOLUME_OPERATION_TYPE_REMOVE),
            eq(PRE_SIGNED_URL),
            anyMap(),
            eq(null),
            eq(null),
            eq(true),
            eq(null),
            any(),
            eq(null));

    doReturn(true).when(client).checkVolumeOperationStatus(any());

    boolean result = client.deleteObject("catalog", "schema", "volume", "objectPath");

    assertTrue(result);
    verify(mockProcessor).process();
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
