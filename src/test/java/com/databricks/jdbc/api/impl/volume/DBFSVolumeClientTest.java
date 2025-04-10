package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.model.client.filesystem.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.error.platform.NotFound;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DBFSVolumeClientTest {
  private static final String PRE_SIGNED_URL = "http://example.com/upload";

  @Mock private VolumeOperationProcessor mockProcessor;
  @Mock private WorkspaceClient mockWorkSpaceClient;
  @Mock private ApiClient mockAPIClient;
  private DBFSVolumeClient client;
  private VolumeOperationProcessor.Builder processorBuilder;

  @TempDir private File tempFolder;

  @BeforeEach
  void setup() {
    // DBFS Client Spy
    when(mockWorkSpaceClient.apiClient()).thenReturn(mockAPIClient);
    client = spy(new DBFSVolumeClient(mockWorkSpaceClient));
  }

  @Test
  void testPrefixExists() throws Exception {
    // Case 1: When prefix is empty, should return false.
    assertFalse(client.prefixExists("catalog", "schema", "volume", "", true));

    // Case 2: Valid non-empty prefix with a matching file.
    ListResponse responseMatch = mock(ListResponse.class);
    FileInfo matchingFile = mock(FileInfo.class);
    // File base name extracted from the path is "file123.txt"
    when(matchingFile.getPath()).thenReturn("/Volumes/catalog/schema/volume/file123.txt");
    when(responseMatch.getFiles()).thenReturn(Arrays.asList(matchingFile));
    doReturn(responseMatch).when(client).getListResponse(anyString());
    // "file" should match "file123.txt" in a case-sensitive manner.
    assertTrue(client.prefixExists("catalog", "schema", "volume", "file", true));

    // Case 3: Non-empty prefix with no matching file.
    ListResponse responseNoMatch = mock(ListResponse.class);
    FileInfo nonMatchingFile = mock(FileInfo.class);
    when(nonMatchingFile.getPath()).thenReturn("/Volumes/catalog/schema/volume/other.txt");
    when(responseNoMatch.getFiles()).thenReturn(Arrays.asList(nonMatchingFile));
    doReturn(responseNoMatch).when(client).getListResponse(anyString());
    assertFalse(client.prefixExists("catalog", "schema", "volume", "file", true));

    // Case 4: getListResponse throws an exception.
    doThrow(
            new DatabricksVolumeOperationException(
                "Failed to get list response",
                DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE))
        .when(client)
        .getListResponse(anyString());
    DatabricksVolumeOperationException ex =
        assertThrows(
            DatabricksVolumeOperationException.class,
            () -> client.prefixExists("catalog", "schema", "volume", "file", true));
    assertTrue(ex.getMessage().contains("Error checking prefix existence"));
  }

  @Test
  void testObjectExists() throws Exception {
    // Case 1: Exact match exists.
    ListResponse responseMatch = mock(ListResponse.class);
    FileInfo fileMatch = mock(FileInfo.class);
    // Assume objectPath "dir/file.txt" -> base name "file.txt"
    when(fileMatch.getPath()).thenReturn("/Volumes/catalog/schema/volume/dir/file.txt");
    when(responseMatch.getFiles()).thenReturn(Arrays.asList(fileMatch));
    doReturn(responseMatch).when(client).getListResponse(anyString());
    assertTrue(client.objectExists("catalog", "schema", "volume", "dir/file.txt", true));

    // Case 2: No matching object exists.
    ListResponse responseNoMatch = mock(ListResponse.class);
    FileInfo fileNoMatch = mock(FileInfo.class);
    when(fileNoMatch.getPath()).thenReturn("/Volumes/catalog/schema/volume/dir/other.txt");
    when(responseNoMatch.getFiles()).thenReturn(Arrays.asList(fileNoMatch));
    doReturn(responseNoMatch).when(client).getListResponse(anyString());
    assertFalse(client.objectExists("catalog", "schema", "volume", "dir/file.txt", true));

    // Case 3: Case-insensitive match: different case should match.
    ListResponse responseDiffCase = mock(ListResponse.class);
    FileInfo fileDiffCase = mock(FileInfo.class);
    when(fileDiffCase.getPath()).thenReturn("/Volumes/catalog/schema/volume/dir/File.TXT");
    when(responseDiffCase.getFiles()).thenReturn(Arrays.asList(fileDiffCase));
    doReturn(responseDiffCase).when(client).getListResponse(anyString());
    assertTrue(client.objectExists("catalog", "schema", "volume", "dir/file.txt", false));

    // Case 4: getListResponse throws an exception.
    doThrow(
            new DatabricksVolumeOperationException(
                "Failed to get list response",
                DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE))
        .when(client)
        .getListResponse(anyString());
    DatabricksVolumeOperationException ex =
        assertThrows(
            DatabricksVolumeOperationException.class,
            () -> client.objectExists("catalog", "schema", "volume", "dir/file.txt", true));
    assertTrue(ex.getMessage().contains("Error checking object existence"));
  }

  @Test
  void testVolumeExists() throws Exception {
    // Case 1: Volume exists.
    reset(client);
    ListResponse responseMatch = Mockito.mock(ListResponse.class);
    doReturn(responseMatch).when(client).getListResponse(anyString());
    assertTrue(client.volumeExists("catalog", "schema", "VolumeA", true));

    // Case 2: Volume does not exist.
    reset(client);
    doThrow(
            new DatabricksVolumeOperationException(
                "Failed to get list response - {Volume 'catalog.schema.VolumeA' does not exist.}",
                new NotFound("Volume 'catalog.schema.VolumeA' does not exist.", new ArrayList<>()),
                DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE))
        .when(client)
        .getListResponse(anyString());
    // When the exception message indicates the volume doesn't exist, volumeExists should return
    // false.
    assertFalse(client.volumeExists("catalog", "schema", "VolumeA", true));

    // Case 3: Error case
    reset(client);
    doThrow(
            new DatabricksVolumeOperationException(
                "Simulated error",
                new Exception(),
                DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE))
        .when(client)
        .getListResponse(anyString());
    DatabricksVolumeOperationException ex =
        assertThrows(
            DatabricksVolumeOperationException.class,
            () -> client.volumeExists("catalog", "schema", "VolumeA", true));
    assertTrue(ex.getMessage().contains("Error checking volume existence"));
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

    List<String> result = client.listObjects("catalog", "schema", "volume", "file", true);

    assertEquals(Arrays.asList("file1", "file2"), result);

    Mockito.verify(mockResponse).getFiles();
    Mockito.verify(file1).getPath();
    Mockito.verify(file2).getPath();
  }

  @Test
  void testGetObjectWithLocalPath() throws Exception {
    // Volume Operation builder spy
    VolumeOperationProcessor.Builder realBuilder = VolumeOperationProcessor.Builder.createBuilder();
    processorBuilder = spy(realBuilder);
    doReturn(mockProcessor).when(processorBuilder).build();

    CreateDownloadUrlResponse mockResponse = mock(CreateDownloadUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateDownloadUrlResponse(any());

    try (MockedStatic<VolumeOperationProcessor.Builder> mockedStatic =
        mockStatic(VolumeOperationProcessor.Builder.class)) {
      mockedStatic
          .when(VolumeOperationProcessor.Builder::createBuilder)
          .thenReturn(processorBuilder);

      boolean result = client.getObject("catalog", "schema", "volume", "objectPath", "localPath");

      assertTrue(result);
      verify(mockProcessor).process();
    }
  }

  @Test
  void testGetObject_getCreateDownloadUrlResponseException() throws Exception {
    DatabricksVolumeOperationException mockException =
        new DatabricksVolumeOperationException(
            "Mocked Exception", DatabricksDriverErrorCode.INVALID_STATE);
    doThrow(mockException).when(client).getCreateDownloadUrlResponse(any());
    assertThrows(
        DatabricksVolumeOperationException.class,
        () -> client.getObject("catalog", "schema", "volume", "objectPath", "localPath"));
  }

  @Test
  void testGetCreateDownloadUrlResponse() throws Exception {
    CreateDownloadUrlResponse mockResponse = new CreateDownloadUrlResponse();
    when(mockAPIClient.execute(any(), eq(CreateDownloadUrlResponse.class)))
        .thenReturn(mockResponse);
    CreateDownloadUrlResponse response = client.getCreateDownloadUrlResponse("path");
    assertEquals(response, mockResponse);
  }

  @Test
  void testGetCreateUploadUrlResponse() throws Exception {
    CreateUploadUrlResponse mockResponse = new CreateUploadUrlResponse();
    when(mockAPIClient.execute(any(), eq(CreateUploadUrlResponse.class))).thenReturn(mockResponse);
    CreateUploadUrlResponse response = client.getCreateUploadUrlResponse("path");
    assertEquals(response, mockResponse);
  }

  @Test
  void testGetCreateDeleteUrlResponse() throws Exception {
    CreateDeleteUrlResponse mockResponse = new CreateDeleteUrlResponse();
    when(mockAPIClient.execute(any(), eq(CreateDeleteUrlResponse.class))).thenReturn(mockResponse);
    CreateDeleteUrlResponse response = client.getCreateDeleteUrlResponse("path");
    assertEquals(response, mockResponse);
  }

  @Test
  void testPutObjectWithLocalPath() throws Exception {
    // Volume Operation builder spy
    VolumeOperationProcessor.Builder realBuilder = VolumeOperationProcessor.Builder.createBuilder();
    processorBuilder = spy(realBuilder);
    doReturn(mockProcessor).when(processorBuilder).build();

    CreateUploadUrlResponse mockResponse = mock(CreateUploadUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateUploadUrlResponse(any());

    try (MockedStatic<VolumeOperationProcessor.Builder> mockedStatic =
        mockStatic(VolumeOperationProcessor.Builder.class)) {
      mockedStatic
          .when(VolumeOperationProcessor.Builder::createBuilder)
          .thenReturn(processorBuilder);

      boolean result =
          client.putObject("catalog", "schema", "volume", "objectPath", "localPath", true);

      assertTrue(result);
      verify(mockProcessor).process();
    }
  }

  @Test
  void testPutObjectWithLocalPath_getCreateUploadUrlResponseException() throws Exception {
    DatabricksVolumeOperationException mockException =
        new DatabricksVolumeOperationException(
            "Mocked Exception", DatabricksDriverErrorCode.INVALID_STATE);
    doThrow(mockException).when(client).getCreateUploadUrlResponse(any());
    assertThrows(
        DatabricksVolumeOperationException.class,
        () -> client.putObject("catalog", "schema", "volume", "objectPath", "localPath", true));
  }

  @Test
  void testPutObjectWithInputStream() throws Exception {
    // Volume Operation builder spy
    VolumeOperationProcessor.Builder realBuilder = VolumeOperationProcessor.Builder.createBuilder();
    processorBuilder = spy(realBuilder);
    doReturn(mockProcessor).when(processorBuilder).build();

    CreateUploadUrlResponse mockResponse = mock(CreateUploadUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateUploadUrlResponse(any());

    try (MockedStatic<VolumeOperationProcessor.Builder> mockedStatic =
        mockStatic(VolumeOperationProcessor.Builder.class)) {

      mockedStatic
          .when(VolumeOperationProcessor.Builder::createBuilder)
          .thenReturn(processorBuilder);

      File file = new File(tempFolder, "dbfs_test_put.txt");
      Files.writeString(file.toPath(), "test-put-stream");
      System.out.println("File created");

      boolean result;
      try (FileInputStream fis = new FileInputStream(file)) {
        result =
            client.putObject("catalog", "schema", "volume", "objectPath", fis, file.length(), true);
      }

      assertTrue(result);
      verify(mockProcessor).process();
    }
  }

  @Test
  void testDeleteObject() throws Exception {
    // Volume Operation builder spy
    VolumeOperationProcessor.Builder realBuilder = VolumeOperationProcessor.Builder.createBuilder();
    processorBuilder = spy(realBuilder);
    doReturn(mockProcessor).when(processorBuilder).build();

    CreateDeleteUrlResponse mockResponse = mock(CreateDeleteUrlResponse.class);
    when(mockResponse.getUrl()).thenReturn(PRE_SIGNED_URL);
    doReturn(mockResponse).when(client).getCreateDeleteUrlResponse(any());

    try (MockedStatic<VolumeOperationProcessor.Builder> mockedStatic =
        mockStatic(VolumeOperationProcessor.Builder.class)) {
      mockedStatic
          .when(VolumeOperationProcessor.Builder::createBuilder)
          .thenReturn(processorBuilder);

      boolean result = client.deleteObject("catalog", "schema", "volume", "objectPath");

      assertTrue(result);
      verify(mockProcessor).process();
    }
  }

  @Test
  void testDeleteObject_getCreateDeleteUrlResponseException() throws Exception {
    DatabricksVolumeOperationException mockException =
        new DatabricksVolumeOperationException(
            "Mocked Exception", DatabricksDriverErrorCode.INVALID_STATE);
    doThrow(mockException).when(client).getCreateDeleteUrlResponse(any());
    assertThrows(
        DatabricksVolumeOperationException.class,
        () -> client.deleteObject("catalog", "schema", "volume", "objectPath"));
  }
}
