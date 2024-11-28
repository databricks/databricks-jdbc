package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.model.client.filesystem.*;
import java.io.InputStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DBFSVolumeClientTest {
  private static final String PRE_SIGNED_URL = "http://example.com/upload";

  @Mock private VolumeOperationProcessorDirect mockProcessor;
  @Spy private DBFSVolumeClient client;

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
}
