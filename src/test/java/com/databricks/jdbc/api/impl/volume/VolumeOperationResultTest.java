package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.ENABLE_VOLUME_OPERATIONS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksSession;
import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.util.VolumeUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class VolumeOperationResultTest {

  private static final String LOCAL_FILE_GET = "getVolFile.csv";
  private static final String LOCAL_FILE_PUT = "putVolFile.csv";
  private static final String PRESIGNED_URL = "http://presignedUrl.site";
  private static final String ALLOWED_PATHS = "getVolFile,putVolFile";
  private static final String HEADERS = "{\"header1\":\"value1\"}";
  @Mock IDatabricksHttpClient mockHttpClient;
  @Mock CloseableHttpResponse httpResponse;
  @Mock StatusLine mockedStatusLine;
  @Mock DatabricksSession session;
  @Mock IExecutionResult resultHandler;
  @Mock IDatabricksStatementInternal statement;
  @Mock IDatabricksConnectionContext context;
  private static final ResultManifest RESULT_MANIFEST =
      new ResultManifest()
          .setIsVolumeOperation(true)
          .setTotalRowCount(1L)
          .setSchema(new ResultSchema().setColumnCount(4L));

  static Stream<Arguments> enableVolumeOperations() {
    return Stream.of(
        Arguments.of("true", true),
        Arguments.of("1", true),
        Arguments.of("0", false),
        Arguments.of("True", true),
        Arguments.of("TrUe", true),
        Arguments.of("null", false),
        Arguments.of("false", false),
        Arguments.of("random_Value", false));
  }

  @Test
  public void testGetResult_Get() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    String localGet = "getVolFile_" + UUID.randomUUID() + ".csv";
    // Ensure no leftover from previous runs
    File pre = new File(localGet);
    if (pre.exists()) {
      pre.delete();
    }
    when(resultHandler.getObject(3)).thenReturn(localGet);
    when(mockHttpClient.execute(isA(HttpGet.class))).thenReturn(httpResponse);
    when(httpResponse.getEntity()).thenReturn(new StringEntity("test"));
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());

    File file = new File(localGet);
    assertTrue(file.exists());
    try (FileInputStream fis = new FileInputStream(file)) {
      String fileContent = new String(fis.readAllBytes());
      assertEquals("test", fileContent);
    } finally {
      assertTrue(file.delete());
    }
  }

  @ParameterizedTest
  @MethodSource("enableVolumeOperations")
  public void testGetResult_InputStream_Get(String propertyValue, boolean expected)
      throws Exception {
    setupCommonInteractions();
    buildClientInfoProperties(Map.of(ENABLE_VOLUME_OPERATIONS.toLowerCase(), propertyValue));
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    if (expected) {
      when(mockHttpClient.execute(isA(HttpGet.class))).thenReturn(httpResponse);
      when(httpResponse.getEntity()).thenReturn(new StringEntity("test"));
      when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
      when(mockedStatusLine.getStatusCode()).thenReturn(200);
    }

    when(statement.isAllowedInputStreamForVolumeOperation()).thenReturn(true);
    if (expected) {
      VolumeOperationResult volumeOperationResult =
          new VolumeOperationResult(
              RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      assertTrue(volumeOperationResult.hasNext());
      assertEquals(-1, volumeOperationResult.getCurrentRow());
      assertSuccessVolumeGetOperations(volumeOperationResult);
    } else {
      try {
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
        fail("Should throw DatabricksSQLException");
      } catch (DatabricksSQLException e) {
        assertEquals(
            "Volume operation status : ABORTED, Error message: enableVolumeOperations property mandatory for Volume operations on stream",
            e.getMessage());
      }
    }
  }

  @Test
  public void testGetResult_InputStream_StatementClosed_Get() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    when(statement.isAllowedInputStreamForVolumeOperation())
        .thenThrow(
            new DatabricksSQLException(
                "statement closed", DatabricksDriverErrorCode.INVALID_STATE));

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("statement closed", e.getMessage());
    }
  }

  @Test
  public void testGetResult_Get_PropertyEmpty() throws Exception {
    when(resultHandler.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false)
        .thenReturn(false);
    when(resultHandler.next()).thenReturn(true).thenReturn(false);
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(2)).thenReturn(HEADERS);
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_GET);
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ""));
    when(session.getConnectionContext()).thenReturn(context);
    when(context.getVolumeOperationAllowedPaths()).thenReturn("");

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Volume ingestion paths are not set",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_Get_PathNotAllowed() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("localFileOther");
    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Local file path is not allowed",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_Get_PathCaseSensitive() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("getvolfile.csv");
    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Local file path is not allowed",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_Get_PathInvalid() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("");
    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Local file path is invalid",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_Get_FileExists() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    String localGet = "getVolFile_" + UUID.randomUUID() + ".csv";
    when(resultHandler.getObject(3)).thenReturn(localGet);

    File file = new File(localGet);
    Files.writeString(file.toPath(), "test-put");

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Local file already exists",
          e.getMessage());
    } finally {
      file.delete();
    }
  }

  @Test
  public void testGetResult_Get_PathContainsParentDir() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("../newFile.csv");

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Local file path is invalid",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_Get_HttpError() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    String localGet = "getVolFile_" + UUID.randomUUID() + ".csv";
    when(resultHandler.getObject(3)).thenReturn(localGet);
    when(mockHttpClient.execute(isA(HttpGet.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(403);

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : FAILED, Error message: Failed to download file",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_Put() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_PUT);
    when(mockHttpClient.execute(isA(HttpPut.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);

    File file = new File(LOCAL_FILE_PUT);
    Files.writeString(file.toPath(), "test-put");

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());
    assertTrue(file.delete());
  }

  @ParameterizedTest
  @MethodSource("enableVolumeOperations")
  public void testGetResult_Put_withInputStream(String propertyValue, boolean expected)
      throws Exception {
    setupCommonInteractions();
    buildClientInfoProperties(Map.of(ENABLE_VOLUME_OPERATIONS.toLowerCase(), propertyValue));
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    if (expected) {
      when(mockHttpClient.execute(isA(HttpPut.class))).thenReturn(httpResponse);
      when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
      when(mockedStatusLine.getStatusCode()).thenReturn(200);
    }
    when(statement.isAllowedInputStreamForVolumeOperation()).thenReturn(true);
    when(statement.getInputStreamForUCVolume())
        .thenReturn(new InputStreamEntity(new ByteArrayInputStream("test-put".getBytes()), 10L));

    if (expected) {
      VolumeOperationResult volumeOperationResult =
          new VolumeOperationResult(
              RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      assertSuccessVolumePutOperations(volumeOperationResult);
    } else {
      try {
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
        fail("Should throw DatabricksSQLException");
      } catch (DatabricksSQLException e) {
        assertEquals(
            "Volume operation status : ABORTED, Error message: enableVolumeOperations property mandatory for Volume operations on stream",
            e.getMessage());
      }
    }
  }

  @Test
  public void testGetResult_Put_withNullInputStream() throws Exception {
    setupCommonInteractions();
    buildClientInfoProperties(Map.of(ENABLE_VOLUME_OPERATIONS.toLowerCase(), "True"));
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    when(statement.isAllowedInputStreamForVolumeOperation()).thenReturn(true);
    when(statement.getInputStreamForUCVolume()).thenReturn(null);

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: InputStream not set for PUT operation",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_Put_withStatementClosed() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    when(statement.isAllowedInputStreamForVolumeOperation()).thenReturn(true);
    when(statement.getInputStreamForUCVolume())
        .thenThrow(
            new DatabricksSQLException(
                "statement closed", DatabricksDriverErrorCode.INVALID_STATE));

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("statement closed", e.getMessage());
    }
  }

  @Test
  public void testGetResult_Put_failedHttpResponse() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_PUT);
    when(mockHttpClient.execute(isA(HttpPut.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(403);

    File file = new File(LOCAL_FILE_PUT);
    Files.writeString(file.toPath(), "test-put");

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : FAILED, Error message: Failed to upload file with error code: 403",
          e.getMessage());
    } finally {
      file.delete();
    }
  }

  @Test
  public void testGetResult_Put_emptyLocalFile() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_PUT);

    File file = new File(LOCAL_FILE_PUT);
    Files.writeString(file.toPath(), "");

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Local file is empty", e.getMessage());
    } finally {
      file.delete();
    }
  }

  @Test
  public void testGetResult_Put_nonExistingLocalFile() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_PUT);

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Local file does not exist or is a directory",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_invalidOperationType() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn(VolumeUtil.VolumeOperationType.OTHER);
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_PUT);

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Invalid operation type",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_Remove() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("REMOVE");
    buildClientInfoProperties(Map.of(ENABLE_VOLUME_OPERATIONS.toLowerCase(), "1"));
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(null);
    when(mockHttpClient.execute(isA(HttpDelete.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());
    try {
      volumeOperationResult.getObject(2);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertTrue(e.getMessage().contains("Invalid column access"));
    }
  }

  @ParameterizedTest
  @MethodSource("enableVolumeOperations")
  void testGetResult_RemoveWithoutEitherPropertySet(String propertyValue, boolean expected)
      throws Exception {
    // Mocks as per your original test
    when(resultHandler.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false)
        .thenReturn(false);
    when(resultHandler.next()).thenReturn(true).thenReturn(false);
    when(resultHandler.getObject(2)).thenReturn(HEADERS);
    Map<String, String> clientProps = new HashMap<>();
    clientProps.put(ENABLE_VOLUME_OPERATIONS.toLowerCase(), propertyValue);
    when(session.getClientInfoProperties()).thenReturn(clientProps);
    when(session.getConnectionContext()).thenReturn(context);
    when(resultHandler.getObject(0)).thenReturn("REMOVE");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(null);
    if (expected) {
      when(mockHttpClient.execute(isA(HttpDelete.class))).thenReturn(httpResponse);
      when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
      when(mockedStatusLine.getStatusCode()).thenReturn(200);
      when(context.getVolumeOperationAllowedPaths()).thenReturn(ALLOWED_PATHS);
    }

    when(session.getConnectionContext()).thenReturn(context);
    if (expected) {
      VolumeOperationResult volumeOperationResult =
          new VolumeOperationResult(
              RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      assertTrue(volumeOperationResult.hasNext());
      assertEquals(-1, volumeOperationResult.getCurrentRow());
      assertTrue(volumeOperationResult.next());
      assertEquals(0, volumeOperationResult.getCurrentRow());
      assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
      assertFalse(volumeOperationResult.hasNext());
      assertFalse(volumeOperationResult.next());
      try {
        volumeOperationResult.getObject(2);
        fail("Should throw DatabricksSQLException");
      } catch (DatabricksSQLException e) {
        assertTrue(e.getMessage().contains("Invalid column access"));
      }
    } else {
      try {
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
        fail("Should throw DatabricksSQLException");
      } catch (DatabricksSQLException e) {
        assertEquals(
            "Volume operation status : ABORTED, Error message: enableVolumeOperations property or Volume ingestion paths required for remove operation on Volume",
            e.getMessage());
      }
    }
  }

  @Test
  public void testGetResult_RemoveFailed() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("REMOVE");
    buildClientInfoProperties(Map.of(ENABLE_VOLUME_OPERATIONS.toLowerCase(), "1"));
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(null);
    when(mockHttpClient.execute(isA(HttpDelete.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(403);
    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : FAILED, Error message: Failed to delete volume",
          e.getMessage());
    }
  }

  @Test
  public void testGetResult_RemoveFailedWithException() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("REMOVE");
    buildClientInfoProperties(Map.of(ENABLE_VOLUME_OPERATIONS.toLowerCase(), "1"));
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(null);
    when(mockHttpClient.execute(isA(HttpDelete.class)))
        .thenThrow(
            new DatabricksHttpException("exception", DatabricksDriverErrorCode.INVALID_STATE));

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : FAILED, Error message: Failed to delete volume: exception",
          e.getMessage());
    }
  }

  @Test
  public void getObject() throws Exception {
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    try {
      volumeOperationResult.getObject(2);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertTrue(e.getMessage().contains("Invalid row access"));
    }
  }

  @Test
  public void testGetResult_Get_emptyLink() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn("");
    String localGet = "getVolFile_" + UUID.randomUUID() + ".csv";
    when(resultHandler.getObject(3)).thenReturn(localGet);

    try {
      new VolumeOperationResult(RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: Volume operation URL is not set",
          e.getMessage());
    }
  }

  private void assertSuccessVolumePutOperations(VolumeOperationResult volumeOperationResult)
      throws Exception {
    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());
  }

  private void assertSuccessVolumeGetOperations(VolumeOperationResult volumeOperationResult)
      throws Exception {
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());

    assertNotNull(volumeOperationResult.getVolumeOperationInputStream());
    assertEquals(
        "test",
        new String(
            volumeOperationResult.getVolumeOperationInputStream().getContent().readAllBytes()));
  }

  private void assertFailedStreamVolumeOperations(VolumeOperationResult volumeOperationResult) {
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : ABORTED, Error message: enableVolumeOperations property mandatory for Volume operations on stream",
          e.getMessage());
    }
  }

  private void setupCommonInteractions() throws Exception {
    when(resultHandler.hasNext())
        .thenReturn(true)
        .thenReturn(false)
        .thenReturn(false)
        .thenReturn(false);
    when(resultHandler.next()).thenReturn(true).thenReturn(false);
    when(resultHandler.getObject(2)).thenReturn(HEADERS);
    buildClientInfoProperties(Collections.emptyMap());
  }

  private void buildClientInfoProperties(Map<String, String> overrides) {
    Map<String, String> clientInfoProperties = new HashMap<>();
    clientInfoProperties.put(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS);

    if (overrides != null) {
      clientInfoProperties.putAll(overrides); // add or override test-specific keys
    }
    when(session.getClientInfoProperties()).thenReturn(clientInfoProperties);
  }
}
