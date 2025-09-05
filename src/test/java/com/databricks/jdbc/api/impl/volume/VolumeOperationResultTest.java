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
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.service.sql.ResultSchema;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_GET);
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

    File file = new File(LOCAL_FILE_GET);
    assertTrue(file.exists());
    try (FileInputStream fis = new FileInputStream(file)) {
      java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      int n;
      while ((n = fis.read(buf)) != -1) {
        baos.write(buf, 0, n);
      }
      String fileContent = new String(baos.toByteArray());
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
    {
      java.util.HashMap<String, String> __m = new java.util.HashMap<String, String>();
      __m.put(ENABLE_VOLUME_OPERATIONS.toLowerCase(), propertyValue);
      buildClientInfoProperties(__m);
    }
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
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    if (expected) {
      assertSuccessVolumeGetOperations(volumeOperationResult);
    } else {
      assertFailedStreamVolumeOperations(volumeOperationResult);
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

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    java.util.Map<String, String> m = new java.util.HashMap<String, String>();
    m.put(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), "");
    when(session.getClientInfoProperties()).thenReturn(m);
    when(session.getConnectionContext()).thenReturn(context);
    when(context.getVolumeOperationAllowedPaths()).thenReturn("");

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_GET);

    File file = new File(LOCAL_FILE_GET);
    java.nio.file.Files.write(file.toPath(), "test-put".getBytes());

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_GET);
    when(mockHttpClient.execute(isA(HttpGet.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(403);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    java.nio.file.Files.write(file.toPath(), "test-put".getBytes());

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
    {
      java.util.HashMap<String, String> __m = new java.util.HashMap<String, String>();
      __m.put(ENABLE_VOLUME_OPERATIONS.toLowerCase(), propertyValue);
      buildClientInfoProperties(__m);
    }
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

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
    if (expected) {
      assertSuccessVolumePutOperations(volumeOperationResult);
    } else {
      assertFailedStreamVolumeOperations(volumeOperationResult);
    }
  }

  @Test
  public void testGetResult_Put_withNullInputStream() throws Exception {
    setupCommonInteractions();
    {
      java.util.HashMap<String, String> __m = new java.util.HashMap<String, String>();
      __m.put(ENABLE_VOLUME_OPERATIONS.toLowerCase(), "True");
      buildClientInfoProperties(__m);
    }
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    when(statement.isAllowedInputStreamForVolumeOperation()).thenReturn(true);
    when(statement.getInputStreamForUCVolume()).thenReturn(null);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());

    try {
      volumeOperationResult.next();
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

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());

    try {
      volumeOperationResult.next();
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
    java.nio.file.Files.write(
        file.toPath(), "test-put".getBytes(java.nio.charset.StandardCharsets.UTF_8));

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    java.nio.file.Files.write(file.toPath(), new byte[0]);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    {
      java.util.HashMap<String, String> __m = new java.util.HashMap<String, String>();
      __m.put(ENABLE_VOLUME_OPERATIONS.toLowerCase(), "1");
      buildClientInfoProperties(__m);
    }
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
      assertEquals("Invalid column access", e.getMessage());
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
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);
    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    if (expected) {
      assertTrue(volumeOperationResult.next());
      assertEquals(0, volumeOperationResult.getCurrentRow());
      assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
      assertFalse(volumeOperationResult.hasNext());
      assertFalse(volumeOperationResult.next());
      try {
        volumeOperationResult.getObject(2);
        fail("Should throw DatabricksSQLException");
      } catch (DatabricksSQLException e) {
        assertEquals("Invalid column access", e.getMessage());
      }
    } else {
      try {
        volumeOperationResult.next();
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
    {
      java.util.HashMap<String, String> __m = new java.util.HashMap<String, String>();
      __m.put(ENABLE_VOLUME_OPERATIONS.toLowerCase(), "1");
      buildClientInfoProperties(__m);
    }
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(null);
    when(mockHttpClient.execute(isA(HttpDelete.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(403);
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());

    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation status : FAILED, Error message: Failed to delete volume",
          e.getMessage());
    }
    assertDoesNotThrow(volumeOperationResult::close);
  }

  @Test
  public void testGetResult_RemoveFailedWithException() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("REMOVE");
    {
      java.util.HashMap<String, String> __m = new java.util.HashMap<String, String>();
      __m.put(ENABLE_VOLUME_OPERATIONS.toLowerCase(), "1");
      buildClientInfoProperties(__m);
    }
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(null);
    when(mockHttpClient.execute(isA(HttpDelete.class)))
        .thenThrow(
            new DatabricksHttpException("exception", DatabricksDriverErrorCode.INVALID_STATE));

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());

    try {
      volumeOperationResult.next();
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
      assertEquals("Invalid row access", e.getMessage());
    }
  }

  @Test
  public void testGetResult_Get_emptyLink() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn("");
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_GET);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            RESULT_MANIFEST, session, resultHandler, mockHttpClient, statement);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
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
    {
      java.io.InputStream __in = volumeOperationResult.getVolumeOperationInputStream().getContent();
      java.io.ByteArrayOutputStream __baos = new java.io.ByteArrayOutputStream();
      byte[] __buf = new byte[8192];
      int __n;
      while ((__n = __in.read(__buf)) != -1) {
        __baos.write(__buf, 0, __n);
      }
      assertEquals("test", new String(__baos.toByteArray()));
    }
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
        .thenReturn(true)
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
