package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.DatabricksSession;
import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.impl.fake.EmptyResultSet;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.ResultSchema;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class VolumeOperationResultTest {

  private static final String STATEMENT_ID = "statementId";
  private static final String LOCAL_FILE_GET = "getVolFile.csv";
  private static final String LOCAL_FILE_PUT = "putVolFile.csv";
  private static final String PRESIGNED_URL = "http://presignedUrl.site";
  private static final String ALLOWED_PATHS = "getVolFile,putVolFile";
  private static final String HEADERS = "{\"header1\":\"value1\"}";

  @Mock IDatabricksHttpClient mockHttpClient;
  @Mock CloseableHttpResponse httpResponse;
  @Mock HttpEntity httpEntity;
  @Mock StatusLine mockedStatusLine;
  @Mock DatabricksSession session;
  @Mock IExecutionResult resultHandler;
  @Mock IDatabricksStatement statement;
  @Mock IDatabricksResultSet resultSet;

  private static final ResultManifest RESULT_MANIFEST =
      new ResultManifest()
          .setIsVolumeOperation(true)
          .setTotalRowCount(1L)
          .setSchema(new ResultSchema().setColumnCount(4L));

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
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());

    File file = new File(LOCAL_FILE_GET);
    assertTrue(file.exists());
    try {
      String fileContent = new String(new FileInputStream(file).readAllBytes());
      assertEquals("test", fileContent);
    } finally {
      assertTrue(file.delete());
    }
  }

  @Test
  public void testGetResult_InputStream_Get() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    when(mockHttpClient.execute(isA(HttpGet.class))).thenReturn(httpResponse);
    when(httpResponse.getEntity()).thenReturn(new StringEntity("test"));
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);
    when(statement.isAllowedInputStreamForVolumeOperation()).thenReturn(true);

    IDatabricksResultSet fakeResultSet = new EmptyResultSet();
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            fakeResultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());

    assertNotNull(fakeResultSet.getVolumeOperationInputStream());
    assertEquals(
        "test",
        new String(fakeResultSet.getVolumeOperationInputStream().getContent().readAllBytes()));
  }

  @Test
  public void testGetResult_InputStream_StatementClosed_Get() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    when(statement.isAllowedInputStreamForVolumeOperation())
        .thenThrow(new DatabricksSQLException("statement closed"));

    IDatabricksResultSet fakeResultSet = new EmptyResultSet();
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            fakeResultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation aborted: Volume operation called on closed statement: statement closed",
          e.getMessage());
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

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Volume operation not supported", e.getMessage());
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
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Local file path is not allowed", e.getMessage());
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
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Local file path is not allowed", e.getMessage());
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
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Local file path is invalid", e.getMessage());
    }
  }

  @Test
  public void testGetResult_Get_FileExists() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("GET");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_GET);

    File file = new File(LOCAL_FILE_GET);
    Files.writeString(file.toPath(), "test-put");

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Local file already exists", e.getMessage());
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
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Local file path is invalid", e.getMessage());
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
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation failed: Failed to download file", e.getMessage());
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
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());
    assertTrue(file.delete());
  }

  @Test
  public void testGetResult_Put_withInputStream() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    when(mockHttpClient.execute(isA(HttpPut.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);
    when(statement.isAllowedInputStreamForVolumeOperation()).thenReturn(true);
    when(statement.getInputStreamForUCVolume())
        .thenReturn(new InputStreamEntity(new ByteArrayInputStream("test-put".getBytes()), 10L));

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(0));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());
  }

  @Test
  public void testGetResult_Put_withNullInputStream() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("PUT");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn("__input_stream__");
    when(statement.isAllowedInputStreamForVolumeOperation()).thenReturn(true);
    when(statement.getInputStreamForUCVolume()).thenReturn(null);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());

    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation aborted: InputStream not set for PUT operation", e.getMessage());
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
        .thenThrow(new DatabricksSQLException("statement closed"));

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());

    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation aborted: PUT operation called on closed statement", e.getMessage());
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

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation failed: Failed to upload file with error code: 403", e.getMessage());
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

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Local file is empty", e.getMessage());
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
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals(
          "Volume operation aborted: Local file does not exist or is a directory", e.getMessage());
    }
  }

  @Test
  public void testGetResult_invalidOperationType() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("FETCH");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(LOCAL_FILE_PUT);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Invalid operation type", e.getMessage());
    }
  }

  @Test
  public void testGetResult_Remove() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("REMOVE");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(null);
    when(mockHttpClient.execute(isA(HttpDelete.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

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

  @Test
  public void testGetResult_RemoveFailed() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("REMOVE");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(null);
    when(mockHttpClient.execute(isA(HttpDelete.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(403);

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());

    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation failed: Failed to delete volume", e.getMessage());
    }
    assertDoesNotThrow(() -> volumeOperationResult.close());
  }

  @Test
  public void testGetResult_RemoveFailedWithException() throws Exception {
    setupCommonInteractions();
    when(resultHandler.getObject(0)).thenReturn("REMOVE");
    when(resultHandler.getObject(1)).thenReturn(PRESIGNED_URL);
    when(resultHandler.getObject(3)).thenReturn(null);
    when(mockHttpClient.execute(isA(HttpDelete.class)))
        .thenThrow(new DatabricksHttpException("exception"));

    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());

    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation failed: Failed to delete volume: exception", e.getMessage());
    }
  }

  @Test
  public void getObject() throws Exception {
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

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
            STATEMENT_ID,
            RESULT_MANIFEST,
            session,
            resultHandler,
            mockHttpClient,
            statement,
            resultSet);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Volume operation URL is not set", e.getMessage());
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
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));
  }
}
