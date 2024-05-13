package com.databricks.jdbc.core;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.VolumeOperationInfo;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
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
  private static final Map<String, String> HEADERS = Map.of("header1", "value1");

  @Mock IDatabricksHttpClient mockHttpClient;
  @Mock CloseableHttpResponse httpResponse;
  @Mock HttpEntity httpEntity;
  @Mock StatusLine mockedStatusLine;
  @Mock DatabricksSession session;
  private static final VolumeOperationInfo VOLUME_OPERATION_INFO =
      new VolumeOperationInfo()
          .setVolumeOperationType("PUT")
          .setLocalFile("localFile")
          .setExternalLinks(
              List.of(
                  new ExternalLink()
                      .setExternalLink("externalLink")
                      .setHttpHeaders(new HashMap<>())));
  private static final ResultData RESULT_DATA =
      new ResultData().setVolumeOperationInfo(VOLUME_OPERATION_INFO);

  @Test
  public void testGetResult_Get() throws Exception {
    when(mockHttpClient.execute(isA(HttpGet.class))).thenReturn(httpResponse);
    when(httpResponse.getEntity()).thenReturn(new StringEntity("test"));
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("GET")
                    .setLocalFile(LOCAL_FILE_GET)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(1));
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
  public void testGetResult_Get_PropertyEmpty() throws Exception {
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ""));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("GET")
                    .setLocalFile(LOCAL_FILE_GET)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("GET")
                    .setLocalFile("localFileOther")
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("GET")
                    .setLocalFile("getvolfile.csv")
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("GET")
                    .setLocalFile("")
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    File file = new File(LOCAL_FILE_GET);
    Files.writeString(file.toPath(), "test-put");

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("GET")
                    .setLocalFile(LOCAL_FILE_GET)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("GET")
                    .setLocalFile("../newFile.csv")
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(mockHttpClient.execute(isA(HttpGet.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(403);
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("GET")
                    .setLocalFile(LOCAL_FILE_GET)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(mockHttpClient.execute(isA(HttpPut.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    File file = new File(LOCAL_FILE_PUT);
    Files.writeString(file.toPath(), "test-put");

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("PUT")
                    .setLocalFile(LOCAL_FILE_PUT)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(1));
    assertFalse(volumeOperationResult.hasNext());
    assertFalse(volumeOperationResult.next());
    assertTrue(file.delete());
  }

  @Test
  public void testGetResult_Put_failedHttpResponse() throws Exception {
    when(mockHttpClient.execute(isA(HttpPut.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(403);
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    File file = new File(LOCAL_FILE_PUT);
    Files.writeString(file.toPath(), "test-put");

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("PUT")
                    .setLocalFile(LOCAL_FILE_PUT)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    File file = new File(LOCAL_FILE_PUT);
    Files.writeString(file.toPath(), "");

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("PUT")
                    .setLocalFile(LOCAL_FILE_PUT)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("PUT")
                    .setLocalFile(LOCAL_FILE_PUT)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("FETCH")
                    .setLocalFile(LOCAL_FILE_PUT)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
    when(mockHttpClient.execute(isA(HttpDelete.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(200);
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("REMOVE")
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    assertTrue(volumeOperationResult.next());
    assertEquals(0, volumeOperationResult.getCurrentRow());
    assertEquals("SUCCEEDED", volumeOperationResult.getObject(1));
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
    when(mockHttpClient.execute(isA(HttpDelete.class))).thenReturn(httpResponse);
    when(httpResponse.getStatusLine()).thenReturn(mockedStatusLine);
    when(mockedStatusLine.getStatusCode()).thenReturn(403);
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("REMOVE")
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

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
  public void getObject() throws Exception {
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ExternalLink presignedUrl =
        new ExternalLink().setHttpHeaders(HEADERS).setExternalLink(PRESIGNED_URL);
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("INVALID")
                    .setLocalFile(LOCAL_FILE_GET)
                    .setExternalLinks(List.of(presignedUrl)));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

    try {
      volumeOperationResult.getObject(2);
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Invalid row access", e.getMessage());
    }
  }

  @Test
  public void testGetResult_Get_emptyLink() throws Exception {
    when(session.getClientInfoProperties())
        .thenReturn(Map.of(ALLOWED_VOLUME_INGESTION_PATHS.toLowerCase(), ALLOWED_PATHS));

    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setVolumeOperationType("GET")
                    .setLocalFile(LOCAL_FILE_GET));
    VolumeOperationResult volumeOperationResult =
        new VolumeOperationResult(resultData, STATEMENT_ID, session, mockHttpClient);

    assertTrue(volumeOperationResult.hasNext());
    assertEquals(-1, volumeOperationResult.getCurrentRow());
    try {
      volumeOperationResult.next();
      fail("Should throw DatabricksSQLException");
    } catch (DatabricksSQLException e) {
      assertEquals("Volume operation aborted: Volume operation URL is not set", e.getMessage());
    }
  }
}
