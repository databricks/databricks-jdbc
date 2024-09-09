package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.util.EntityUtils;

/** Executor for volume operations */
class VolumeOperationExecutor implements Runnable {
  public enum VolumeOperationStatus {
    PENDING,
    RUNNING,
    ABORTED,
    SUCCEEDED,
    FAILED;
  }

  class StatusMessage {
    private final VolumeOperationStatus status;
    private final String message;

    public StatusMessage(VolumeOperationStatus status, String message) {
      this.status = status;
      this.message = message;
    }
  }

  private static final String COMMA_SEPARATOR = ",";
  private static final String PARENT_DIRECTORY_REF = "..";
  private static final String GET_OPERATION = "get";
  private static final String PUT_OPERATION = "put";
  private static final String REMOVE_OPERATION = "remove";

  private static final Long PUT_SIZE_LIMITS = 5 * 1024 * 1024 * 1024L; // 5GB
  private final String operationType;
  private final String operationUrl;
  private final String localFilePath;
  private final Map<String, String> headers;
  private final Set<String> allowedVolumeIngestionPaths;
  private final IDatabricksStatement statement;
  private final IDatabricksResultSet resultSet;
  private final IDatabricksHttpClient databricksHttpClient;
  private AtomicReference<StatusMessage> statusMessage;

  VolumeOperationExecutor(
      String operationType,
      String operationUrl,
      Map<String, String> headers,
      String localFilePath,
      String allowedVolumeIngestionPathString,
      IDatabricksHttpClient databricksHttpClient,
      IDatabricksStatement statement,
      IDatabricksResultSet resultSet) {
    this.operationType = operationType;
    this.operationUrl = operationUrl;
    this.localFilePath = localFilePath;
    this.headers = headers;
    this.allowedVolumeIngestionPaths = getAllowedPaths(allowedVolumeIngestionPathString);
    this.databricksHttpClient = databricksHttpClient;
    this.statement = statement;
    this.resultSet = resultSet;
    this.statusMessage =
        new AtomicReference<>(new StatusMessage(VolumeOperationStatus.PENDING, ""));
  }

  private static Set<String> getAllowedPaths(String allowedVolumeIngestionPathString) {
    if (allowedVolumeIngestionPathString == null || allowedVolumeIngestionPathString.isEmpty()) {
      return Collections.emptySet();
    }
    return new HashSet<>(Arrays.asList(allowedVolumeIngestionPathString.split(COMMA_SEPARATOR)));
  }

  private void updateStatus(VolumeOperationStatus status) {
    updateStatus(status, "");
  }

  private void updateStatus(VolumeOperationStatus status, String message) {
    statusMessage.set(new StatusMessage(status, message));
  }

  @Override
  public void run() {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "Running volume operation {%s} on local file {%s}",
            operationType, localFilePath == null ? "" : localFilePath));
    if (operationUrl == null || operationUrl.isEmpty()) {
      LoggingUtil.log(LogLevel.ERROR, "Volume operation URL is not set");
      updateStatus(VolumeOperationStatus.ABORTED, "Volume operation URL is not set");
      return;
    }
    validateLocalFilePath();
    if (statusMessage.get().status == VolumeOperationStatus.ABORTED) {
      return;
    }
    updateStatus(VolumeOperationStatus.RUNNING);
    switch (operationType.toLowerCase()) {
      case GET_OPERATION:
        executeGetOperation();
        break;
      case PUT_OPERATION:
        executePutOperation();
        break;
      case REMOVE_OPERATION:
        executeDeleteOperation();
        break;
      default:
        updateStatus(VolumeOperationStatus.ABORTED, "Invalid operation type");
    }
  }

  VolumeOperationStatus getStatus() {
    return statusMessage.get().status;
  }

  String getErrorMessage() {
    return statusMessage.get().message;
  }

  private void validateLocalFilePath() {
    try {
      if (statement.isAllowedInputStreamForVolumeOperation()) {
        return;
      }
    } catch (DatabricksSQLException e) {
      String message = "Volume operation called on closed statement: " + e.getMessage();
      LoggingUtil.log(LogLevel.ERROR, message);
      updateStatus(VolumeOperationStatus.ABORTED, message);
      return;
    }
    if (allowedVolumeIngestionPaths.isEmpty()) {
      LoggingUtil.log(LogLevel.ERROR, "Volume ingestion paths are not set");
      updateStatus(VolumeOperationStatus.ABORTED, "Volume operation not supported");
      return;
    }
    if (operationType.equalsIgnoreCase(REMOVE_OPERATION)) {
      return;
    }
    if (localFilePath == null
        || localFilePath.isEmpty()
        || localFilePath.contains(PARENT_DIRECTORY_REF)) {
      LoggingUtil.log(
          LogLevel.ERROR, String.format("Local file path is invalid {%s}", localFilePath));
      updateStatus(VolumeOperationStatus.ABORTED, "Local file path is invalid");
      return;
    }
    Optional<Boolean> pathMatched =
        allowedVolumeIngestionPaths.stream()
            .map(localFilePath::startsWith)
            .filter(x -> x)
            .findFirst();
    if (pathMatched.isEmpty() || !pathMatched.get()) {
      LoggingUtil.log(
          LogLevel.ERROR, String.format("Local file path is not allowed {%s}", localFilePath));
      updateStatus(VolumeOperationStatus.ABORTED, "Local file path is not allowed");
    }
  }

  private void executeGetOperation() {
    HttpGet httpGet = new HttpGet(operationUrl);
    headers.forEach(httpGet::addHeader);

    HttpEntity entity = null;
    try {
      // We return the input stream directly to clients, if they want to consume as input stream
      if (statement.isAllowedInputStreamForVolumeOperation()) {
        CloseableHttpResponse response = databricksHttpClient.execute(httpGet);
        if (!isSuccessfulHttpResponse(response)) {
          String message =
              String.format(
                  "Failed to fetch content from volume with error code {%s} for input stream and error {%s}",
                  response.getStatusLine().getStatusCode(),
                  response.getStatusLine().getReasonPhrase());
          LoggingUtil.log(LogLevel.ERROR, message);
          updateStatus(VolumeOperationStatus.FAILED, message);
          return;
        }
        entity = response.getEntity();
        if (entity != null) {
          this.resultSet.setVolumeOperationEntityStream(entity);
        }
        updateStatus(VolumeOperationStatus.SUCCEEDED);
        return;
      }
    } catch (SQLException | IOException e) {
      String message = "Failed to execute GET operation for input stream: " + e.getMessage();
      LoggingUtil.log(LogLevel.ERROR, message);
      updateStatus(VolumeOperationStatus.FAILED, message);
      return;
    }

    // Copy the data in local file as requested by user
    File localFile = new File(localFilePath);
    if (localFile.exists()) {
      LoggingUtil.log(
          LogLevel.ERROR,
          String.format("Local file already exists for GET operation {%s}", localFilePath));
      updateStatus(VolumeOperationStatus.ABORTED, "Local file already exists");
      return;
    }

    try (CloseableHttpResponse response = databricksHttpClient.execute(httpGet)) {
      if (!isSuccessfulHttpResponse(response)) {
        LoggingUtil.log(
            LogLevel.ERROR,
            String.format(
                "Failed to fetch content from volume with error {%s} for local file {%s}",
                response.getStatusLine().getStatusCode(), localFilePath));
        updateStatus(VolumeOperationStatus.FAILED, "Failed to download file");
        return;
      }
      entity = response.getEntity();
      if (entity != null) {
        // Get the content of the HttpEntity
        InputStream inputStream = entity.getContent();
        // Create a FileOutputStream to write the content to a file
        try (FileOutputStream outputStream = new FileOutputStream(localFile)) {
          // Copy the content of the InputStream to the FileOutputStream
          byte[] buffer = new byte[1024];
          int length;
          while ((length = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, length);
          }
          updateStatus(VolumeOperationStatus.SUCCEEDED);
        } catch (FileNotFoundException e) {
          LoggingUtil.log(
              LogLevel.ERROR,
              String.format("Local file path is invalid or a directory {%s}", localFilePath));
          updateStatus(VolumeOperationStatus.FAILED, "Local file path is invalid or a directory");
        } catch (IOException e) {
          // TODO: handle retries
          LoggingUtil.log(
              LogLevel.ERROR,
              String.format(
                  "Failed to write to local file {%s} with error {%s}",
                  localFilePath, e.getMessage()));
          updateStatus(
              VolumeOperationStatus.FAILED, "Failed to write to local file: " + e.getMessage());
        } finally {
          // It's important to consume the entity content fully and ensure the stream is closed
          EntityUtils.consume(entity);
        }
      }
    } catch (IOException | DatabricksHttpException e) {
      updateStatus(VolumeOperationStatus.FAILED, "Failed to download file: " + e.getMessage());
    }
  }

  private void executePutOperation() {
    HttpPut httpPut = new HttpPut(operationUrl);
    headers.forEach(httpPut::addHeader);

    try {
      if (statement.isAllowedInputStreamForVolumeOperation()) {
        InputStreamEntity inputStream = statement.getInputStreamForUCVolume();
        if (inputStream == null) {
          String message = "InputStream not set for PUT operation";
          LoggingUtil.log(LogLevel.ERROR, message);
          updateStatus(VolumeOperationStatus.ABORTED, message);
          return;
        }
        httpPut.setEntity(inputStream);
      } else {
        // Set the FileEntity as the request body
        File file = new File(localFilePath);

        if (localFileHasErrorForPutOperation(file)) {
          return;
        }
        httpPut.setEntity(new FileEntity(file, ContentType.DEFAULT_BINARY));
      }
    } catch (DatabricksSQLException e) {
      String message = "PUT operation called on closed statement";
      LoggingUtil.log(LogLevel.ERROR, message);
      updateStatus(VolumeOperationStatus.ABORTED, message);
    }

    // Execute the request
    try (CloseableHttpResponse response = databricksHttpClient.execute(httpPut)) {
      // Process the response
      if (isSuccessfulHttpResponse(response)) {
        updateStatus(VolumeOperationStatus.SUCCEEDED);
      } else {
        LoggingUtil.log(
            LogLevel.ERROR,
            String.format(
                "Failed to upload file {%s} with error code: {%s}",
                localFilePath, response.getStatusLine().getStatusCode()));
        // TODO: handle retries
        updateStatus(
            VolumeOperationStatus.FAILED,
            "Failed to upload file with error code: " + response.getStatusLine().getStatusCode());
      }
    } catch (IOException | DatabricksHttpException e) {
      LoggingUtil.log(
          LogLevel.ERROR,
          String.format(
              "Failed to upload file {%s} with error {%s}", localFilePath, e.getMessage()));
      updateStatus(VolumeOperationStatus.FAILED, "Failed to upload file: " + e.getMessage());
    }
  }

  private boolean localFileHasErrorForPutOperation(File file) {
    if (!file.exists() || file.isDirectory()) {
      LoggingUtil.log(
          LogLevel.ERROR,
          String.format("Local file does not exist or is a directory {%s}", localFilePath));
      updateStatus(VolumeOperationStatus.ABORTED, "Local file does not exist or is a directory");
      return true;
    }
    if (file.length() == 0) {
      LoggingUtil.log(LogLevel.ERROR, String.format("Local file is empty {%s}", localFilePath));
      updateStatus(VolumeOperationStatus.ABORTED, "Local file is empty");
      return true;
    }

    if (file.length() > PUT_SIZE_LIMITS) {
      LoggingUtil.log(LogLevel.ERROR, String.format("Local file too large {%s}", localFilePath));
      updateStatus(VolumeOperationStatus.ABORTED, "Local file too large");
      return true;
    }
    return false;
  }

  private void executeDeleteOperation() {
    // TODO: Check for AWS specific handling
    HttpDelete httpDelete = new HttpDelete(operationUrl);
    headers.forEach(httpDelete::addHeader);
    try (CloseableHttpResponse response = databricksHttpClient.execute(httpDelete)) {
      if (isSuccessfulHttpResponse(response)) {
        updateStatus(VolumeOperationStatus.SUCCEEDED);
      } else {
        LoggingUtil.log(
            LogLevel.ERROR,
            String.format(
                "Failed to delete volume with error code: {%s}",
                response.getStatusLine().getStatusCode()));
        updateStatus(VolumeOperationStatus.FAILED, "Failed to delete volume");
      }
    } catch (DatabricksHttpException | IOException e) {
      LoggingUtil.log(
          LogLevel.ERROR, String.format("Failed to delete volume with error {%s}", e.getMessage()));
      updateStatus(VolumeOperationStatus.FAILED, "Failed to delete volume: " + e.getMessage());
    }
  }

  private boolean isSuccessfulHttpResponse(CloseableHttpResponse response) {
    return response.getStatusLine().getStatusCode() >= 200
        && response.getStatusLine().getStatusCode() < 300;
  }
}
