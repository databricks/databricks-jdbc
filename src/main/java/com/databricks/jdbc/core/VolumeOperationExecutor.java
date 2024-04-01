package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.IDatabricksHttpClient;
import java.io.*;
import java.util.*;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Executor for volume operations */
class VolumeOperationExecutor implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(VolumeOperationExecutor.class);

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
  private VolumeOperationStatus status;
  private IDatabricksHttpClient databricksHttpClient;
  private String errorMessage;

  VolumeOperationExecutor(
      String operationType,
      String operationUrl,
      String localFilePath,
      Map<String, String> headers,
      String allowedVolumeIngestionPathString,
      IDatabricksHttpClient databricksHttpClient) {
    this.operationType = operationType;
    this.operationUrl = operationUrl;
    this.localFilePath = localFilePath;
    this.headers = headers;
    this.allowedVolumeIngestionPaths = getAllowedPaths(allowedVolumeIngestionPathString);
    this.databricksHttpClient = databricksHttpClient;
    this.status = VolumeOperationStatus.PENDING;
    this.errorMessage = null;
  }

  private static Set<String> getAllowedPaths(String allowedVolumeIngestionPathString) {
    if (allowedVolumeIngestionPathString == null || allowedVolumeIngestionPathString.isEmpty()) {
      return Collections.emptySet();
    }
    return new HashSet<>(Arrays.asList(allowedVolumeIngestionPathString.split(COMMA_SEPARATOR)));
  }

  @Override
  public void run() {
    LOGGER.debug("Running volume operation {} on local file {}", operationType,
        localFilePath == null ? "" : localFilePath);
    validateLocalFilePath();
    if (status == VolumeOperationStatus.ABORTED) {
      return;
    }
    status = VolumeOperationStatus.RUNNING;
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
        status = VolumeOperationStatus.ABORTED;
        errorMessage = "Invalid operation type";
    }
  }

  VolumeOperationStatus getStatus() {
    return status;
  }

  String getErrorMessage() {
    return errorMessage;
  }

  private void validateLocalFilePath() {
    if (allowedVolumeIngestionPaths.isEmpty()) {
      LOGGER.error("Volume ingestion paths are not set");
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Volume operation not supported";
      return;
    }
    if (operationType.equalsIgnoreCase(REMOVE_OPERATION)) {
      return;
    }
    if (localFilePath == null
        || localFilePath.isEmpty()
        || localFilePath.contains(PARENT_DIRECTORY_REF)) {
      LOGGER.error("Local file path is invalid {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file path is invalid";
      return;
    }
    Optional<Boolean> pathMatched =
        allowedVolumeIngestionPaths.stream()
            .map(localFilePath::startsWith)
            .filter(x -> x)
            .findFirst();
    if (pathMatched.isEmpty() || !pathMatched.get()) {
      LOGGER.error("Local file path is not allowed {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file path is not allowed";
    }
  }

  private void executeGetOperation() {
    HttpGet httpGet = new HttpGet(operationUrl);
    headers.forEach(httpGet::addHeader);

    File localFile = new File(localFilePath);
    if (localFile.exists()) {
      LOGGER.error("Local file already exists for GET operation {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file already exists";
      return;
    }

    try (CloseableHttpResponse response = databricksHttpClient.execute(httpGet)) {
      if (!isSuccessfulHttpResponse(response)) {
        LOGGER.error(
            "Failed to fetch content from volume with error {} for local file {}",
            response.getStatusLine().getStatusCode(),
            localFilePath);
        status = VolumeOperationStatus.FAILED;
        errorMessage = "Failed to download file";
        return;
      }
      HttpEntity entity = response.getEntity();
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
          status = VolumeOperationStatus.SUCCEEDED;
        } catch (FileNotFoundException e) {
          LOGGER.error("Local file path is invalid or a directory {}", localFilePath);
          status = VolumeOperationStatus.FAILED;
          errorMessage = "Local file path is invalid or a directory";
        } catch (IOException e) {
          // TODO: handle retries
          LOGGER.error(
              "Failed to write to local file {} with error {}", localFilePath, e.getMessage());
          status = VolumeOperationStatus.FAILED;
          errorMessage = "Failed to write to local file: " + e.getMessage();
        } finally {
          // It's important to consume the entity content fully and ensure the stream is closed
          EntityUtils.consume(entity);
        }
      }
    } catch (IOException | DatabricksHttpException e) {
      status = VolumeOperationStatus.FAILED;
      errorMessage = "Failed to download file: " + e.getMessage();
    }
  }

  private void executePutOperation() {
    HttpPut httpPut = new HttpPut(operationUrl);
    headers.forEach(httpPut::addHeader);

    // Set the FileEntity as the request body
    File file = new File(localFilePath);
    if (!file.exists() || file.isDirectory()) {
      LOGGER.error("Local file does not exist or is a directory {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file does not exist or is a directory";
      return;
    }
    if (file.length() == 0) {
      LOGGER.error("Local file is empty {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file is empty";
      return;
    }

    if (file.length() > PUT_SIZE_LIMITS) {
      LOGGER.error("Local file too large {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file too large";
      return;
    }

    FileEntity fileEntity = new FileEntity(file, ContentType.DEFAULT_BINARY);
    httpPut.setEntity(fileEntity);

    // Execute the request
    try (CloseableHttpResponse response = databricksHttpClient.execute(httpPut)) {
      // Process the response
      if (isSuccessfulHttpResponse(response)) {
        status = VolumeOperationStatus.SUCCEEDED;
      } else {
        LOGGER.error(
            "Failed to upload file {} with error code: {}",
            localFilePath,
            response.getStatusLine().getStatusCode());
        // TODO: handle retries
        status = VolumeOperationStatus.FAILED;
        errorMessage =
            "Failed to upload file with error code: " + response.getStatusLine().getStatusCode();
      }
    } catch (IOException | DatabricksHttpException e) {
      LOGGER.error("Failed to upload file {} with error {}", localFilePath, e.getMessage());
      status = VolumeOperationStatus.FAILED;
      errorMessage = "Failed to upload file: " + e.getMessage();
    }
  }

  private void executeDeleteOperation() {
    // TODO: Check for AWS specific handling
    HttpDelete httpDelete = new HttpDelete(operationUrl);
    headers.forEach(httpDelete::addHeader);
    try (CloseableHttpResponse response = databricksHttpClient.execute(httpDelete)) {
      if (isSuccessfulHttpResponse(response)) {
        status = VolumeOperationStatus.SUCCEEDED;
      } else {
        LOGGER.error(
            "Failed to delete volume with error code: {}",
            response.getStatusLine().getStatusCode());
        status = VolumeOperationStatus.FAILED;
        errorMessage = "Failed to delete volume";
      }
    } catch (DatabricksHttpException | IOException e) {
      LOGGER.error("Failed to delete volume with error {}", e.getMessage());
      status = VolumeOperationStatus.FAILED;
      errorMessage = "Failed to delete volume: " + e.getMessage();
    }
  }

  private boolean isSuccessfulHttpResponse(CloseableHttpResponse response) {
    return response.getStatusLine().getStatusCode() >= 200
        && response.getStatusLine().getStatusCode() < 300;
  }

  static enum VolumeOperationStatus {
    PENDING,
    RUNNING,
    ABORTED,
    SUCCEEDED,
    FAILED;
  }
}
