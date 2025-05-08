package com.databricks.jdbc.api.impl.volume;

import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import com.databricks.jdbc.common.util.HttpUtil;
import com.databricks.jdbc.common.util.VolumeUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.io.*;
import java.util.*;
import java.util.function.Consumer;
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
class VolumeOperationProcessor {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(VolumeOperationProcessor.class);
  private static final String COMMA_SEPARATOR = ",";
  private static final String PARENT_DIRECTORY_REF = "..";

  private static final Long PUT_SIZE_LIMITS = 5 * 1024 * 1024 * 1024L; // 5GB
  private final VolumeUtil.VolumeOperationType operationType;
  private final String operationUrl;
  private final String localFilePath;
  private final Map<String, String> headers;
  private final Set<String> allowedVolumeIngestionPaths;
  private final boolean isAllowedInputStreamForVolumeOperation;
  private final IDatabricksHttpClient databricksHttpClient;
  private final InputStreamEntity inputStream;
  private final Consumer<HttpEntity> getStreamReceiver;
  private VolumeOperationStatus status;
  private String errorMessage;

  private VolumeOperationProcessor(Builder builder) {
    this.operationType = builder.operationType;
    this.operationUrl = builder.operationUrl;
    this.localFilePath = builder.localFilePath;
    this.headers = builder.headers;
    this.allowedVolumeIngestionPaths = builder.allowedVolumeIngestionPaths;
    this.isAllowedInputStreamForVolumeOperation = builder.isAllowedInputStreamForVolumeOperation;
    this.inputStream = builder.inputStream;
    this.getStreamReceiver = builder.getStreamReceiver;
    this.databricksHttpClient = builder.databricksHttpClient;
    this.status = builder.status;
    this.errorMessage = builder.errorMessage;
  }

  public static class Builder {
    private VolumeUtil.VolumeOperationType operationType;
    private String operationUrl;
    private String localFilePath = null;
    private Map<String, String> headers = new HashMap<>();
    private Set<String> allowedVolumeIngestionPaths = null;
    private boolean isAllowedInputStreamForVolumeOperation = false;
    private IDatabricksHttpClient databricksHttpClient = null;
    private InputStreamEntity inputStream = null;
    private Consumer<HttpEntity> getStreamReceiver = null;
    private VolumeOperationStatus status = VolumeOperationStatus.PENDING;
    private String errorMessage = null;

    public static Builder createBuilder() {
      return new Builder();
    }

    public Builder operationType(VolumeUtil.VolumeOperationType operationType) {
      this.operationType = operationType;
      return this;
    }

    public Builder operationUrl(String operationUrl) {
      this.operationUrl = operationUrl;
      return this;
    }

    public Builder localFilePath(String localFilePath) {
      this.localFilePath = localFilePath;
      return this;
    }

    public Builder headers(Map<String, String> headers) {
      this.headers = headers;
      return this;
    }

    public Builder allowedVolumeIngestionPathString(String allowedVolumeIngestionPathString) {
      this.allowedVolumeIngestionPaths = getAllowedPaths(allowedVolumeIngestionPathString);
      return this;
    }

    public Builder isAllowedInputStreamForVolumeOperation(
        boolean isAllowedInputStreamForVolumeOperation) {
      this.isAllowedInputStreamForVolumeOperation = isAllowedInputStreamForVolumeOperation;
      return this;
    }

    public Builder databricksHttpClient(IDatabricksHttpClient databricksHttpClient) {
      this.databricksHttpClient = databricksHttpClient;
      return this;
    }

    public Builder inputStream(InputStreamEntity inputStream) {
      this.inputStream = inputStream;
      return this;
    }

    public Builder getStreamReceiver(Consumer<HttpEntity> getStreamReceiver) {
      this.getStreamReceiver = getStreamReceiver;
      return this;
    }

    public Builder status(VolumeOperationStatus status) {
      this.status = status;
      return this;
    }

    public Builder errorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    public VolumeOperationProcessor build() {
      return new VolumeOperationProcessor(this);
    }
  }

  private static Set<String> getAllowedPaths(String allowedVolumeIngestionPathString) {
    if (allowedVolumeIngestionPathString == null || allowedVolumeIngestionPathString.isEmpty()) {
      return Collections.emptySet();
    }
    return new HashSet<>(Arrays.asList(allowedVolumeIngestionPathString.split(COMMA_SEPARATOR)));
  }

  void process() {
    LOGGER.debug(
        String.format(
            "Running volume operation {%s} on local file {%s}",
            operationType, localFilePath == null ? "" : localFilePath));
    if (operationUrl == null || operationUrl.isEmpty()) {
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Volume operation URL is not set";
      LOGGER.error(errorMessage);
      return;
    }
    validateLocalFilePath();
    if (status == VolumeOperationStatus.ABORTED) {
      return;
    }
    status = VolumeOperationStatus.RUNNING;
    switch (operationType) {
      case GET:
        executeGetOperation();
        break;
      case PUT:
        executePutOperation();
        break;
      case REMOVE:
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
    if (isAllowedInputStreamForVolumeOperation) {
      return;
    }

    if (allowedVolumeIngestionPaths.isEmpty()) {
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Volume ingestion paths are not set";
      LOGGER.error(errorMessage);
      return;
    }
    if (operationType == VolumeUtil.VolumeOperationType.REMOVE) {
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

  private void closeResponse(CloseableHttpResponse response) {
    if (response != null) {
      try {
        if (response.getEntity() != null) {
          EntityUtils.consume(response.getEntity());
        }
        response.close();
      } catch (IOException e) {
        /* silent close */
      }
    }
  }

  @VisibleForTesting
  void executeGetOperation() {
    HttpGet httpGet = new HttpGet(operationUrl);
    headers.forEach(httpGet::addHeader);

    HttpEntity entity;
    CloseableHttpResponse responseStream = null;
    try {
      // We return the input stream directly to clients, if they want to consume as input stream
      if (isAllowedInputStreamForVolumeOperation) {
        responseStream = databricksHttpClient.execute(httpGet);
        if (!HttpUtil.isSuccessfulHttpResponse(responseStream)) {
          status = VolumeOperationStatus.FAILED;
          errorMessage =
              String.format(
                  "Failed to fetch content from volume with error code {%s} for input stream and error {%s}",
                  responseStream.getStatusLine().getStatusCode(),
                  responseStream.getStatusLine().getReasonPhrase());
          LOGGER.error(errorMessage);
          closeResponse(responseStream);
          return;
        }
        getStreamReceiver.accept(responseStream.getEntity());
        status = VolumeOperationStatus.SUCCEEDED;
        return;
      }
    } catch (DatabricksHttpException e) {
      closeResponse(responseStream);
      status = VolumeOperationStatus.FAILED;
      errorMessage = "Failed to execute GET operation for input stream: " + e.getMessage();
      LOGGER.error(errorMessage);
      return;
    }

    // Copy the data in local file as requested by user
    File localFile = new File(localFilePath);
    if (localFile.exists()) {
      LOGGER.error("Local file already exists for GET operation {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file already exists";
      return;
    }

    try (CloseableHttpResponse response = databricksHttpClient.execute(httpGet)) {
      if (!HttpUtil.isSuccessfulHttpResponse(response)) {
        LOGGER.error(
            "Failed to fetch content from volume with error {%s} for local file {%s}",
            response.getStatusLine().getStatusCode(), localFilePath);
        status = VolumeOperationStatus.FAILED;
        errorMessage = "Failed to download file";
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
          status = VolumeOperationStatus.SUCCEEDED;
        } catch (FileNotFoundException e) {
          LOGGER.error("Local file path is invalid or a directory {}", localFilePath);
          status = VolumeOperationStatus.FAILED;
          errorMessage = "Local file path is invalid or a directory";
        } catch (IOException e) {
          // TODO: Add retries
          LOGGER.error(
              e,
              "Failed to write to local file {%s} with error {%s}",
              localFilePath,
              e.getMessage());
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

  @VisibleForTesting
  void executePutOperation() {
    HttpPut httpPut = new HttpPut(operationUrl);
    headers.forEach(httpPut::addHeader);

    if (isAllowedInputStreamForVolumeOperation) {
      if (inputStream == null) {
        status = VolumeOperationStatus.ABORTED;
        errorMessage = "InputStream not set for PUT operation";
        LOGGER.error(errorMessage);
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

    // Execute the request
    try (CloseableHttpResponse response = databricksHttpClient.execute(httpPut)) {
      // Process the response
      if (HttpUtil.isSuccessfulHttpResponse(response)) {
        status = VolumeOperationStatus.SUCCEEDED;
      } else {
        LOGGER.error(
            "Failed to upload file {%s} with error code: {%s}",
            localFilePath, response.getStatusLine().getStatusCode());
        // TODO: Add retries
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

  private boolean localFileHasErrorForPutOperation(File file) {
    if (!file.exists() || file.isDirectory()) {
      LOGGER.error("Local file does not exist or is a directory {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file does not exist or is a directory";
      return true;
    }
    if (file.length() == 0) {
      LOGGER.error("Local file is empty {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file is empty";
      return true;
    }

    if (file.length() > PUT_SIZE_LIMITS) {
      LOGGER.error("Local file too large {}", localFilePath);
      status = VolumeOperationStatus.ABORTED;
      errorMessage = "Local file too large";
      return true;
    }
    return false;
  }

  private void executeDeleteOperation() {
    // TODO: Implement AWS-specific logic if required
    HttpDelete httpDelete = new HttpDelete(operationUrl);
    headers.forEach(httpDelete::addHeader);
    try (CloseableHttpResponse response = databricksHttpClient.execute(httpDelete)) {
      if (HttpUtil.isSuccessfulHttpResponse(response)) {
        status = VolumeOperationStatus.SUCCEEDED;
      } else {
        LOGGER.error(
            "Failed to delete volume with error code: {%s}",
            response.getStatusLine().getStatusCode());
        status = VolumeOperationStatus.FAILED;
        errorMessage = "Failed to delete volume";
      }
    } catch (DatabricksHttpException | IOException e) {
      LOGGER.error(e, "Failed to delete volume with error {}", e.getMessage());
      status = VolumeOperationStatus.FAILED;
      errorMessage = "Failed to delete volume: " + e.getMessage();
    }
  }
}
