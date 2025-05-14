package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.api.impl.volume.DatabricksUCVolumeClient.getObjectFullPath;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.JSON_HTTP_HEADERS;
import static com.databricks.jdbc.common.util.VolumeUtil.VolumeOperationType.constructListPath;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;
import static com.databricks.jdbc.telemetry.TelemetryHelper.exportFailureLog;

import com.databricks.jdbc.api.IDatabricksVolumeClient;
import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.common.util.StringUtil;
import com.databricks.jdbc.common.util.VolumeUtil;
import com.databricks.jdbc.common.util.WildcardUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.filesystem.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.error.platform.NotFound;
import com.databricks.sdk.core.http.Request;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.hc.client5.http.async.methods.*;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.nio.entity.AsyncEntityProducers;
import org.apache.hc.core5.http.nio.support.AsyncRequestBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;

/** Implementation of Volume Client that directly calls SQL Exec API for the Volume Operations */
public class DBFSVolumeClient implements IDatabricksVolumeClient, Closeable {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DBFSVolumeClient.class);
  private final IDatabricksConnectionContext connectionContext;
  private final IDatabricksHttpClient databricksHttpClient;
  private VolumeInputStream volumeInputStream = null;
  private long volumeStreamContentLength = -1L;
  final WorkspaceClient workspaceClient;
  final ApiClient apiClient;
  private final String allowedVolumeIngestionPaths;
  private static final int MAX_CONCURRENT_PRESIGNED_REQUESTS = 50; // Adjust based on server limits
  private static final int MAX_RETRIES = 5;
  private static final long INITIAL_RETRY_DELAY_MS = 200;
  private static final long MAX_RETRY_DELAY_MS = 10000; // 10 seconds max delay
  private final Semaphore presignedUrlSemaphore = new Semaphore(MAX_CONCURRENT_PRESIGNED_REQUESTS);
  private final ThreadLocalRandom random = ThreadLocalRandom.current();

  @VisibleForTesting
  public DBFSVolumeClient(WorkspaceClient workspaceClient) {
    this.connectionContext = null;
    this.workspaceClient = workspaceClient;
    this.apiClient = workspaceClient.apiClient();
    this.databricksHttpClient = null;
    this.allowedVolumeIngestionPaths = "";
  }

  public DBFSVolumeClient(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.workspaceClient = getWorkspaceClientFromConnectionContext(connectionContext);
    this.apiClient = workspaceClient.apiClient();
    this.databricksHttpClient =
        DatabricksHttpClientFactory.getInstance()
            .getClient(connectionContext, HttpClientType.VOLUME);
    this.allowedVolumeIngestionPaths = connectionContext.getVolumeOperationAllowedPaths();
  }

  /** {@inheritDoc} */
  @Override
  public boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "Entering prefixExists method with parameters: catalog = {%s}, schema = {%s}, volume = {%s}, prefix = {%s}, caseSensitive = {%s}",
            catalog, schema, volume, prefix, caseSensitive));
    if (WildcardUtil.isNullOrEmpty(prefix)) {
      return false;
    }
    try {
      List<String> objects = listObjects(catalog, schema, volume, prefix, caseSensitive);
      return !objects.isEmpty();
    } catch (Exception e) {
      LOGGER.error(
          String.format(
              "Error checking prefix existence: catalog = {%s}, schema = {%s}, volume = {%s}, prefix = {%s}, caseSensitive = {%s}",
              catalog, schema, volume, prefix, caseSensitive),
          e);
      throw new DatabricksVolumeOperationException(
          "Error checking prefix existence: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean objectExists(
      String catalog, String schema, String volume, String objectPath, boolean caseSensitive)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "Entering objectExists method with parameters: catalog = {%s}, schema = {%s}, volume = {%s}, objectPath = {%s}, caseSensitive = {%s}",
            catalog, schema, volume, objectPath, caseSensitive));
    if (WildcardUtil.isNullOrEmpty(objectPath)) {
      return false;
    }
    try {
      String baseName = StringUtil.getBaseNameFromPath(objectPath);
      ListResponse listResponse =
          getListResponse(constructListPath(catalog, schema, volume, objectPath));
      if (listResponse != null && listResponse.getFiles() != null) {
        for (FileInfo file : listResponse.getFiles()) {
          String fileName = StringUtil.getBaseNameFromPath(file.getPath());
          if (caseSensitive ? fileName.equals(baseName) : fileName.equalsIgnoreCase(baseName)) {
            return true;
          }
        }
      }
      return false;
    } catch (Exception e) {
      LOGGER.error(
          String.format(
              "Error checking object existence: catalog = {%s}, schema = {%s}, volume = {%s}, objectPath = {%s}, caseSensitive = {%s}",
              catalog, schema, volume, objectPath, caseSensitive),
          e);
      throw new DatabricksVolumeOperationException(
          "Error checking object existence: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean volumeExists(
      String catalog, String schema, String volumeName, boolean caseSensitive) throws SQLException {
    LOGGER.debug(
        String.format(
            "Entering volumeExists method with parameters: catalog = {%s}, schema = {%s}, volumeName = {%s}, caseSensitive = {%s}",
            catalog, schema, volumeName, caseSensitive));
    if (WildcardUtil.isNullOrEmpty(volumeName)) {
      return false;
    }
    try {
      String volumePath = StringUtil.getVolumePath(catalog, schema, volumeName);
      // If getListResponse does not throw, then the volume exists (even if it's empty).
      getListResponse(volumePath);
      return true;
    } catch (DatabricksVolumeOperationException e) {
      // If the exception indicates an invalid path (i.e. missing volume name),
      // then the volume does not exist. Otherwise, rethrow with proper error details.
      if (e.getCause() instanceof NotFound) {
        return false;
      }
      LOGGER.error(
          String.format(
              "Error checking volume existence: catalog = {%s}, schema = {%s}, volumeName = {%s}, caseSensitive = {%s}",
              catalog, schema, volumeName, caseSensitive),
          e);
      throw new DatabricksVolumeOperationException(
          "Error checking volume existence: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<String> listObjects(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "Entering listObjects method with parameters: catalog={%s}, schema={%s}, volume={%s}, prefix={%s}, caseSensitive={%s}",
            catalog, schema, volume, prefix, caseSensitive));

    String basename = StringUtil.getBaseNameFromPath(prefix);
    ListResponse listResponse = getListResponse(constructListPath(catalog, schema, volume, prefix));

    return listResponse.getFiles().stream()
        .map(FileInfo::getPath)
        .map(path -> path.substring(path.lastIndexOf('/') + 1))
        . // Get the file name after the last slash
        filter(fileName -> StringUtil.checkPrefixMatch(basename, fileName, caseSensitive))
        . // Comparing whether the prefix matches or not
        collect(Collectors.toList());
  }

  /** {@inheritDoc} */
  @Override
  public boolean getObject(
      String catalog, String schema, String volume, String objectPath, String localPath)
      throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format(
            "Entering getObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}, localPath={%s}",
            catalog, schema, volume, objectPath, localPath));

    try {
      // Fetching the Pre signed URL
      CreateDownloadUrlResponse response =
          getCreateDownloadUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      // Downloading the object from the presigned url
      VolumeOperationProcessor volumeOperationProcessor =
          VolumeOperationProcessor.Builder.createBuilder()
              .operationType(VolumeUtil.VolumeOperationType.GET)
              .operationUrl(response.getUrl())
              .localFilePath(localPath)
              .allowedVolumeIngestionPathString(allowedVolumeIngestionPaths)
              .databricksHttpClient(databricksHttpClient)
              .build();

      volumeOperationProcessor.process();
      checkVolumeOperationError(volumeOperationProcessor);
    } catch (DatabricksSQLException e) {
      String errorMessage = String.format("Failed to get object - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, e, DatabricksDriverErrorCode.VOLUME_OPERATION_FILE_DOWNLOAD_ERROR);
    }
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public InputStreamEntity getObject(
      String catalog, String schema, String volume, String objectPath)
      throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format(
            "Entering getObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}",
            catalog, schema, volume, objectPath));

    try {
      // Fetching the Pre Signed Url
      CreateDownloadUrlResponse response =
          getCreateDownloadUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      // Downloading the object from the presigned url
      VolumeOperationProcessor volumeOperationProcessor =
          VolumeOperationProcessor.Builder.createBuilder()
              .operationType(VolumeUtil.VolumeOperationType.GET)
              .operationUrl(response.getUrl())
              .isAllowedInputStreamForVolumeOperation(true)
              .databricksHttpClient(databricksHttpClient)
              .getStreamReceiver(
                  (entity) -> {
                    try {
                      this.setVolumeOperationEntityStream(entity);
                    } catch (Exception e) {
                      throw new RuntimeException(
                          "Failed to set result set volumeOperationEntityStream", e);
                    }
                  })
              .build();

      volumeOperationProcessor.process();
      checkVolumeOperationError(volumeOperationProcessor);

      return getVolumeOperationInputStream();
    } catch (DatabricksSQLException e) {
      String errorMessage = String.format("Failed to get object - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, e, DatabricksDriverErrorCode.VOLUME_OPERATION_FILE_DOWNLOAD_ERROR);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean putObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean toOverwrite)
      throws DatabricksVolumeOperationException {

    LOGGER.debug(
        String.format(
            "Entering putObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}, localPath={%s}",
            catalog, schema, volume, objectPath, localPath));

    try {
      // Fetching the Pre Signed Url
      CreateUploadUrlResponse response =
          getCreateUploadUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      // Uploading the object to the Pre Signed Url
      VolumeOperationProcessor volumeOperationProcessor =
          VolumeOperationProcessor.Builder.createBuilder()
              .operationType(VolumeUtil.VolumeOperationType.PUT)
              .operationUrl(response.getUrl())
              .localFilePath(localPath)
              .allowedVolumeIngestionPathString(allowedVolumeIngestionPaths)
              .databricksHttpClient(databricksHttpClient)
              .build();

      volumeOperationProcessor.process();
      checkVolumeOperationError(volumeOperationProcessor);
    } catch (DatabricksSQLException e) {
      String errorMessage = String.format("Failed to put object - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, e, DatabricksDriverErrorCode.VOLUME_OPERATION_PUT_OPERATION_EXCEPTION);
    }
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean putObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      InputStream inputStream,
      long contentLength,
      boolean toOverwrite)
      throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format(
            "Entering putObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}, inputStream={%s}, contentLength={%s}, toOverwrite={%s}",
            catalog, schema, volume, objectPath, inputStream, contentLength, toOverwrite));

    try {
      // Fetching the Pre Signed Url
      CreateUploadUrlResponse response =
          getCreateUploadUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      InputStreamEntity inputStreamEntity = new InputStreamEntity(inputStream, contentLength);
      // Uploading the object to the Pre Signed Url
      VolumeOperationProcessor volumeOperationProcessor =
          VolumeOperationProcessor.Builder.createBuilder()
              .operationType(VolumeUtil.VolumeOperationType.PUT)
              .operationUrl(response.getUrl())
              .isAllowedInputStreamForVolumeOperation(true)
              .inputStream(inputStreamEntity)
              .databricksHttpClient(databricksHttpClient)
              .build();

      volumeOperationProcessor.process();
      checkVolumeOperationError(volumeOperationProcessor);
    } catch (DatabricksSQLException e) {
      String errorMessage =
          String.format("Failed to put object with inputStream- {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, e, DatabricksDriverErrorCode.VOLUME_OPERATION_PUT_OPERATION_EXCEPTION);
    }
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteObject(String catalog, String schema, String volume, String objectPath)
      throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format(
            "Entering deleteObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}",
            catalog, schema, volume, objectPath));

    try {
      // Fetching the Pre Signed Url
      CreateDeleteUrlResponse response =
          getCreateDeleteUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      // Uploading the object to the Pre Signed Url
      VolumeOperationProcessor volumeOperationProcessor =
          VolumeOperationProcessor.Builder.createBuilder()
              .operationType(VolumeUtil.VolumeOperationType.REMOVE)
              .operationUrl(response.getUrl())
              .isAllowedInputStreamForVolumeOperation(true)
              .databricksHttpClient(databricksHttpClient)
              .build();

      volumeOperationProcessor.process();
      checkVolumeOperationError(volumeOperationProcessor);
    } catch (DatabricksSQLException e) {
      String errorMessage = String.format("Failed to delete object {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, e, DatabricksDriverErrorCode.VOLUME_OPERATION_DELETE_OPERATION_EXCEPTION);
    }
    return true;
  }

  WorkspaceClient getWorkspaceClientFromConnectionContext(
      IDatabricksConnectionContext connectionContext) {
    return DatabricksClientConfiguratorManager.getInstance()
        .getConfigurator(connectionContext)
        .getWorkspaceClient();
  }

  /** Fetches the pre signed url for uploading to the volume using the SQL Exec API */
  CreateUploadUrlResponse getCreateUploadUrlResponse(String objectPath)
      throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format(
            "Entering getCreateUploadUrlResponse method with parameters: objectPath={%s}",
            objectPath));

    CreateUploadUrlRequest request = new CreateUploadUrlRequest(objectPath);
    try {
      Request req = new Request(Request.POST, CREATE_UPLOAD_URL_PATH, apiClient.serialize(request));
      req.withHeaders(JSON_HTTP_HEADERS);
      return apiClient.execute(req, CreateUploadUrlResponse.class);
    } catch (IOException | DatabricksException e) {
      String errorMessage =
          String.format("Failed to get create upload url response - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, e, DatabricksDriverErrorCode.VOLUME_OPERATION_URL_GENERATION_ERROR);
    }
  }

  /** Fetches the pre signed url for downloading the object contents using the SQL Exec API */
  CreateDownloadUrlResponse getCreateDownloadUrlResponse(String objectPath)
      throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format(
            "Entering getCreateDownloadUrlResponse method with parameters: objectPath={%s}",
            objectPath));

    CreateDownloadUrlRequest request = new CreateDownloadUrlRequest(objectPath);

    try {
      Request req =
          new Request(Request.POST, CREATE_DOWNLOAD_URL_PATH, apiClient.serialize(request));
      req.withHeaders(JSON_HTTP_HEADERS);
      return apiClient.execute(req, CreateDownloadUrlResponse.class);
    } catch (IOException | DatabricksException e) {
      String errorMessage =
          String.format("Failed to get create download url response - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, e, DatabricksDriverErrorCode.VOLUME_OPERATION_URL_GENERATION_ERROR);
    }
  }

  /** Fetches the pre signed url for deleting object from the volume using the SQL Exec API */
  CreateDeleteUrlResponse getCreateDeleteUrlResponse(String objectPath)
      throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format(
            "Entering getCreateDeleteUrlResponse method with parameters: objectPath={%s}",
            objectPath));
    CreateDeleteUrlRequest request = new CreateDeleteUrlRequest(objectPath);

    try {
      Request req = new Request(Request.POST, CREATE_DELETE_URL_PATH, apiClient.serialize(request));
      req.withHeaders(JSON_HTTP_HEADERS);
      return apiClient.execute(req, CreateDeleteUrlResponse.class);
    } catch (IOException | DatabricksException e) {
      String errorMessage =
          String.format("Failed to get create delete url response - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, e, DatabricksDriverErrorCode.VOLUME_OPERATION_URL_GENERATION_ERROR);
    }
  }

  /** Fetches the list of objects in the volume using the SQL Exec API */
  ListResponse getListResponse(String listPath) throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format("Entering getListResponse method with parameters : listPath={%s}", listPath));
    ListRequest request = new ListRequest(listPath);
    try {
      Request req = new Request(Request.GET, LIST_PATH);
      req.withHeaders(JSON_HTTP_HEADERS);
      ApiClient.setQuery(req, request);
      return apiClient.execute(req, ListResponse.class);
    } catch (IOException | DatabricksException e) {
      String errorMessage = String.format("Failed to get list response - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(
          errorMessage, e, DatabricksDriverErrorCode.VOLUME_OPERATION_INVALID_STATE);
    }
  }

  private void checkVolumeOperationError(VolumeOperationProcessor volumeOperationProcessor)
      throws DatabricksSQLException {
    if (volumeOperationProcessor.getStatus() == VolumeOperationStatus.FAILED) {
      throw new DatabricksSQLException(
          "Volume operation failed: " + volumeOperationProcessor.getErrorMessage(),
          DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (volumeOperationProcessor.getStatus() == VolumeOperationStatus.ABORTED) {
      throw new DatabricksSQLException(
          "Volume operation aborted: " + volumeOperationProcessor.getErrorMessage(),
          DatabricksDriverErrorCode.INVALID_STATE);
    }
  }

  public void setVolumeOperationEntityStream(HttpEntity httpEntity) throws IOException {
    this.volumeInputStream = new VolumeInputStream(httpEntity);
    this.volumeStreamContentLength = httpEntity.getContentLength();
  }

  public InputStreamEntity getVolumeOperationInputStream() {
    return new InputStreamEntity(this.volumeInputStream, this.volumeStreamContentLength);
  }

  @Override
  public void close() throws IOException {
    DatabricksThreadContextHolder.clearConnectionContext();
  }

  /** Upload multiple files from local paths to DBFS volume in parallel. */
  @Override
  public List<VolumePutResult> putFiles(
      String catalog,
      String schema,
      String volume,
      List<String> objectPaths,
      List<String> localPaths,
      boolean overwrite) {

    LOGGER.debug(
        String.format(
            "Entering putFiles: catalog=%s, schema=%s, volume=%s, files=%d",
            catalog, schema, volume, objectPaths.size()));

    if (objectPaths.size() != localPaths.size()) {
      String errorMessage = "objectPaths and localPaths – sizes differ";
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    // Track progress and timing
    java.util.concurrent.atomic.AtomicInteger completedCounter =
        new java.util.concurrent.atomic.AtomicInteger(0);
    long startTime = System.nanoTime();

    // Store completion futures and results
    List<CompletableFuture<VolumePutResult>> futures = new ArrayList<>(objectPaths.size());

    for (int i = 0; i < objectPaths.size(); i++) {
      final String objPath = objectPaths.get(i);
      final String fullPath = getObjectFullPath(catalog, schema, volume, objPath);
      final String localPath = localPaths.get(i);
      final java.io.File file = new java.io.File(localPath);
      final int fileIndex = i;

      if (!file.exists() || !file.isFile()) {
        String errorMessage = "File not found or not a file: " + localPath;
        LOGGER.error(errorMessage);
        exportFailureLog(
            connectionContext,
            DatabricksDriverErrorCode.VOLUME_OPERATION_LOCAL_FILE_EXISTS_ERROR.name(),
            errorMessage);
        futures.add(
            CompletableFuture.completedFuture(
                new VolumePutResult(404, VolumeOperationStatus.FAILED, errorMessage)));
        completedCounter.incrementAndGet();
        continue;
      }

      LOGGER.debug(
          String.format(
              "Uploading file %d/%d: %s → %s (%d bytes)",
              fileIndex + 1, objectPaths.size(), localPath, objPath, file.length()));

      // Create a CompletableFuture for this file upload
      CompletableFuture<VolumePutResult> uploadFuture = new CompletableFuture<>();
      futures.add(uploadFuture);

      // Use retry logic with rate limiting for presigned URL requests
      requestPresignedUrlWithRetry(fullPath, objPath, 1)
          .thenAccept(
              response -> {
                String presignedUrl = response.getUrl();
                LOGGER.debug(
                    String.format("Got presigned URL for file %d: %s", fileIndex + 1, objPath));

                // Step 2: Upload file to presigned URL (no auth needed)
                try {
                  AsyncRequestProducer uploadProducer =
                      AsyncRequestBuilder.put()
                          .setUri(URI.create(presignedUrl))
                          .setEntity(AsyncEntityProducers.create(file, ContentType.DEFAULT_BINARY))
                          .build();
                  AsyncResponseConsumer<SimpleHttpResponse> uploadConsumer =
                      SimpleResponseConsumer.create();

                  // Create callback for file upload with retry support
                  FileUploadCallback uploadCallback =
                      new FileUploadCallback(
                          completedCounter,
                          uploadFuture,
                          objPath,
                          objectPaths.size(),
                          startTime,
                          fullPath,
                          file,
                          1);

                  databricksHttpClient.executeAsync(uploadProducer, uploadConsumer, uploadCallback);
                } catch (Exception e) {
                  String errorMessage =
                      String.format("Error uploading file %s: %s", objPath, e.getMessage());
                  LOGGER.error(errorMessage, e);
                  completedCounter.incrementAndGet();
                  uploadFuture.complete(
                      new VolumePutResult(500, VolumeOperationStatus.FAILED, errorMessage));
                }
              })
          .exceptionally(
              e -> {
                String errorMessage =
                    String.format(
                        "Failed to get presigned URL for %s: %s", objPath, e.getMessage());
                LOGGER.error(errorMessage, e);
                completedCounter.incrementAndGet();
                uploadFuture.complete(
                    new VolumePutResult(500, VolumeOperationStatus.FAILED, errorMessage));
                return null;
              });
    }

    // Wait for all operations to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    // Convert futures to results
    List<VolumePutResult> results = new ArrayList<>(futures.size());
    for (CompletableFuture<VolumePutResult> future : futures) {
      results.add(future.join());
    }

    // Log final results
    long successCount = 0;
    for (VolumePutResult result : results) {
      if (result.getStatus() == VolumeOperationStatus.SUCCEEDED) {
        successCount++;
      }
    }

    double totalTimeSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
    double filesPerSecond = objectPaths.size() / totalTimeSeconds;

    LOGGER.info(
        String.format(
            "Completed uploads: %d/%d files successful in %.2f seconds (%.2f files/sec)",
            successCount, objectPaths.size(), totalTimeSeconds, filesPerSecond));

    return results;
  }

  /** Callback handler for file upload requests */
  private class FileUploadCallback implements FutureCallback<SimpleHttpResponse> {
    private final java.util.concurrent.atomic.AtomicInteger completedCounter;
    private final CompletableFuture<VolumePutResult> uploadFuture;
    private final String objPath;
    private final int totalFiles;
    private final long startTime;
    private final String fullPath;
    private final java.io.File file;
    private final int attempt;
    private static final int MAX_UPLOAD_RETRIES = 3;

    public FileUploadCallback(
        java.util.concurrent.atomic.AtomicInteger completedCounter,
        CompletableFuture<VolumePutResult> uploadFuture,
        String objPath,
        int totalFiles,
        long startTime,
        String fullPath,
        java.io.File file,
        int attempt) {
      this.completedCounter = completedCounter;
      this.uploadFuture = uploadFuture;
      this.objPath = objPath;
      this.totalFiles = totalFiles;
      this.startTime = startTime;
      this.fullPath = fullPath;
      this.file = file;
      this.attempt = attempt;
    }

    @Override
    public void completed(SimpleHttpResponse uploadResult) {
      if (uploadResult.getCode() >= 200 && uploadResult.getCode() < 300) {
        // Success case
        VolumeOperationStatus status = VolumeOperationStatus.SUCCEEDED;

        int completedCount = completedCounter.incrementAndGet();
        if (completedCount % 10 == 0 || completedCount == totalFiles) {
          double elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
          double filesPerSecond = completedCount / elapsedSeconds;
          LOGGER.info(
              String.format(
                  "Progress: %d/%d files (%.2f files/sec)",
                  completedCount, totalFiles, filesPerSecond));
        }

        uploadFuture.complete(new VolumePutResult(uploadResult.getCode(), status, null));
      } else if ((uploadResult.getCode() == 500
              || uploadResult.getCode() == 502
              || uploadResult.getCode() == 503
              || uploadResult.getCode() == 504)
          && attempt < MAX_UPLOAD_RETRIES) {
        // Server error - retry with backoff
        long retryDelayMs = calculateRetryDelay(attempt);
        LOGGER.warn(
            String.format(
                "Upload failed for %s: HTTP %d - %s. Retrying in %d ms (attempt %d/%d)",
                objPath,
                uploadResult.getCode(),
                uploadResult.getReasonPhrase(),
                retryDelayMs,
                attempt,
                MAX_UPLOAD_RETRIES));

        // Retry the entire upload process
        retryFileUpload(retryDelayMs);
      } else {
        // Permanent failure or max retries exceeded
        VolumeOperationStatus status = VolumeOperationStatus.FAILED;
        String message = uploadResult.getReasonPhrase();

        LOGGER.error(
            String.format(
                "Upload failed for %s: HTTP %d - %s", objPath, uploadResult.getCode(), message));

        completedCounter.incrementAndGet();
        uploadFuture.complete(new VolumePutResult(uploadResult.getCode(), status, message));
      }
    }

    @Override
    public void failed(Exception ex) {
      if (attempt < MAX_UPLOAD_RETRIES) {
        long retryDelayMs = calculateRetryDelay(attempt);
        LOGGER.warn(
            String.format(
                "Upload failed for %s: %s. Retrying in %d ms (attempt %d/%d)",
                objPath, ex.getMessage(), retryDelayMs, attempt, MAX_UPLOAD_RETRIES));

        // Retry the entire upload process
        retryFileUpload(retryDelayMs);
      } else {
        LOGGER.error(String.format("Upload failed for %s: %s", objPath, ex.getMessage()), ex);
        completedCounter.incrementAndGet();
        uploadFuture.complete(
            new VolumePutResult(500, VolumeOperationStatus.FAILED, ex.getMessage()));
      }
    }

    private void retryFileUpload(long delayMs) {
      CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS)
          .execute(
              () -> {
                // Get a new presigned URL and retry the upload
                requestPresignedUrlWithRetry(fullPath, objPath, 1)
                    .thenAccept(
                        response -> {
                          String presignedUrl = response.getUrl();
                          LOGGER.debug(
                              String.format(
                                  "Got new presigned URL for retry of %s (attempt %d)",
                                  objPath, attempt + 1));

                          try {
                            AsyncRequestProducer uploadProducer =
                                AsyncRequestBuilder.put()
                                    .setUri(URI.create(presignedUrl))
                                    .setEntity(
                                        AsyncEntityProducers.create(
                                            file, ContentType.DEFAULT_BINARY))
                                    .build();
                            AsyncResponseConsumer<SimpleHttpResponse> uploadConsumer =
                                SimpleResponseConsumer.create();

                            // Create callback with incremented attempt count
                            FileUploadCallback uploadCallback =
                                new FileUploadCallback(
                                    completedCounter,
                                    uploadFuture,
                                    objPath,
                                    totalFiles,
                                    startTime,
                                    fullPath,
                                    file,
                                    attempt + 1);

                            databricksHttpClient.executeAsync(
                                uploadProducer, uploadConsumer, uploadCallback);
                          } catch (Exception e) {
                            String errorMessage =
                                String.format(
                                    "Error setting up retry for %s: %s", objPath, e.getMessage());
                            LOGGER.error(errorMessage, e);
                            completedCounter.incrementAndGet();
                            uploadFuture.complete(
                                new VolumePutResult(
                                    500, VolumeOperationStatus.FAILED, errorMessage));
                          }
                        })
                    .exceptionally(
                        e -> {
                          String errorMessage =
                              String.format(
                                  "Failed to get presigned URL for retry of %s: %s",
                                  objPath, e.getMessage());
                          LOGGER.error(errorMessage, e);
                          completedCounter.incrementAndGet();
                          uploadFuture.complete(
                              new VolumePutResult(500, VolumeOperationStatus.FAILED, errorMessage));
                          return null;
                        });
              });
    }

    @Override
    public void cancelled() {
      LOGGER.warn(String.format("Upload cancelled for %s", objPath));
      completedCounter.incrementAndGet();
      uploadFuture.complete(
          new VolumePutResult(499, VolumeOperationStatus.ABORTED, "Upload cancelled"));
    }
  }

  /** Upload multiple files from input streams to DBFS volume in parallel. */
  public List<VolumePutResult> putFiles(
      String catalog,
      String schema,
      String volume,
      List<String> objectPaths,
      List<InputStream> inputStreams,
      List<Long> contentLengths,
      boolean overwrite) {

    LOGGER.debug(
        String.format(
            "Entering putFiles: catalog=%s, schema=%s, volume=%s, streams=%d",
            catalog, schema, volume, objectPaths.size()));

    if (objectPaths.size() != inputStreams.size() || inputStreams.size() != contentLengths.size()) {
      String errorMessage = "objectPaths, inputStreams, contentLengths – sizes differ";
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    // Track progress and timing
    java.util.concurrent.atomic.AtomicInteger completedCounter =
        new java.util.concurrent.atomic.AtomicInteger(0);
    long startTime = System.nanoTime();

    // Store completion futures and results
    List<CompletableFuture<VolumePutResult>> futures = new ArrayList<>(objectPaths.size());

    for (int i = 0; i < objectPaths.size(); i++) {
      final String objPath = objectPaths.get(i);
      final String fullPath = getObjectFullPath(catalog, schema, volume, objPath);
      final InputStream inputStream = inputStreams.get(i);
      final long contentLength = contentLengths.get(i);
      final int fileIndex = i;

      // Try to mark the stream for possible retries if supported
      if (inputStream.markSupported()) {
        try {
          inputStream.mark((int) contentLength);
        } catch (Exception e) {
          LOGGER.warn("Could not mark input stream for potential retries: " + e.getMessage());
        }
      }

      LOGGER.debug(
          String.format(
              "Uploading stream %d/%d: %s (%d bytes)",
              fileIndex + 1, objectPaths.size(), objPath, contentLength));

      // Create a CompletableFuture for this stream upload
      CompletableFuture<VolumePutResult> uploadFuture = new CompletableFuture<>();
      futures.add(uploadFuture);

      // Use retry logic with rate limiting for presigned URL requests
      requestPresignedUrlWithRetry(fullPath, objPath, 1)
          .thenAccept(
              response -> {
                String presignedUrl = response.getUrl();
                LOGGER.debug(
                    String.format("Got presigned URL for stream %d: %s", fileIndex + 1, objPath));

                // Step 2: Upload stream to presigned URL (no auth needed)
                try {
                  AsyncEntityProducer entity =
                      new InputStreamFixedLenProducer(
                          inputStream, contentLength, ContentType.APPLICATION_OCTET_STREAM);

                  AsyncRequestProducer uploadProducer =
                      AsyncRequestBuilder.put()
                          .setUri(URI.create(presignedUrl))
                          .setEntity(entity)
                          .build();
                  AsyncResponseConsumer<SimpleHttpResponse> uploadConsumer =
                      SimpleResponseConsumer.create();

                  // Create callback for stream upload with retry support
                  StreamUploadCallback uploadCallback =
                      new StreamUploadCallback(
                          completedCounter,
                          uploadFuture,
                          objPath,
                          objectPaths.size(),
                          startTime,
                          fullPath,
                          inputStream,
                          contentLength,
                          1);

                  databricksHttpClient.executeAsync(uploadProducer, uploadConsumer, uploadCallback);
                } catch (Exception e) {
                  String errorMessage =
                      String.format("Error uploading stream %s: %s", objPath, e.getMessage());
                  LOGGER.error(errorMessage, e);
                  completedCounter.incrementAndGet();
                  uploadFuture.complete(
                      new VolumePutResult(500, VolumeOperationStatus.FAILED, errorMessage));
                }
              })
          .exceptionally(
              e -> {
                String errorMessage =
                    String.format(
                        "Failed to get presigned URL for %s: %s", objPath, e.getMessage());
                LOGGER.error(errorMessage, e);
                completedCounter.incrementAndGet();
                uploadFuture.complete(
                    new VolumePutResult(500, VolumeOperationStatus.FAILED, errorMessage));
                return null;
              });
    }

    // Wait for all operations to complete
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

    // Convert futures to results
    List<VolumePutResult> results = new ArrayList<>(futures.size());
    for (CompletableFuture<VolumePutResult> future : futures) {
      results.add(future.join());
    }

    // Log final results
    long successCount = 0;
    for (VolumePutResult result : results) {
      if (result.getStatus() == VolumeOperationStatus.SUCCEEDED) {
        successCount++;
      }
    }

    double totalTimeSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
    double streamsPerSecond = objectPaths.size() / totalTimeSeconds;

    LOGGER.info(
        String.format(
            "Completed uploads: %d/%d streams successful in %.2f seconds (%.2f streams/sec)",
            successCount, objectPaths.size(), totalTimeSeconds, streamsPerSecond));

    return results;
  }

  /** Callback handler for stream upload requests */
  private class StreamUploadCallback implements FutureCallback<SimpleHttpResponse> {
    private final java.util.concurrent.atomic.AtomicInteger completedCounter;
    private final CompletableFuture<VolumePutResult> uploadFuture;
    private final String objPath;
    private final int totalFiles;
    private final long startTime;
    private final String fullPath;
    private final InputStream inputStream;
    private final long contentLength;
    private final int attempt;
    private static final int MAX_UPLOAD_RETRIES = 3;

    public StreamUploadCallback(
        java.util.concurrent.atomic.AtomicInteger completedCounter,
        CompletableFuture<VolumePutResult> uploadFuture,
        String objPath,
        int totalFiles,
        long startTime,
        String fullPath,
        InputStream inputStream,
        long contentLength,
        int attempt) {
      this.completedCounter = completedCounter;
      this.uploadFuture = uploadFuture;
      this.objPath = objPath;
      this.totalFiles = totalFiles;
      this.startTime = startTime;
      this.fullPath = fullPath;
      this.inputStream = inputStream;
      this.contentLength = contentLength;
      this.attempt = attempt;
    }

    @Override
    public void completed(SimpleHttpResponse uploadResult) {
      if (uploadResult.getCode() >= 200 && uploadResult.getCode() < 300) {
        // Success case
        VolumeOperationStatus status = VolumeOperationStatus.SUCCEEDED;

        int completedCount = completedCounter.incrementAndGet();
        if (completedCount % 10 == 0 || completedCount == totalFiles) {
          double elapsedSeconds = (System.nanoTime() - startTime) / 1_000_000_000.0;
          double streamsPerSecond = completedCount / elapsedSeconds;
          LOGGER.info(
              String.format(
                  "Progress: %d/%d streams (%.2f streams/sec)",
                  completedCount, totalFiles, streamsPerSecond));
        }

        uploadFuture.complete(new VolumePutResult(uploadResult.getCode(), status, null));
      } else if ((uploadResult.getCode() == 500
              || uploadResult.getCode() == 502
              || uploadResult.getCode() == 503
              || uploadResult.getCode() == 504)
          && attempt < MAX_UPLOAD_RETRIES) {
        // Server error - retry with backoff
        long retryDelayMs = calculateRetryDelay(attempt);
        LOGGER.warn(
            String.format(
                "Upload failed for %s: HTTP %d - %s. Retrying in %d ms (attempt %d/%d)",
                objPath,
                uploadResult.getCode(),
                uploadResult.getReasonPhrase(),
                retryDelayMs,
                attempt,
                MAX_UPLOAD_RETRIES));

        // Retry the entire upload process
        retryStreamUpload(retryDelayMs);
      } else {
        // Permanent failure or max retries exceeded
        VolumeOperationStatus status = VolumeOperationStatus.FAILED;
        String message = uploadResult.getReasonPhrase();

        LOGGER.error(
            String.format(
                "Upload failed for %s: HTTP %d - %s", objPath, uploadResult.getCode(), message));

        completedCounter.incrementAndGet();
        uploadFuture.complete(new VolumePutResult(uploadResult.getCode(), status, message));
      }
    }

    @Override
    public void failed(Exception ex) {
      if (attempt < MAX_UPLOAD_RETRIES) {
        long retryDelayMs = calculateRetryDelay(attempt);
        LOGGER.warn(
            String.format(
                "Upload failed for %s: %s. Retrying in %d ms (attempt %d/%d)",
                objPath, ex.getMessage(), retryDelayMs, attempt, MAX_UPLOAD_RETRIES));

        // Retry the entire upload process
        retryStreamUpload(retryDelayMs);
      } else {
        LOGGER.error(String.format("Upload failed for %s: %s", objPath, ex.getMessage()), ex);
        completedCounter.incrementAndGet();
        uploadFuture.complete(
            new VolumePutResult(500, VolumeOperationStatus.FAILED, ex.getMessage()));
      }
    }

    private void retryStreamUpload(long delayMs) {
      CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS)
          .execute(
              () -> {
                // Need to reset the input stream if possible
                InputStream retryStream = inputStream;
                if (inputStream.markSupported()) {
                  try {
                    inputStream.reset();
                  } catch (IOException e) {
                    LOGGER.warn("Could not reset input stream for retry: " + e.getMessage());
                    // Will need to pass in a fresh stream
                  }
                } else {
                  LOGGER.warn("Input stream does not support reset for retry");
                  // Will need to pass in a fresh stream
                }

                // Get a new presigned URL and retry the upload
                requestPresignedUrlWithRetry(fullPath, objPath, 1)
                    .thenAccept(
                        response -> {
                          String presignedUrl = response.getUrl();
                          LOGGER.debug(
                              String.format(
                                  "Got new presigned URL for retry of %s (attempt %d)",
                                  objPath, attempt + 1));

                          try {
                            AsyncEntityProducer entity =
                                new InputStreamFixedLenProducer(
                                    retryStream,
                                    contentLength,
                                    ContentType.APPLICATION_OCTET_STREAM);

                            AsyncRequestProducer uploadProducer =
                                AsyncRequestBuilder.put()
                                    .setUri(URI.create(presignedUrl))
                                    .setEntity(entity)
                                    .build();
                            AsyncResponseConsumer<SimpleHttpResponse> uploadConsumer =
                                SimpleResponseConsumer.create();

                            // Create callback with incremented attempt count
                            StreamUploadCallback uploadCallback =
                                new StreamUploadCallback(
                                    completedCounter,
                                    uploadFuture,
                                    objPath,
                                    totalFiles,
                                    startTime,
                                    fullPath,
                                    retryStream,
                                    contentLength,
                                    attempt + 1);

                            databricksHttpClient.executeAsync(
                                uploadProducer, uploadConsumer, uploadCallback);
                          } catch (Exception e) {
                            String errorMessage =
                                String.format(
                                    "Error setting up retry for %s: %s", objPath, e.getMessage());
                            LOGGER.error(errorMessage, e);
                            completedCounter.incrementAndGet();
                            uploadFuture.complete(
                                new VolumePutResult(
                                    500, VolumeOperationStatus.FAILED, errorMessage));
                          }
                        })
                    .exceptionally(
                        e -> {
                          String errorMessage =
                              String.format(
                                  "Failed to get presigned URL for retry of %s: %s",
                                  objPath, e.getMessage());
                          LOGGER.error(errorMessage, e);
                          completedCounter.incrementAndGet();
                          uploadFuture.complete(
                              new VolumePutResult(500, VolumeOperationStatus.FAILED, errorMessage));
                          return null;
                        });
              });
    }

    @Override
    public void cancelled() {
      LOGGER.warn(String.format("Upload cancelled for %s", objPath));
      completedCounter.incrementAndGet();
      uploadFuture.complete(
          new VolumePutResult(499, VolumeOperationStatus.ABORTED, "Upload cancelled"));
    }
  }

  private CompletableFuture<CreateUploadUrlResponse> requestPresignedUrlWithRetry(
      String fullPath, String objPath, int attempt) {
    CompletableFuture<CreateUploadUrlResponse> future = new CompletableFuture<>();

    try {
      // Acquire semaphore permit for rate limiting
      presignedUrlSemaphore.acquire();
      LOGGER.debug(String.format("Requesting presigned URL for %s (attempt %d)", objPath, attempt));

      // Create request for presigned URL
      CreateUploadUrlRequest request = new CreateUploadUrlRequest(fullPath);
      String requestBody;
      try {
        requestBody = apiClient.serialize(request);
      } catch (IOException e) {
        presignedUrlSemaphore.release();
        future.completeExceptionally(e);
        return future;
      }

      // Build async request with auth headers
      AsyncRequestBuilder requestBuilder =
          AsyncRequestBuilder.post(
              URI.create(connectionContext.getHostUrl() + CREATE_UPLOAD_URL_PATH));

      try {
        Map<String, String> authHeaders = workspaceClient.config().authenticate();
        for (Map.Entry<String, String> header : authHeaders.entrySet()) {
          requestBuilder.addHeader(header.getKey(), header.getValue());
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to add authentication headers: " + e.getMessage(), e);
      }

      // Add standard headers
      for (Map.Entry<String, String> entry : JSON_HTTP_HEADERS.entrySet()) {
        requestBuilder.addHeader(entry.getKey(), entry.getValue());
      }

      // Set request body
      requestBuilder.setEntity(
          AsyncEntityProducers.create(requestBody.getBytes(), ContentType.APPLICATION_JSON));

      // Execute request
      databricksHttpClient.executeAsync(
          requestBuilder.build(),
          SimpleResponseConsumer.create(),
          new FutureCallback<SimpleHttpResponse>() {
            @Override
            public void completed(SimpleHttpResponse result) {
              // Always release the semaphore when done
              presignedUrlSemaphore.release();

              if (result.getCode() >= 200 && result.getCode() < 300) {
                try {
                  String responseBody = result.getBodyText();
                  CreateUploadUrlResponse response =
                      JsonUtil.getMapper().readValue(responseBody, CreateUploadUrlResponse.class);
                  future.complete(response);
                } catch (Exception e) {
                  future.completeExceptionally(e);
                }
              } else if (result.getCode() == 429 && attempt < MAX_RETRIES) {
                // Rate limited - apply exponential backoff
                long retryDelayMs = calculateRetryDelay(attempt);
                LOGGER.info(
                    String.format(
                        "Rate limited (429) for %s. Retrying in %d ms (attempt %d/%d)",
                        objPath, retryDelayMs, attempt, MAX_RETRIES));

                // Schedule retry after delay
                CompletableFuture.delayedExecutor(retryDelayMs, TimeUnit.MILLISECONDS)
                    .execute(
                        () -> {
                          requestPresignedUrlWithRetry(fullPath, objPath, attempt + 1)
                              .whenComplete(
                                  (response, ex) -> {
                                    if (ex != null) {
                                      future.completeExceptionally(ex);
                                    } else {
                                      future.complete(response);
                                    }
                                  });
                        });
              } else {
                // Other error status codes
                String errorMsg =
                    String.format(
                        "Failed to get presigned URL for %s: HTTP %d - %s",
                        objPath, result.getCode(), result.getReasonPhrase());
                future.completeExceptionally(
                    new DatabricksVolumeOperationException(
                        errorMsg,
                        null,
                        DatabricksDriverErrorCode.VOLUME_OPERATION_URL_GENERATION_ERROR));
              }
            }

            @Override
            public void failed(Exception ex) {
              presignedUrlSemaphore.release();
              if (attempt < MAX_RETRIES) {
                // Apply exponential backoff for network failures too
                long retryDelayMs = calculateRetryDelay(attempt);
                LOGGER.info(
                    String.format(
                        "Request failed for %s: %s. Retrying in %d ms (attempt %d/%d)",
                        objPath, ex.getMessage(), retryDelayMs, attempt, MAX_RETRIES));

                // Schedule retry after delay
                CompletableFuture.delayedExecutor(retryDelayMs, TimeUnit.MILLISECONDS)
                    .execute(
                        () -> {
                          requestPresignedUrlWithRetry(fullPath, objPath, attempt + 1)
                              .whenComplete(
                                  (response, e) -> {
                                    if (e != null) {
                                      future.completeExceptionally(e);
                                    } else {
                                      future.complete(response);
                                    }
                                  });
                        });
              } else {
                future.completeExceptionally(ex);
              }
            }

            @Override
            public void cancelled() {
              presignedUrlSemaphore.release();
              future.completeExceptionally(
                  new CancellationException(
                      "Request for presigned URL was cancelled for " + objPath));
            }
          });

    } catch (Exception e) {
      // Make sure to release semaphore on unexpected exceptions
      presignedUrlSemaphore.release();
      future.completeExceptionally(e);
    }

    return future;
  }

  // Helper method to calculate retry delay with exponential backoff and jitter
  private long calculateRetryDelay(int attempt) {
    // Calculate exponential backoff: initialDelay * 2^attempt
    long delay = INITIAL_RETRY_DELAY_MS * (1L << attempt);
    // Cap at max delay
    delay = Math.min(delay, MAX_RETRY_DELAY_MS);
    // Add jitter (±20% randomness)
    return (long) (delay * (0.8 + random.nextDouble(0.4)));
  }
}
