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

  /**
   * Upload multiple files to DBFS volume in parallel using the async HTTP client. Each upload
   * consists of two dependent stages (fetch presigned URL then PUT upload). The stages are chained
   * with {@code CompletableFuture#thenCompose} so that the PUT executes only after the presigned
   * URL arrives, while all files progress concurrently.
   *
   * @return list of {@link VolumePutResult} in the same order as the input lists.
   */
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
            "Entering putFiles method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPaths.size={%d}, inputStreams.size={%d}, contentLengths.size={%d}, overwrite={%s}",
            catalog,
            schema,
            volume,
            objectPaths.size(),
            inputStreams.size(),
            contentLengths.size(),
            overwrite));

    if (objectPaths.size() != inputStreams.size() || inputStreams.size() != contentLengths.size()) {
      String errorMessage = "objectPaths, inputStreams, contentLengths – sizes differ";
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    ExecutorService pool = Executors.newFixedThreadPool(Math.min(16, objectPaths.size()));
    List<CompletableFuture<VolumePutResult>> chains = new ArrayList<>(objectPaths.size());

    for (int i = 0; i < objectPaths.size(); i++) {
      final String objPath = objectPaths.get(i);
      final String fullPath = getObjectFullPath(catalog, schema, volume, objPath);
      final InputStream in = inputStreams.get(i);
      final long len = contentLengths.get(i);

      LOGGER.debug(
          String.format(
              "Processing stream %d/%d: objPath={%s}, size={%d} bytes",
              i + 1, objectPaths.size(), objPath, len));

      CompletableFuture<VolumePutResult> chain =
          requestUploadUrlAsync(fullPath, pool)
              .thenCompose(
                  resp -> {
                    LOGGER.debug(String.format("Got presigned URL for %s", objPath));
                    return uploadAsync(resp.getUrl(), in, len);
                  })
              .exceptionally(
                  ex -> {
                    LOGGER.error(
                        String.format("Failed to upload %s: %s", objPath, ex.getMessage()), ex);

                    return failureResult(ex);
                  });
      chains.add(chain);
    }

    CompletableFuture.allOf(chains.toArray(new CompletableFuture[0])).join();
    List<VolumePutResult> streamResults =
        chains.stream().map(CompletableFuture::join).collect(Collectors.toList());

    long streamSuccessCount =
        streamResults.stream()
            .filter(r -> r.getStatus() == VolumeOperationStatus.SUCCEEDED)
            .count();

    LOGGER.debug(
        String.format(
            "Completed putFiles: %d/%d streams successfully uploaded",
            streamSuccessCount, objectPaths.size()));

    return streamResults;
  }

  /** Upload a file to a presigned URL using the shared async HTTP client. */
  private CompletableFuture<VolumePutResult> uploadFileAsync(
      String presignedUrl, java.io.File file) {
    LOGGER.debug(
        String.format(
            "Starting uploadFileAsync to URL: %s, file: %s (size: %d bytes)",
            presignedUrl, file.getPath(), file.length()));

    CompletableFuture<VolumePutResult> cf = new CompletableFuture<>();
    try {
      // Build PUT request with file entity
      AsyncRequestProducer producer =
          AsyncRequestBuilder.put()
              .setUri(URI.create(presignedUrl))
              .setEntity(AsyncEntityProducers.create(file, ContentType.DEFAULT_BINARY))
              .build();
      AsyncResponseConsumer<SimpleHttpResponse> consumer = SimpleResponseConsumer.create();

      // Execute and handle the response
      executeUploadRequest(presignedUrl, producer, consumer, cf);
    } catch (Exception e) {
      LOGGER.error(String.format("Failed in uploadFileAsync: %s", e.getMessage()), e);
      cf.completeExceptionally(e);
    }
    return cf;
  }

  /** Upload from an input stream to a presigned URL using the shared async HTTP client. */
  private CompletableFuture<VolumePutResult> uploadAsync(
      String presignedUrl, InputStream in, long len) {
    LOGGER.debug(
        String.format(
            "Starting uploadAsync to URL: %s, input stream size: %d bytes", presignedUrl, len));

    CompletableFuture<VolumePutResult> cf = new CompletableFuture<>();
    try {
      AsyncEntityProducer entity =
          new InputStreamFixedLenProducer(in, len, ContentType.APPLICATION_OCTET_STREAM);
      AsyncRequestProducer producer =
          AsyncRequestBuilder.put().setUri(URI.create(presignedUrl)).setEntity(entity).build();
      AsyncResponseConsumer<SimpleHttpResponse> consumer = SimpleResponseConsumer.create();

      // Execute and handle the response
      executeUploadRequest(presignedUrl, producer, consumer, cf);
    } catch (Exception e) {
      LOGGER.error(String.format("Failed in uploadAsync: %s", e.getMessage()), e);
      cf.completeExceptionally(e);
    }
    return cf;
  }

  //  private String getErrorCodeFromException(Throwable ex) {
  //    try {
  //      if (ex instanceof CompletionException && ex.getCause() != null) {
  //        return getErrorCodeFromException(ex.getCause());
  //      }
  //
  //      if (ex instanceof DatabricksVolumeOperationException) {
  //        // Try to extract error code from message
  //        String message = ex.toString();
  //        if (message.contains("ErrorCode=")) {
  //          int startIndex = message.indexOf("ErrorCode=") + 10;
  //          int endIndex = message.indexOf(']', startIndex);
  //          if (endIndex > startIndex) {
  //            return message.substring(startIndex, endIndex);
  //          }
  //        }
  //        // If can't extract, use default for volume operations
  //        return DatabricksDriverErrorCode.VOLUME_OPERATION_EXCEPTION.name();
  //      }
  //    } catch (Exception e) {
  //      // If any error in parsing, fall back to a safe default
  //      LOGGER.warn("Error extracting error code from exception", e);
  //    }
  //
  //    // Default error code based on operation type
  //    return DatabricksDriverErrorCode.VOLUME_OPERATION_PUT_OPERATION_EXCEPTION.name();
  //  }

  private VolumePutResult failureResult(Throwable ex) {
    //    String errorCodeName;
    //
    //    if (ex instanceof CompletionException
    //        && ex.getCause() instanceof DatabricksVolumeOperationException) {
    //      // This is a URL generation error
    //      errorCodeName = DatabricksDriverErrorCode.VOLUME_OPERATION_URL_GENERATION_ERROR.name();
    //    } else {
    //      errorCodeName = getErrorCodeFromException(ex);
    //    }
    //
    //    if (connectionContext != null) {
    //      exportFailureLog(connectionContext, errorCodeName, ex.getMessage());
    //    }

    return new VolumePutResult(500, VolumeOperationStatus.FAILED, ex.getMessage());
  }

  /**
   * Upload multiple files from local paths to DBFS volume in parallel.
   *
   * @param catalog The catalog name
   * @param schema The schema name
   * @param volume The volume name
   * @param objectPaths List of object paths in the volume
   * @param localPaths List of local file paths to upload
   * @param overwrite Whether to overwrite existing files
   * @return list of {@link VolumePutResult} in the same order as the input lists
   * @throws DatabricksVolumeOperationException if the operation fails
   */
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
            "Entering putFiles method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPaths.size={%d}, localPaths.size={%d}, overwrite={%s}",
            catalog, schema, volume, objectPaths.size(), localPaths.size(), overwrite));

    if (objectPaths.size() != localPaths.size()) {
      String errorMessage = "objectPaths and localPaths – sizes differ";
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    //    ExecutorService pool = Executors.newFixedThreadPool(Math.min(16, objectPaths.size()));
    int PARALLELISM = Math.max(4, Runtime.getRuntime().availableProcessors() * 2);
    //    ExecutorService IO_POOL = Executors.newFixedThreadPool(PARALLELISM);
    // one pool for presign (cheap I/O)
    ExecutorService PRESIGN_POOL =
        Executors.newFixedThreadPool(Math.max(32, Runtime.getRuntime().availableProcessors()));

    // one pool for uploads (more threads – blocking I/O)
    ExecutorService UPLOAD_POOL =
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);

    List<CompletableFuture<VolumePutResult>> chains = new ArrayList<>(objectPaths.size());

    for (int i = 0; i < objectPaths.size(); i++) {
      final String objPath = objectPaths.get(i);
      final String fullPath = getObjectFullPath(catalog, schema, volume, objPath);
      final String localPath = localPaths.get(i);
      final java.io.File file = new java.io.File(localPath);

      LOGGER.debug(
          String.format(
              "Processing file %d/%d: objPath={%s}, localPath={%s}",
              i + 1, objectPaths.size(), objPath, localPath));

      if (!file.exists() || !file.isFile()) {
        String errorMessage = "File not found or not a file: " + localPath;
        LOGGER.error(errorMessage);
        exportFailureLog(
            connectionContext,
            DatabricksDriverErrorCode.VOLUME_OPERATION_LOCAL_FILE_EXISTS_ERROR.name(),
            errorMessage);
        chains.add(
            CompletableFuture.completedFuture(
                new VolumePutResult(404, VolumeOperationStatus.FAILED, errorMessage)));
        continue;
      }

      LOGGER.debug(String.format("Uploading file: %s (size: %d bytes)", localPath, file.length()));
      CompletableFuture<VolumePutResult> chain =
          requestUploadUrlAsync(fullPath, PRESIGN_POOL)
              .thenComposeAsync( // hop to *same* pool
                  //                      LOGGER.info("Got presigned URL for " + objPath);
                  resp -> uploadFileAsync(resp.getUrl(), file),
                  UPLOAD_POOL)
              .exceptionally(
                  ex -> {
                    LOGGER.error(
                        String.format("Failed to upload %s: %s", objPath, ex.getMessage()), ex);
                    return failureResult(ex);
                  });
      chains.add(chain);
    }

    CompletableFuture.allOf(chains.toArray(new CompletableFuture[0])).join();
    List<VolumePutResult> results =
        chains.stream().map(CompletableFuture::join).collect(Collectors.toList());
    //    pool.shutdown();
    //    IO_POOL.shutdown();
    PRESIGN_POOL.shutdown();
    UPLOAD_POOL.shutdown();

    long successCount =
        results.stream().filter(r -> r.getStatus() == VolumeOperationStatus.SUCCEEDED).count();

    LOGGER.debug(
        String.format(
            "Completed putFiles: %d/%d files successfully uploaded",
            successCount, objectPaths.size()));

    return results;
  }

  /** Asynchronously request a presigned upload URL. */
  private CompletableFuture<CreateUploadUrlResponse> requestUploadUrlAsync(
      String fullPath, Executor exec) {
    LOGGER.debug(String.format("Requesting upload URL for path: %s", fullPath));
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            CreateUploadUrlResponse response = getCreateUploadUrlResponse(fullPath);
            LOGGER.debug(String.format("Received upload URL for path: %s", fullPath));
            return response;
          } catch (DatabricksVolumeOperationException e) {
            LOGGER.error(
                String.format(
                    "Failed to get upload URL for path: %s - %s", fullPath, e.getMessage()),
                e);
            if (connectionContext != null) {
              exportFailureLog(
                  connectionContext,
                  DatabricksDriverErrorCode.VOLUME_OPERATION_URL_GENERATION_ERROR.name(),
                  "Failed to get upload URL: " + e.getMessage());
            }
            throw new CompletionException(e);
          }
        },
        exec);
  }

  /** Common helper method to handle upload requests using the shared async HTTP client. */
  private void executeUploadRequest(
      String presignedUrl,
      AsyncRequestProducer producer,
      AsyncResponseConsumer<SimpleHttpResponse> consumer,
      CompletableFuture<VolumePutResult> cf) {
    LOGGER.debug(String.format("Executing upload request to URL: %s", presignedUrl));
    databricksHttpClient.executeAsync(
        producer,
        consumer,
        new FutureCallback<>() {
          @Override
          public void completed(SimpleHttpResponse result) {
            VolumeOperationStatus st =
                (result.getCode() >= 200 && result.getCode() < 300)
                    ? VolumeOperationStatus.SUCCEEDED
                    : VolumeOperationStatus.FAILED;
            String message =
                st == VolumeOperationStatus.SUCCEEDED ? null : result.getReasonPhrase();
            LOGGER.debug(
                String.format(
                    "Upload request completed with status: %s, code: %d, message: %s",
                    st, result.getCode(), message != null ? message : "success"));

            // Only log failures to telemetry
            if (st == VolumeOperationStatus.FAILED) {
              exportFailureLog(
                  connectionContext,
                  DatabricksDriverErrorCode.VOLUME_OPERATION_PUT_OPERATION_EXCEPTION.name(),
                  "Upload failed with HTTP status " + result.getCode() + ": " + message);
            }

            cf.complete(new VolumePutResult(result.getCode(), st, message));
          }

          @Override
          public void failed(Exception ex) {
            LOGGER.error(String.format("Upload request failed: %s", ex.getMessage()), ex);
            exportFailureLog(
                connectionContext,
                DatabricksDriverErrorCode.VOLUME_OPERATION_PUT_OPERATION_EXCEPTION.name(),
                "Upload request failed: " + ex.getMessage());
            cf.complete(new VolumePutResult(500, VolumeOperationStatus.FAILED, ex.getMessage()));
          }

          @Override
          public void cancelled() {
            LOGGER.warn(String.format("Upload request cancelled for URL: %s", presignedUrl));
            exportFailureLog(
                connectionContext,
                DatabricksDriverErrorCode.VOLUME_OPERATION_EXCEPTION.name(),
                "Upload request cancelled");
            cf.complete(new VolumePutResult(499, VolumeOperationStatus.ABORTED, "cancelled"));
          }
        });
  }
}
