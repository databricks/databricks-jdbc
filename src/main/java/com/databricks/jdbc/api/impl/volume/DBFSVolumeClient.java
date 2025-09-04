package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.api.impl.volume.DatabricksUCVolumeClient.getObjectFullPath;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.JSON_HTTP_HEADERS;
import static com.databricks.jdbc.common.util.VolumeUtil.VolumeOperationType.constructListPath;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;

import com.databricks.jdbc.api.IDatabricksVolumeClient;
import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.common.util.StringUtil;
import com.databricks.jdbc.common.util.VolumeRetryUtil;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
  private final boolean enableVolumeOperations;

  /**
   * Initial delay in milliseconds before the first retry attempt. Used as the base value for
   * exponential backoff calculations.
   */
  private static final long INITIAL_RETRY_DELAY_MS = 200;

  /**
   * Maximum delay in milliseconds between retry attempts. Caps the exponential backoff to prevent
   * excessively long delays.
   */
  private static final long MAX_RETRY_DELAY_MS = 10000; // 10 seconds max delay

  /**
   * Semaphore to limit concurrent presigned URL requests to prevent connection pool exhaustion. The
   * limit is configurable via connection context (default: 50 concurrent requests). Each presigned
   * URL request acquires a permit and releases it when the request completes.
   */
  private final Semaphore presignedUrlSemaphore;

  private final ThreadLocalRandom random = ThreadLocalRandom.current();

  // Scheduler for retrying operations in a JDK 8-compatible way
  private static final ScheduledExecutorService RETRY_SCHEDULER =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r, "dbfs-retry");
              t.setDaemon(true);
              return t;
            }
          });

  @VisibleForTesting
  public DBFSVolumeClient(WorkspaceClient workspaceClient) {
    this.connectionContext = null;
    this.workspaceClient = workspaceClient;
    this.apiClient = workspaceClient.apiClient();
    this.databricksHttpClient = null;
    this.allowedVolumeIngestionPaths = "";
    this.enableVolumeOperations = false;
    this.presignedUrlSemaphore = new Semaphore(50);
  }

  public DBFSVolumeClient(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.workspaceClient = getWorkspaceClientFromConnectionContext(connectionContext);
    this.apiClient = workspaceClient.apiClient();
    this.databricksHttpClient =
        DatabricksHttpClientFactory.getInstance()
            .getClient(connectionContext, HttpClientType.VOLUME);
    this.allowedVolumeIngestionPaths = connectionContext.getVolumeOperationAllowedPaths();
    // When enableVolumeOperations is disabled, volume operations on streams are blocked which are
    // accessed directly via a connection URL.
    // Operations performed through  `DBFSVolumeClient` are not restricted by this setting.
    this.enableVolumeOperations = true;
    int maxConcurrentRequests = connectionContext.getMaxDBFSConcurrentPresignedRequests();
    this.presignedUrlSemaphore = new Semaphore(maxConcurrentRequests);
  }

  /** {@inheritDoc} */
  @Override
  public boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {
    LOGGER.debug(
        "Entering prefixExists method with parameters: catalog = {}, schema = {}, volume = {}, prefix = {}, caseSensitive = {}",
        catalog,
        schema,
        volume,
        prefix,
        caseSensitive);
    if (WildcardUtil.isNullOrEmpty(prefix)) {
      return false;
    }
    try {
      List<String> objects = listObjects(catalog, schema, volume, prefix, caseSensitive);
      return !objects.isEmpty();
    } catch (Exception e) {
      LOGGER.error(
          e,
          "Error checking prefix existence: catalog = {}, schema = {}, volume = {}, prefix = {}, caseSensitive = {}",
          catalog,
          schema,
          volume,
          prefix,
          caseSensitive);
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
        "Entering objectExists method with parameters: catalog = {}, schema = {}, volume = {}, objectPath = {}, caseSensitive = {}",
        catalog,
        schema,
        volume,
        objectPath,
        caseSensitive);
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
          e,
          "Error checking object existence: catalog = {}, schema = {}, volume = {}, objectPath = {}, caseSensitive = {}",
          catalog,
          schema,
          volume,
          objectPath,
          caseSensitive);
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
        "Entering volumeExists method with parameters: catalog = {}, schema = {}, volumeName = {}, caseSensitive = {}",
        catalog,
        schema,
        volumeName,
        caseSensitive);
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
          e,
          "Error checking volume existence: catalog = {}, schema = {}, volumeName = {}, caseSensitive = {}",
          catalog,
          schema,
          volumeName,
          caseSensitive);
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
        "Entering listObjects method with parameters: catalog={}, schema={}, volume={}, prefix={}, caseSensitive={}",
        catalog,
        schema,
        volume,
        prefix,
        caseSensitive);

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
        "Entering getObject method with parameters: catalog={}, schema={}, volume={}, objectPath={}, localPath={}",
        catalog,
        schema,
        volume,
        objectPath,
        localPath);

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
        "Entering getObject method with parameters: catalog={}, schema={}, volume={}, objectPath={}",
        catalog,
        schema,
        volume,
        objectPath);

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
              .isEnableVolumeOperations(enableVolumeOperations)
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
        "Entering putObject method with parameters: catalog={}, schema={}, volume={}, objectPath={}, localPath={}",
        catalog,
        schema,
        volume,
        objectPath,
        localPath);

    try {
      // Fetching the Pre Signed Url
      CreateUploadUrlResponse response =
          getCreateUploadUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      // Uploading the object to the Pre Signed Url
      VolumeOperationProcessor volumeOperationProcessor =
          VolumeOperationProcessor.Builder.createBuilder()
              .operationType(VolumeUtil.VolumeOperationType.PUT)
              .isEnableVolumeOperations(enableVolumeOperations)
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
        "Entering putObject method with parameters: catalog={}, schema={}, volume={}, objectPath={}, inputStream={}, contentLength={}, toOverwrite={}",
        catalog,
        schema,
        volume,
        objectPath,
        inputStream,
        contentLength,
        toOverwrite);

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
              .isEnableVolumeOperations(enableVolumeOperations)
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
        "Entering deleteObject method with parameters: catalog={}, schema={}, volume={}, objectPath={}",
        catalog,
        schema,
        volume,
        objectPath);

    try {
      // Fetching the Pre Signed Url
      CreateDeleteUrlResponse response =
          getCreateDeleteUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      // Uploading the object to the Pre Signed Url
      VolumeOperationProcessor volumeOperationProcessor =
          VolumeOperationProcessor.Builder.createBuilder()
              .operationType(VolumeUtil.VolumeOperationType.REMOVE)
              .isEnableVolumeOperations(enableVolumeOperations)
              .operationUrl(response.getUrl())
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
        "Entering getCreateUploadUrlResponse method with parameters: objectPath={}", objectPath);

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
        "Entering getCreateDownloadUrlResponse method with parameters: objectPath={}", objectPath);

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
        "Entering getCreateDeleteUrlResponse method with parameters: objectPath={}", objectPath);
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
    LOGGER.debug("Entering getListResponse method with parameters : listPath={}", listPath);
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

  /** {@inheritDoc} */
  @Override
  public List<VolumePutResult> putFiles(
      String catalog,
      String schema,
      String volume,
      List<String> objectPaths,
      List<String> localPaths,
      boolean toOverwrite) {

    LOGGER.debug(
        "Entering putFiles: catalog={}, schema={}, volume={}, files={}",
        catalog,
        schema,
        volume,
        objectPaths.size());

    if (objectPaths.size() != localPaths.size()) {
      String errorMessage = "objectPaths and localPaths – sizes differ";
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    // Create upload requests - Optional.empty() for errors, Optional.of() for valid requests
    List<Optional<UploadRequest>> uploadRequests = new ArrayList<>(objectPaths.size());

    for (int i = 0; i < objectPaths.size(); i++) {
      final String objPath = objectPaths.get(i);
      final String fullPath = getObjectFullPath(catalog, schema, volume, objPath);
      final String localPath = localPaths.get(i);
      final Path file = Paths.get(localPath);

      if (!Files.exists(file) || !Files.isRegularFile(file)) {
        String errorMessage = "File not found or not a file: " + localPath;
        LOGGER.error(errorMessage);
        // Optional.empty() represents an error case
        uploadRequests.add(Optional.empty());
        continue;
      }

      // Create file upload request
      UploadRequest request = new UploadRequest();
      request.objectPath = objPath;
      request.ucVolumePath = fullPath;
      request.file = file;
      request.originalIndex = i;
      request.errorMessage = null;

      // Optional.of() represents a valid request
      uploadRequests.add(Optional.of(request));
    }

    // Execute uploads in parallel
    return executeUploads(uploadRequests, localPaths);
  }

  /** {@inheritDoc} */
  public List<VolumePutResult> putFiles(
      String catalog,
      String schema,
      String volume,
      List<String> objectPaths,
      List<InputStream> inputStreams,
      List<Long> contentLengths,
      boolean toOverwrite) {

    LOGGER.debug(
        "Entering putFiles: catalog={}, schema={}, volume={}, streams={}",
        catalog,
        schema,
        volume,
        objectPaths.size());

    if (objectPaths.size() != inputStreams.size() || inputStreams.size() != contentLengths.size()) {
      String errorMessage = "objectPaths, inputStreams, contentLengths – sizes differ";
      LOGGER.error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }

    // Create upload requests - Optional.empty() for errors, Optional.of() for valid requests
    List<Optional<UploadRequest>> uploadRequests = new ArrayList<>(objectPaths.size());

    for (int i = 0; i < objectPaths.size(); i++) {
      final String objPath = objectPaths.get(i);
      final String fullPath = getObjectFullPath(catalog, schema, volume, objPath);
      final InputStream inputStream = inputStreams.get(i);
      final long contentLength = contentLengths.get(i);

      // Create stream upload request
      UploadRequest request = new UploadRequest();
      request.objectPath = objPath;
      request.ucVolumePath = fullPath;
      request.inputStream = inputStream;
      request.contentLength = contentLength;
      request.originalIndex = i;
      request.errorMessage = null;

      // All stream requests are valid (no file existence check needed)
      uploadRequests.add(Optional.of(request));
    }

    // Execute uploads in parallel
    return executeUploads(uploadRequests, objectPaths);
  }

  /** Request class that holds all necessary information for either file or stream uploads. */
  public static class UploadRequest {
    /** Relative path within the volume (used for logging and error messages) */
    public String objectPath;

    /** Full UC volume path (e.g., /Volumes/catalog/schema/volume/path/file.txt) */
    public String ucVolumePath;

    public Path file;
    public InputStream inputStream;
    public long contentLength;
    public int originalIndex;
    public String errorMessage;

    public boolean isFile() {
      return file != null;
    }
  }

  /** Common method to execute uploads in parallel. */
  private List<VolumePutResult> executeUploads(
      List<Optional<UploadRequest>> uploadRequests, List<String> originalPaths) {

    // Create futures array to maintain order
    CompletableFuture<VolumePutResult>[] futures = new CompletableFuture[uploadRequests.size()];

    for (int i = 0; i < uploadRequests.size(); i++) {
      final int index = i; // Make effectively final for lambda usage
      Optional<UploadRequest> optionalRequest = uploadRequests.get(index);

      if (!optionalRequest.isPresent()) {
        // Error case: create a failed result immediately
        String errorMessage = "File not found or not a file: " + originalPaths.get(index);
        futures[index] =
            CompletableFuture.completedFuture(
                new VolumePutResult(400, VolumeOperationStatus.FAILED, errorMessage));
        continue;
      }

      // Valid request: process the upload
      UploadRequest request = optionalRequest.get();
      CompletableFuture<VolumePutResult> uploadFuture = new CompletableFuture<VolumePutResult>();
      futures[index] = uploadFuture;

      LOGGER.debug(
          "Uploading {} {}/{}: {} ({} bytes)",
          request.isFile() ? "file" : "stream",
          index + 1,
          uploadRequests.size(),
          request.objectPath,
          request.isFile() ? request.file.toFile().length() : request.contentLength);

      // Get presigned URL and start upload
      requestPresignedUrlWithRetry(request.ucVolumePath, request.objectPath, 1)
          .thenAccept(
              response -> {
                String presignedUrl = response.getUrl();
                LOGGER.debug(
                    "Got presigned URL for {} {}: {}",
                    request.isFile() ? "file" : "stream",
                    index + 1,
                    request.objectPath);

                try {
                  // Upload to presigned URL
                  AsyncRequestProducer uploadProducer;
                  if (request.isFile()) {
                    // File upload
                    uploadProducer =
                        AsyncRequestBuilder.put()
                            .setUri(URI.create(presignedUrl))
                            .setEntity(
                                AsyncEntityProducers.create(
                                    request.file.toFile(), ContentType.DEFAULT_BINARY))
                            .build();
                  } else {
                    // Stream upload
                    AsyncEntityProducer entity =
                        new InputStreamFixedLenProducer(request.inputStream, request.contentLength);
                    uploadProducer =
                        AsyncRequestBuilder.put()
                            .setUri(URI.create(presignedUrl))
                            .setEntity(entity)
                            .build();
                  }

                  AsyncResponseConsumer<SimpleHttpResponse> uploadConsumer =
                      SimpleResponseConsumer.create();

                  // Create callback
                  VolumeUploadCallback uploadCallback =
                      new VolumeUploadCallback(
                          databricksHttpClient,
                          uploadFuture,
                          request,
                          presignedUrlSemaphore,
                          this::requestPresignedUrlWithRetry,
                          this::calculateRetryDelay,
                          connectionContext);

                  databricksHttpClient.executeAsync(uploadProducer, uploadConsumer, uploadCallback);
                } catch (Exception e) {
                  String errorMessage =
                      String.format("Error uploading %s: %s", request.objectPath, e.getMessage());
                  LOGGER.error(e, errorMessage);
                  uploadFuture.complete(
                      new VolumePutResult(500, VolumeOperationStatus.FAILED, errorMessage));
                }
              })
          .exceptionally(
              e -> {
                String errorMessage =
                    String.format(
                        "Failed to get presigned URL for %s: %s",
                        request.objectPath, e.getMessage());
                LOGGER.error(e, errorMessage);
                uploadFuture.complete(
                    new VolumePutResult(500, VolumeOperationStatus.FAILED, errorMessage));
                return null;
              });
    }

    // Wait for all operations to complete
    CompletableFuture.allOf(futures).join();

    // Convert futures to results - order is automatically maintained
    List<VolumePutResult> results = new ArrayList<>(futures.length);
    for (CompletableFuture<VolumePutResult> future : futures) {
      results.add(future.join());
    }

    // Log results
    long successCount =
        results.stream()
            .mapToLong(result -> result.getStatus() == VolumeOperationStatus.SUCCEEDED ? 1 : 0)
            .sum();

    boolean isFileUpload =
        uploadRequests.stream()
            .filter(Optional::isPresent)
            .findFirst()
            .map(opt -> opt.get().isFile())
            .orElse(true);

    LOGGER.info(
        "Completed uploads: {}/{} {} successful",
        successCount,
        results.size(),
        isFileUpload ? "files" : "streams");

    return results;
  }

  // Refactored method with robust semaphore handling and attempt-based retries
  CompletableFuture<CreateUploadUrlResponse> requestPresignedUrlWithRetry(
      String ucVolumePath, String objectPath, int attempt) {
    return requestPresignedUrlWithRetry(
        ucVolumePath, objectPath, attempt, System.currentTimeMillis());
  }

  // Internal method with retry start time tracking and attempt-based retries
  private CompletableFuture<CreateUploadUrlResponse> requestPresignedUrlWithRetry(
      String ucVolumePath, String objectPath, int attempt, long retryStartTime) {
    final CompletableFuture<CreateUploadUrlResponse> future = new CompletableFuture<>();

    try {
      // Acquire permit *before* the try block to handle InterruptedException separately.
      presignedUrlSemaphore.acquire();

      // The whenComplete block acts as a "finally" for the async operation.
      // It guarantees the semaphore is released when the future is done, for any reason.
      future.whenComplete(
          (response, throwable) -> {
            LOGGER.debug("Releasing semaphore permit for {}", objectPath);
            presignedUrlSemaphore.release();
          });

      // All subsequent operations are inside a try-catch to ensure we complete the future.
      try {
        LOGGER.debug("Requesting presigned URL for {} (attempt {})", objectPath, attempt);

        CreateUploadUrlRequest request = new CreateUploadUrlRequest(ucVolumePath);
        String requestBody = apiClient.serialize(request);

        // Build async request
        AsyncRequestBuilder requestBuilder =
            AsyncRequestBuilder.post(
                URI.create(connectionContext.getHostUrl() + CREATE_UPLOAD_URL_PATH));

        // Add headers
        Map<String, String> authHeaders = workspaceClient.config().authenticate();
        authHeaders.forEach(requestBuilder::addHeader);
        JSON_HTTP_HEADERS.forEach(requestBuilder::addHeader);

        requestBuilder.setEntity(
            AsyncEntityProducers.create(requestBody.getBytes(), ContentType.APPLICATION_JSON));

        // Execute async request
        databricksHttpClient.executeAsync(
            requestBuilder.build(),
            SimpleResponseConsumer.create(),
            new FutureCallback<SimpleHttpResponse>() {
              @Override
              public void completed(SimpleHttpResponse result) {
                if (result.getCode() >= 200 && result.getCode() < 300) {
                  try {
                    CreateUploadUrlResponse response =
                        JsonUtil.getMapper()
                            .readValue(result.getBodyText(), CreateUploadUrlResponse.class);
                    future.complete(response);
                  } catch (Exception e) {
                    future.completeExceptionally(e);
                  }
                } else if (VolumeRetryUtil.isRetryableHttpCode(result.getCode(), connectionContext)
                    && VolumeRetryUtil.shouldRetry(attempt, retryStartTime, connectionContext)) {
                  handleRetry(ucVolumePath, objectPath, attempt, future, retryStartTime);
                } else {
                  String errorMsg =
                      String.format(
                          "Failed to get presigned URL for %s: HTTP %d - %s",
                          objectPath, result.getCode(), result.getReasonPhrase());
                  LOGGER.error(errorMsg);
                  future.completeExceptionally(
                      new DatabricksVolumeOperationException(
                          errorMsg,
                          null,
                          DatabricksDriverErrorCode.VOLUME_OPERATION_URL_GENERATION_ERROR));
                }
              }

              @Override
              public void failed(Exception ex) {
                if (VolumeRetryUtil.shouldRetry(attempt, retryStartTime, connectionContext)) {
                  handleRetry(ucVolumePath, objectPath, attempt, future, retryStartTime);
                } else {
                  LOGGER.error(
                      ex, "Failed to get presigned URL for {} (attempt {})", objectPath, attempt);
                  future.completeExceptionally(ex);
                }
              }

              @Override
              public void cancelled() {
                future.cancel(true);
              }
            });

      } catch (Throwable t) {
        // If any synchronous error occurs (e.g., serialization, auth), fail the future.
        // The whenComplete block will then trigger the semaphore release.
        future.completeExceptionally(t);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      future.completeExceptionally(
          new CancellationException("Thread was interrupted while waiting for semaphore permit."));
    }

    return future;
  }

  // Helper method for retry logic to avoid code duplication
  private void handleRetry(
      String ucVolumePath,
      String objectPath,
      int attempt,
      CompletableFuture<CreateUploadUrlResponse> future,
      long retryStartTime) {
    long retryDelayMs = calculateRetryDelay(attempt);
    long elapsedSeconds = (System.currentTimeMillis() - retryStartTime) / 1000;
    int timeoutSeconds = VolumeRetryUtil.getRetryTimeoutSeconds(connectionContext);
    LOGGER.info(
        "Request for {} failed or was rate-limited. Retrying in {} ms (elapsed: {}s, timeout: {}s)",
        objectPath,
        retryDelayMs,
        elapsedSeconds,
        timeoutSeconds);

    RETRY_SCHEDULER.schedule(
        () -> {
          // The retry will return a new future; we pipe its result into our original future.
          requestPresignedUrlWithRetry(ucVolumePath, objectPath, attempt + 1, retryStartTime)
              .whenComplete(
                  (response, ex) -> {
                    if (ex != null) {
                      LOGGER.error(
                          ex,
                          "Failed to get presigned URL for {} (attempt {})",
                          objectPath,
                          attempt + 1);
                      future.completeExceptionally(ex);
                    } else {
                      future.complete(response);
                    }
                  });
        },
        retryDelayMs,
        TimeUnit.MILLISECONDS);
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
