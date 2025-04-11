package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.api.impl.volume.DatabricksUCVolumeClient.getObjectFullPath;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.JSON_HTTP_HEADERS;
import static com.databricks.jdbc.common.util.VolumeUtil.VolumeOperationType.constructListPath;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;

import com.databricks.jdbc.api.IDatabricksVolumeClient;
import com.databricks.jdbc.api.impl.VolumeOperationStatus;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.StringUtil;
import com.databricks.jdbc.common.util.VolumeUtil;
import com.databricks.jdbc.common.util.WildcardUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
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
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
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
      // If getListResponse does not throw, then the volume exists (even if it’s empty).
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
    ClientConfigurator clientConfigurator = new ClientConfigurator(connectionContext);
    DatabricksThreadContextHolder.setDatabricksConfig(clientConfigurator.getDatabricksConfig());
    return clientConfigurator.getWorkspaceClient();
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
}
