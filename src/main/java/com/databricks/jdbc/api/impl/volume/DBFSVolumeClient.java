package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.api.impl.volume.DatabricksUCVolumeClient.getObjectFullPath;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.JSON_HTTP_HEADERS;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksVolumeClient;
import com.databricks.jdbc.common.util.DatabricksConnectionContextHolder;
import com.databricks.jdbc.common.util.StringUtil;
import com.databricks.jdbc.common.util.VolumeUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.filesystem.*;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
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
  private final String allowedVolumeIngestionPaths;

  @VisibleForTesting
  public DBFSVolumeClient(WorkspaceClient workspaceClient) {
    this.connectionContext = null;
    this.workspaceClient = workspaceClient;
    this.databricksHttpClient = null;
    this.allowedVolumeIngestionPaths = "";
  }

  public DBFSVolumeClient(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.workspaceClient = getWorkspaceClientFromConnectionContext(connectionContext);
    this.databricksHttpClient =
        DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
    this.allowedVolumeIngestionPaths = connectionContext.getVolumeOperationAllowedPaths();
  }

  /** {@inheritDoc} */
  @Override
  public boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws UnsupportedOperationException {
    String errorMessage = "prefixExists function is unsupported in DBFSVolumeClient";
    LOGGER.error(errorMessage);
    throw new UnsupportedOperationException(errorMessage);
  }

  /** {@inheritDoc} */
  @Override
  public boolean objectExists(
      String catalog, String schema, String volume, String objectPath, boolean caseSensitive)
      throws UnsupportedOperationException {
    String errorMessage = "objectExists function is unsupported in DBFSVolumeClient";
    LOGGER.error(errorMessage);
    throw new UnsupportedOperationException(errorMessage);
  }

  /** {@inheritDoc} */
  @Override
  public boolean volumeExists(
      String catalog, String schema, String volumeName, boolean caseSensitive)
      throws UnsupportedOperationException {
    String errorMessage = "volumeExists function is unsupported in DBFSVolumeClient";
    LOGGER.error(errorMessage);
    throw new UnsupportedOperationException(errorMessage);
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

    String folder = StringUtil.getFolderNameFromPath(prefix);
    String basename = StringUtil.getBaseNameFromPath(prefix);

    String listPath =
        (folder.isEmpty())
            ? StringUtil.getVolumePath(catalog, schema, volume)
            : StringUtil.getVolumePath(catalog, schema, volume + "/" + folder);

    ListResponse listResponse = getListResponse(listPath);

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

      // TODO : allowedVolumeIngestionPathString is currently set to only /tmp foler.
      //        Make it configurable

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
      throw new DatabricksVolumeOperationException(errorMessage, e);
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
      throw new DatabricksVolumeOperationException(errorMessage, e);
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

      // TODO : allowedVolumeIngestionPathString is currently set to only /tmp foler.
      //        Make it configurable

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
      throw new DatabricksVolumeOperationException(errorMessage, e);
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
      throw new DatabricksVolumeOperationException(errorMessage, e);
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
      throw new DatabricksVolumeOperationException(errorMessage, e);
    }
    return true;
  }

  WorkspaceClient getWorkspaceClientFromConnectionContext(
      IDatabricksConnectionContext connectionContext) {
    return new ClientConfigurator(connectionContext).getWorkspaceClient();
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
      return workspaceClient
          .apiClient()
          .POST(CREATE_UPLOAD_URL_PATH, request, CreateUploadUrlResponse.class, JSON_HTTP_HEADERS);
    } catch (DatabricksException e) {
      String errorMessage =
          String.format("Failed to get create upload url response - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(errorMessage, e);
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
      return workspaceClient
          .apiClient()
          .POST(
              CREATE_DOWNLOAD_URL_PATH,
              request,
              CreateDownloadUrlResponse.class,
              JSON_HTTP_HEADERS);
    } catch (DatabricksException e) {
      String errorMessage =
          String.format("Failed to get create download url response - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(errorMessage, e);
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
      return workspaceClient
          .apiClient()
          .POST(CREATE_DELETE_URL_PATH, request, CreateDeleteUrlResponse.class, JSON_HTTP_HEADERS);
    } catch (DatabricksException e) {
      String errorMessage =
          String.format("Failed to get create delete url response - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(errorMessage, e);
    }
  }

  /** Fetches the list of objects in the volume using the SQL Exec API */
  ListResponse getListResponse(String listPath) throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format("Entering getListResponse method with parameters : listPath={%s}", listPath));
    ListRequest request = new ListRequest(listPath);
    try {
      return workspaceClient
          .apiClient()
          .GET(LIST_PATH, request, ListResponse.class, JSON_HTTP_HEADERS);
    } catch (DatabricksException e) {
      String errorMessage = String.format("Failed to get list response - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksVolumeOperationException(errorMessage, e);
    }
  }

  private void checkVolumeOperationError(VolumeOperationProcessor volumeOperationProcessor)
      throws DatabricksSQLException {
    if (volumeOperationProcessor.getStatus() == VolumeUtil.VolumeOperationStatus.FAILED) {
      throw new DatabricksSQLException(
          "Volume operation failed: " + volumeOperationProcessor.getErrorMessage());
    }
    if (volumeOperationProcessor.getStatus() == VolumeUtil.VolumeOperationStatus.ABORTED) {
      throw new DatabricksSQLException(
          "Volume operation aborted: " + volumeOperationProcessor.getErrorMessage());
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
    DatabricksConnectionContextHolder.clear();
  }
}
