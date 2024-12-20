package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.api.impl.volume.DatabricksUCVolumeClient.getObjectFullPath;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.JSON_HTTP_HEADERS;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksVolumeClient;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.filesystem.*;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.google.common.annotations.VisibleForTesting;
import java.io.InputStream;
import java.util.List;
import org.apache.http.entity.InputStreamEntity;

/** Implementation of Volume Client that directly calls SQL Exec API for the Volume Operations */
public class DBFSVolumeClient implements IDatabricksVolumeClient {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DBFSVolumeClient.class);
  private final IDatabricksConnectionContext connectionContext;
  @VisibleForTesting final WorkspaceClient workspaceClient;

  @VisibleForTesting
  public DBFSVolumeClient(WorkspaceClient workspaceClient) {
    this.connectionContext = null;
    this.workspaceClient = workspaceClient;
  }

  public DBFSVolumeClient(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.workspaceClient = getWorkspaceClientFromConnectionContext(connectionContext);
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
      throws UnsupportedOperationException {
    String errorMessage = "listObjects function is unsupported in DBFSVolumeClient";
    LOGGER.error(errorMessage);
    throw new UnsupportedOperationException(errorMessage);
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

    boolean isOperationSucceeded = false;

    try {
      // Fetching the Pre signed URL
      CreateDownloadUrlResponse response =
          getCreateDownloadUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      // Downloading the object from the pre signed Url
      VolumeOperationProcessorDirect volumeOperationProcessorDirect =
          getVolumeOperationProcessorDirect(response.getUrl(), localPath);
      volumeOperationProcessorDirect.executeGetOperation();
      isOperationSucceeded = true;
    } catch (DatabricksVolumeOperationException e) {
      String errorMessage = String.format("Failed to get object - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw e;
    }
    return isOperationSucceeded;
  }

  /** {@inheritDoc} */
  @Override
  public InputStreamEntity getObject(
      String catalog, String schema, String volume, String objectPath)
      throws UnsupportedOperationException {
    String errorMessage =
        "getObject returning InputStreamEntity function is unsupported in DBFSVolumeClient";
    LOGGER.error(errorMessage);
    throw new UnsupportedOperationException(errorMessage);
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
    boolean isOperationSucceeded = false;
    try {
      // Fetching the Pre Signed Url
      CreateUploadUrlResponse response =
          getCreateUploadUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      // Uploading the object to the Pre Signed Url
      VolumeOperationProcessorDirect volumeOperationProcessorDirect =
          getVolumeOperationProcessorDirect(response.getUrl(), localPath);
      volumeOperationProcessorDirect.executePutOperation();
      isOperationSucceeded = true;
    } catch (DatabricksVolumeOperationException e) {
      String errorMessage = String.format("Failed to put object - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw e;
    }
    return isOperationSucceeded;
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
      throws UnsupportedOperationException {
    String errorMessage = "putObject for InputStream function is unsupported in DBFSVolumeClient";
    LOGGER.error(errorMessage);
    throw new UnsupportedOperationException(errorMessage);
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteObject(String catalog, String schema, String volume, String objectPath)
      throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format(
            "Entering deleteObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}",
            catalog, schema, volume, objectPath));

    boolean isOperationSucceeded = false;
    try {
      // Fetching the Pre Signed Url
      CreateDeleteUrlResponse response =
          getCreateDeleteUrlResponse(getObjectFullPath(catalog, schema, volume, objectPath));

      // Uploading the object to the Pre Signed Url
      VolumeOperationProcessorDirect volumeOperationProcessorDirect =
          getVolumeOperationProcessorDirect(response.getUrl(), null);
      volumeOperationProcessorDirect.executeDeleteOperation();
      isOperationSucceeded = true;
    } catch (DatabricksVolumeOperationException e) {
      String errorMessage = String.format("Failed to delete object - {%s}", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw e;
    }
    return isOperationSucceeded;
  }

  WorkspaceClient getWorkspaceClientFromConnectionContext(
      IDatabricksConnectionContext connectionContext) {
    return new ClientConfigurator(connectionContext).getWorkspaceClient();
  }

  VolumeOperationProcessorDirect getVolumeOperationProcessorDirect(
      String operationUrl, String localFilePath) {
    return new VolumeOperationProcessorDirect(operationUrl, localFilePath, connectionContext);
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
}
