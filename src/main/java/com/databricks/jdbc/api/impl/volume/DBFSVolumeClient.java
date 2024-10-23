package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.api.impl.volume.DatabricksUCVolumeClient.getObjectFullPath;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.CREATE_UPLOAD_URL_PATH;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksUCVolumeClient;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.common.util.ClientUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksVolumeOperationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.filesystem.CreateUploadUrlRequest;
import com.databricks.jdbc.model.client.filesystem.CreateUploadUrlResponse;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.http.entity.InputStreamEntity;

public class DBFSVolumeClient implements IDatabricksUCVolumeClient {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DBFSVolumeClient.class);
  private final DatabricksConnection connection;

  public DBFSVolumeClient(Connection connection) {
    this.connection = (DatabricksConnection) connection;
  }

  private CreateUploadUrlResponse getCreateUploadUrlResponse(String objectPath)
      throws DatabricksVolumeOperationException {
    LOGGER.debug(
        String.format(
            "Entering getCreateUploadUrlResponse method with parameters: objectPath={%s}",
            objectPath));
    IDatabricksClient client = connection.getSession().getDatabricksClient();
    IDatabricksConnectionContext connectionContext = client.getConnectionContext();
    WorkspaceClient workspaceClient =
        new ClientConfigurator(connectionContext).getWorkspaceClient();

    CreateUploadUrlRequest request = new CreateUploadUrlRequest(objectPath);
    try {
      return workspaceClient
          .apiClient()
          .POST(
              CREATE_UPLOAD_URL_PATH,
              request,
              CreateUploadUrlResponse.class,
              ClientUtil.getHeaders());
    } catch (DatabricksException e) {
      String errorMessage =
          String.format("Failed to get create upload url response - {%s}", e.getMessage());
      LOGGER.error(errorMessage);
      throw new DatabricksVolumeOperationException(errorMessage, e);
    }
  }

  public boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {
    String errorMessage = "prefixExists function is unsupported in DBFSVolumeClient";
    throw new SQLException(errorMessage);
  }

  public boolean objectExists(
      String catalog, String schema, String volume, String objectPath, boolean caseSensitive)
      throws SQLException {
    String errorMessage = "objectExists function is unsupported in DBFSVolumeClient";
    throw new SQLException(errorMessage);
  }

  public boolean volumeExists(
      String catalog, String schema, String volumeName, boolean caseSensitive) throws SQLException {
    String errorMessage = "volumeExists function is unsupported in DBFSVolumeClient";
    throw new SQLException(errorMessage);
  }

  public List<String> listObjects(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {
    String errorMessage = "listObjects function is unsupported in DBFSVolumeClient";
    throw new SQLException(errorMessage);
  }

  public boolean getObject(
      String catalog, String schema, String volume, String objectPath, String localPath)
      throws SQLException {
    String errorMessage = "getObject returning boolean function is unsupported in DBFSVolumeClient";
    throw new SQLException(errorMessage);
  }

  public InputStreamEntity getObject(
      String catalog, String schema, String volume, String objectPath) throws SQLException {
    String errorMessage =
        "getObject returning InputStreamEntity function is unsupported in DBFSVolumeClient";
    throw new SQLException(errorMessage);
  }

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
          new VolumeOperationProcessorDirect(
              response.getUrl(), localPath, response.getHeaders(), connection.getSession());
      volumeOperationProcessorDirect.executePutOperation();
      isOperationSucceeded = true;
    } catch (DatabricksVolumeOperationException e) {
      String errorMessage = String.format("Failed to put object - {%s}", e.getMessage());
      LOGGER.error(errorMessage);
      throw e;
    }
    return isOperationSucceeded;
  }

  public boolean putObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      InputStream inputStream,
      long contentLength,
      boolean toOverwrite)
      throws SQLException {
    String errorMessage = "putObject for InputStream function is unsupported in DBFSVolumeClient";
    throw new SQLException(errorMessage);
  }

  public boolean deleteObject(String catalog, String schema, String volume, String objectPath)
      throws SQLException {
    String errorMessage = "deleteObject function is unsupported in DBFSVolumeClient";
    throw new SQLException(errorMessage);
  }
}
