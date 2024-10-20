package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.api.impl.volume.DatabricksUCVolumeClient.getObjectFullPath;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.CREATE_UPLOAD_URL_PATH;

import com.databricks.jdbc.api.IDBFSVolumeClient;
import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.filesystem.CreateUploadUrlRequest;
import com.databricks.jdbc.model.client.filesystem.CreateUploadUrlResponse;
import com.databricks.sdk.WorkspaceClient;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class DBFSVolumeClient implements IDBFSVolumeClient {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DBFSVolumeClient.class);
  private final DatabricksConnection connection;

  public DBFSVolumeClient(Connection connection) {
    this.connection = (DatabricksConnection) connection;
  }

  private Map<String, String> getHeaders() {
    return Map.of(
        "Accept", "application/json",
        "Content-Type", "application/json");
  }

  private CreateUploadUrlResponse getCreateUploadUrlResponse(String objectPath)
      throws SQLException {
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
          .POST(CREATE_UPLOAD_URL_PATH, request, CreateUploadUrlResponse.class, getHeaders());
    } catch (Exception e) {
      LOGGER.error(
          String.format("Failed to get create upload url response - {%s}", e.getMessage()));
      throw e;
    }
  }

  public boolean putObject(
      String catalog, String schema, String volume, String objectPath, String localPath)
      throws SQLException {

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
    } catch (Exception e) {
      LOGGER.error(String.format("Failed to put object - {%s}", e.getMessage()));
      throw e;
    }
    return isOperationSucceeded;
  }
}
