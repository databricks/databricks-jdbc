package com.databricks.jdbc.client.impl.sdk;

import com.databricks.jdbc.client.IDatabricksUCVolumeClient;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation for DatabricksUCVolumeClient using SDK client + SQL Exec API */
public class DatabricksUCVolumeClient implements IDatabricksUCVolumeClient {

  private final Connection connection;

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSdkClient.class);

  public DatabricksUCVolumeClient(Connection connection) {
    this.connection = connection;
  }

  @Override
  public boolean prefixExists(
      String catalog, String schema, String volume, String prefix, Statement statement)
      throws SQLException {

    String listFilesSQL = "LIST '/Volumes/" + catalog + "/" + schema + "/" + volume + "/'";

    ResultSet resultSet = statement.executeQuery(listFilesSQL);

    boolean exists = false;
    while (resultSet.next()) {
      String fileName = resultSet.getString("name");
      if (fileName.startsWith(prefix)) {
        exists = true;
        break;
      }
    }
    return exists;
  }
}
