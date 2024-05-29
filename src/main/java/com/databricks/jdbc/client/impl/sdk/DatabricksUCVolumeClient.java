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

  private String createListQuery(String catalog, String schema, String volume) {
    return String.format("LIST '/Volumes/%s/%s/%s/'", catalog, schema, volume);
  }

  @Override
  public boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {

    LOGGER.info(
        "Entering prefixExists method with parameters: catalog={}, schema={}, volume={}, prefix={}, caseSensitive={}",
        catalog,
        schema,
        volume,
        prefix,
        caseSensitive);

    String listFilesSQLQuery = createListQuery(catalog, schema, volume);

    try (Statement statement = connection.createStatement()) {
      LOGGER.info("Executing SQL query: {}", listFilesSQLQuery);
      ResultSet resultSet = statement.executeQuery(listFilesSQLQuery);
      LOGGER.info("SQL query executed successfully");

      boolean exists = false;
      while (resultSet.next()) {
        String fileName = resultSet.getString("name");
        if (caseSensitive) {
          if (fileName.startsWith(prefix)) {
            exists = true;
            break;
          }
        } else {
          if (fileName.toLowerCase().startsWith(prefix.toLowerCase())) {
            exists = true;
            break;
          }
        }
      }
      return exists;
    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed", e);
      throw e;
    }
  }
}
