package com.databricks.jdbc.client.impl.sdk;

import com.databricks.jdbc.client.IDatabricksUCVolumeClient;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation for DatabricksUCVolumeClient */
public class DatabricksUCVolumeClient implements IDatabricksUCVolumeClient {

  private final Connection connection;

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSdkClient.class);

  public DatabricksUCVolumeClient(Connection connection) {
    this.connection = connection;
  }

  private String createListQuery(String catalog, String schema, String volume) {
    return String.format("LIST '/Volumes/%s/%s/%s/'", catalog, schema, volume);
  }

  public boolean prefixExists(String catalog, String schema, String volume, String prefix)
      throws SQLException {
    return prefixExists(catalog, schema, volume, prefix, true);
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
      ResultSet resultSet = statement.executeQuery(listFilesSQLQuery);
      LOGGER.info("SQL query executed successfully");

      boolean exists = false;
      while (resultSet.next()) {
        String fileName = resultSet.getString("name");
        if (fileName.regionMatches(
            /* ignoreCase= */ !caseSensitive,
            /* targetOffset= */ 0,
            /* StringToCheck= */ prefix,
            /* sourceOffset= */ 0,
            /* lengthToMatch= */ prefix.length())) {
          exists = true;
          break;
        }
      }
      return exists;
    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed", e);
      throw e;
    }
  }

  @Override
  public boolean objectExists(
      String catalog, String schema, String volume, String objectName, boolean caseSensitive)
      throws SQLException {

    LOGGER.info(
        "Entering objectExists method with parameters: catalog={}, schema={}, volume={}, objectName={}, caseSensitive={}",
        catalog,
        schema,
        volume,
        objectName,
        caseSensitive);

    String listFilesSQLQuery = createListQuery(catalog, schema, volume);

    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(listFilesSQLQuery);
      LOGGER.info("SQL query executed successfully");

      boolean exists = false;
      while (resultSet.next()) {
        String fileName = resultSet.getString("name");
        if (fileName.regionMatches(
            /* ignoreCase= */ !caseSensitive,
            /* targetOffset= */ 0,
            /* StringToCheck= */ objectName,
            /* sourceOffset= */ 0,
            /* lengthToMatch= */ objectName.length())) {
          exists = true;
          break;
        }
      }
      return exists;
    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed", e);
      throw e;
    }
  }

  public boolean objectExists(String catalog, String schema, String volume, String objectName)
      throws SQLException {
    return objectExists(catalog, schema, volume, objectName, true);
  }
}
