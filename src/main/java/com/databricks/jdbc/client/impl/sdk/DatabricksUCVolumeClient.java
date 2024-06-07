package com.databricks.jdbc.client.impl.sdk;

import com.databricks.jdbc.client.IDatabricksUCVolumeClient;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Implementation for DatabricksUCVolumeClient */
public class DatabricksUCVolumeClient implements IDatabricksUCVolumeClient {

  private final Connection connection;

  private static final Logger LOGGER = LogManager.getLogger(DatabricksSdkClient.class);

  private static final String UC_VOLUME_COLUMN_NAME =
      "name"; // Column name for the file names within a volume

  private static final String UC_VOLUME_NAME =
      "volume_name"; // Column name for the volume names within a schema

  public DatabricksUCVolumeClient(Connection connection) {
    this.connection = connection;
  }

  private String createListQuery(String catalog, String schema, String volume) {
    return String.format("LIST '/Volumes/%s/%s/%s/'", catalog, schema, volume);
  }

  private String createShowVolumesQuery(String catalog, String schema) {
    return String.format("SHOW VOLUMES IN %s.%s", catalog, schema);
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

    // Extract the sub-folder and append to volume to use LIST at the correct location, prefix is
    // checked for after listing
    int lastSlashIndex = prefix.lastIndexOf("/");
    if (lastSlashIndex != -1) {
      String folder = prefix.substring(0, lastSlashIndex);
      volume = volume + "/" + folder;
      prefix = prefix.substring(lastSlashIndex + 1);
    }

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
      String catalog, String schema, String volume, String objectPath, boolean caseSensitive)
      throws SQLException {

    LOGGER.info(
        "Entering objectExists method with parameters: catalog={}, schema={}, volume={}, objectPath={}, caseSensitive={}",
        catalog,
        schema,
        volume,
        objectPath,
        caseSensitive);

    // Extract the sub-folder and append to volume to use LIST at the correct location, objectName
    // is checked for after listing
    String objectName;

    int lastSlashIndex = objectPath.lastIndexOf("/");
    if (lastSlashIndex != -1) {
      String folder = objectPath.substring(0, lastSlashIndex);
      volume = volume + "/" + folder;
      objectName = objectPath.substring(lastSlashIndex + 1);
    } else {
      objectName = objectPath;
    }

    String listFilesSQLQuery = createListQuery(catalog, schema, volume);

    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(listFilesSQLQuery);
      LOGGER.info("SQL query executed successfully");

      boolean exists = false;
      while (resultSet.next()) {
        String fileName = resultSet.getString(UC_VOLUME_COLUMN_NAME);
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

  public boolean objectExists(String catalog, String schema, String volume, String objectPath)
      throws SQLException {
    return objectExists(catalog, schema, volume, objectPath, true);
  }

  @Override
  public boolean volumeExists(
      String catalog, String schema, String volumeName, boolean caseSensitive) throws SQLException {

    LOGGER.info(
        "Entering volumeExists method with parameters: catalog={}, schema={}, volumeName={}, caseSensitive={}",
        catalog,
        schema,
        volumeName,
        caseSensitive);

    String showVolumesSQLQuery = createShowVolumesQuery(catalog, schema);

    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(showVolumesSQLQuery);
      LOGGER.info("SQL query executed successfully");

      boolean exists = false;
      while (resultSet.next()) {
        String volume = resultSet.getString(UC_VOLUME_NAME);
        if (volume.regionMatches(
            /* ignoreCase= */ !caseSensitive,
            /* targetOffset= */ 0,
            /* other= */ volumeName,
            /* sourceOffset= */ 0,
            /* len= */ volumeName.length())) {
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

  public boolean volumeExists(String catalog, String schema, String volumeName)
      throws SQLException {
    return volumeExists(catalog, schema, volumeName, true);
  }

  @Override
  public List<String> listObjects(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {

    LOGGER.info(
        "Entering listObjects method with parameters: catalog={}, schema={}, volume={}, prefix={}, caseSensitive={}",
        catalog,
        schema,
        volume,
        prefix,
        caseSensitive);

    // Extract the sub-folder and append to volume to use LIST at the correct location, prefix is
    // checked for after listing
    int lastSlashIndex = prefix.lastIndexOf("/");
    if (lastSlashIndex != -1) {
      String folder = prefix.substring(0, lastSlashIndex);
      volume = volume + "/" + folder;
      prefix = prefix.substring(lastSlashIndex + 1);
    }

    String listFilesSQLQuery = createListQuery(catalog, schema, volume);

    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(listFilesSQLQuery);
      LOGGER.info("SQL query executed successfully");

      List<String> filenames = new ArrayList<>();
      while (resultSet.next()) {
        String fileName = resultSet.getString("name");
        if (fileName.regionMatches(
            /* ignoreCase= */ !caseSensitive,
            /* targetOffset= */ 0,
            /* StringToCheck= */ prefix,
            /* sourceOffset= */ 0,
            /* lengthToMatch= */ prefix.length())) {
          filenames.add(fileName);
        }
      }
      return filenames;
    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed", e);
      throw e;
    }
  }

  public List<String> listObjects(String catalog, String schema, String volume, String prefix)
      throws SQLException {
    return listObjects(catalog, schema, volume, prefix, true);
  }
}
