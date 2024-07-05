package com.databricks.jdbc.client.impl.sdk;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_SUCCEEDED;

import com.databricks.jdbc.client.IDatabricksUCVolumeClient;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.LoggingUtil;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/** Implementation for DatabricksUCVolumeClient */
public class DatabricksUCVolumeClient implements IDatabricksUCVolumeClient {

  private final Connection connection;

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

  private String createGetObjectQuery(
      String catalog, String schema, String volume, String localPath) {
    return String.format("GET '/Volumes/%s/%s/%s/' TO %s", catalog, schema, volume, localPath);
  }

  public boolean prefixExists(String catalog, String schema, String volume, String prefix)
      throws SQLException {
    return prefixExists(catalog, schema, volume, prefix, true);
  }

  @Override
  public boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {

    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "Entering prefixExists method with parameters: catalog={%s}, schema={%s}, volume={%s}, prefix={%s}, caseSensitive={%s}",
            catalog, schema, volume, prefix, caseSensitive));

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
      LoggingUtil.log(LogLevel.INFO, "SQL query executed successfully");

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
      LoggingUtil.log(LogLevel.ERROR, "SQL query execution failed " + e);
      throw e;
    }
  }

  @Override
  public boolean objectExists(
      String catalog, String schema, String volume, String objectPath, boolean caseSensitive)
      throws SQLException {

    LoggingUtil.log(
        LogLevel.INFO,
        String.format(
            "Entering objectExists method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}, caseSensitive={%s}",
            catalog, schema, volume, objectPath, caseSensitive));

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
      LoggingUtil.log(LogLevel.INFO, "SQL query executed successfully");

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
      LoggingUtil.log(LogLevel.ERROR, "SQL query execution failed " + e);
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

    LoggingUtil.log(
        LogLevel.INFO,
        String.format(
            "Entering volumeExists method with parameters: catalog={%s}, schema={%s}, volumeName={%s}, caseSensitive={%s}",
            catalog, schema, volumeName, caseSensitive));

    String showVolumesSQLQuery = createShowVolumesQuery(catalog, schema);

    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(showVolumesSQLQuery);
      LoggingUtil.log(LogLevel.INFO, "SQL query executed successfully");

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
      LoggingUtil.log(LogLevel.ERROR, "SQL query execution failed " + e);
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

    LoggingUtil.log(
        LogLevel.INFO,
        String.format(
            "Entering listObjects method with parameters: catalog={%s}, schema={%s}, volume={%s}, prefix={%s}, caseSensitive={%s}",
            catalog, schema, volume, prefix, caseSensitive));

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
      LoggingUtil.log(LogLevel.INFO, "SQL query executed successfully");

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
      LoggingUtil.log(LogLevel.ERROR, "SQL query execution failed" + e);
      throw e;
    }
  }

  public List<String> listObjects(String catalog, String schema, String volume, String prefix)
      throws SQLException {
    return listObjects(catalog, schema, volume, prefix, true);
  }

  public boolean getObject(String catalog, String schema, String volume, String localPath)
      throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "Entering getObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, localPath={%s}",
            catalog, schema, volume, localPath));

    String getObjectQuery = createGetObjectQuery(catalog, schema, volume, localPath);

    boolean volumeOperationStatus = false;

    try (Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(getObjectQuery);
      LoggingUtil.log(LogLevel.INFO, "SQL query executed successfully");

      if (resultSet.next()) {
        String volumeOperationStatusString =
            resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME);
        volumeOperationStatus =
            VOLUME_OPERATION_STATUS_SUCCEEDED.equals(volumeOperationStatusString);
      }
    } catch (SQLException e) {
      LoggingUtil.log(LogLevel.ERROR, "SQL query execution failed " + e);
      throw e;
    }

    return volumeOperationStatus;
  }
}
