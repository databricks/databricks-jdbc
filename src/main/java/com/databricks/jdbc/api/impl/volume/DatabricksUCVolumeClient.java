package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_SUCCEEDED;
import static com.databricks.jdbc.common.util.StringUtil.escapeStringLiteral;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.IDatabricksUCVolumeClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.entity.InputStreamEntity;

/** Implementation for DatabricksUCVolumeClient */
public class DatabricksUCVolumeClient implements IDatabricksUCVolumeClient {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksUCVolumeClient.class);
  private final Connection connection;

  private static final String UC_VOLUME_COLUMN_NAME =
      "name"; // Column name for the file names within a volume

  private static final String UC_VOLUME_NAME =
      "volume_name"; // Column name for the volume names within a schema

  public DatabricksUCVolumeClient(Connection connection) {
    this.connection = connection;
  }

  private String getVolumePath(String catalog, String schema, String volume) {
    // We need to escape '' to prevent SQL injection
    return escapeStringLiteral(String.format("/Volumes/%s/%s/%s/", catalog, schema, volume));
  }

  private String getObjectFullPath(
      String catalog, String schema, String volume, String objectPath) {
    return getVolumePath(catalog, schema, volume) + escapeStringLiteral(objectPath);
  }

  private String createListQuery(String catalog, String schema, String volume) {
    return String.format("LIST '%s'", getVolumePath(catalog, schema, volume));
  }

  private String createShowVolumesQuery(String catalog, String schema) {
    return String.format("SHOW VOLUMES IN %s.%s", catalog, schema);
  }

  private String createGetObjectQuery(
      String catalog, String schema, String volume, String objectPath, String localPath) {
    return String.format(
        "GET '%s' TO '%s'",
        getObjectFullPath(catalog, schema, volume, objectPath), escapeStringLiteral(localPath));
  }

  private String createGetObjectQueryForInputStream(
      String catalog, String schema, String volume, String objectPath) {
    return String.format(
        "GET '%s' TO '__input_stream__'", getObjectFullPath(catalog, schema, volume, objectPath));
  }

  private String createPutObjectQuery(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean toOverwrite) {
    return String.format(
        "PUT '%s' INTO '%s'%s",
        escapeStringLiteral(localPath),
        getObjectFullPath(catalog, schema, volume, objectPath),
        toOverwrite ? " OVERWRITE" : "");
  }

  private String createPutObjectQueryForInputStream(
      String catalog, String schema, String volume, String objectPath, boolean toOverwrite) {
    return String.format(
        "PUT '__input_stream__' INTO '%s'%s",
        getObjectFullPath(catalog, schema, volume, objectPath), toOverwrite ? " OVERWRITE" : "");
  }

  private String createDeleteObjectQuery(
      String catalog, String schema, String volume, String objectPath) {
    return String.format("REMOVE '%s'", getObjectFullPath(catalog, schema, volume, objectPath));
  }

  public boolean prefixExists(String catalog, String schema, String volume, String prefix)
      throws SQLException {
    return prefixExists(catalog, schema, volume, prefix, true);
  }

  @Override
  public boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {

    LOGGER.debug(
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
      try (ResultSet resultSet = statement.executeQuery(listFilesSQLQuery)) {
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
      }
    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed " + e);
      throw e;
    }
  }

  @Override
  public boolean objectExists(
      String catalog, String schema, String volume, String objectPath, boolean caseSensitive)
      throws SQLException {

    LOGGER.info(
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
      try (ResultSet resultSet = statement.executeQuery(listFilesSQLQuery)) {
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
      }

    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed " + e);
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
        String.format(
            "Entering volumeExists method with parameters: catalog={%s}, schema={%s}, volumeName={%s}, caseSensitive={%s}",
            catalog, schema, volumeName, caseSensitive));

    String showVolumesSQLQuery = createShowVolumesQuery(catalog, schema);

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(showVolumesSQLQuery)) {
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
      }
    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed " + e);
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
      try (ResultSet resultSet = statement.executeQuery(listFilesSQLQuery)) {
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
      }
    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed" + e);
      throw e;
    }
  }

  public List<String> listObjects(String catalog, String schema, String volume, String prefix)
      throws SQLException {
    return listObjects(catalog, schema, volume, prefix, true);
  }

  public boolean getObject(
      String catalog, String schema, String volume, String objectPath, String localPath)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "Entering getObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}, localPath={%s}",
            catalog, schema, volume, objectPath, localPath));

    String getObjectQuery = createGetObjectQuery(catalog, schema, volume, objectPath, localPath);

    boolean volumeOperationStatus = false;

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(getObjectQuery)) {
        LOGGER.info("GET query executed successfully");
        if (resultSet.next()) {
          String volumeOperationStatusString =
              resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME);
          volumeOperationStatus =
              VOLUME_OPERATION_STATUS_SUCCEEDED.equals(volumeOperationStatusString);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("GET query execution failed " + e);
      throw e;
    }

    return volumeOperationStatus;
  }

  @Override
  public InputStreamEntity getObject(
      String catalog, String schema, String volume, String objectPath) throws SQLException {

    LOGGER.debug(
        String.format(
            "Entering getObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}",
            catalog, schema, volume, objectPath));

    String getObjectQuery = createGetObjectQueryForInputStream(catalog, schema, volume, objectPath);

    try (Statement statement = connection.createStatement()) {
      IDatabricksStatement databricksStatement = (IDatabricksStatement) statement;
      databricksStatement.allowInputStreamForVolumeOperation(true);

      try (ResultSet resultSet = statement.executeQuery(getObjectQuery)) {
        LOGGER.info("GET query executed successfully");
        if (resultSet.next()) {
          return ((IDatabricksResultSet) resultSet).getVolumeOperationInputStream();
        } else {
          return null;
        }
      } catch (SQLException e) {
        LOGGER.error("GET query execution failed " + e);
        throw e;
      }
    }
  }

  public boolean putObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean toOverwrite)
      throws SQLException {

    LOGGER.debug(
        String.format(
            "Entering putObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}, localPath={%s}, toOverwrite={%s}",
            catalog, schema, volume, objectPath, localPath, toOverwrite));

    String putObjectQuery =
        createPutObjectQuery(catalog, schema, volume, objectPath, localPath, toOverwrite);

    boolean volumeOperationStatus = false;

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(putObjectQuery)) {
        LOGGER.info("PUT query executed successfully");
        if (resultSet.next()) {
          String volumeOperationStatusString =
              resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME);
          volumeOperationStatus =
              VOLUME_OPERATION_STATUS_SUCCEEDED.equals(volumeOperationStatusString);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("PUT query execution failed " + e);
      throw e;
    }

    return volumeOperationStatus;
  }

  @Override
  public boolean putObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      InputStream inputStream,
      long contentLength,
      boolean toOverwrite)
      throws SQLException {

    LOGGER.debug(
        String.format(
            "Entering putObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}, inputStream={%s}, toOverwrite={%s}",
            catalog, schema, volume, objectPath, inputStream, toOverwrite));

    String putObjectQueryForInputStream =
        createPutObjectQueryForInputStream(catalog, schema, volume, objectPath, toOverwrite);

    boolean volumeOperationStatus = false;

    try (Statement statement = connection.createStatement()) {
      IDatabricksStatement databricksStatement = (IDatabricksStatement) statement;
      databricksStatement.allowInputStreamForVolumeOperation(true);
      databricksStatement.setInputStreamForUCVolume(
          new InputStreamEntity(inputStream, contentLength));

      try (ResultSet resultSet = statement.executeQuery(putObjectQueryForInputStream)) {
        LOGGER.info("PUT query executed successfully");
        if (resultSet.next()) {
          String volumeOperationStatusString =
              resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME);
          volumeOperationStatus =
              VOLUME_OPERATION_STATUS_SUCCEEDED.equals(volumeOperationStatusString);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("PUT query execution failed " + e);
      throw e;
    }

    return volumeOperationStatus;
  }

  public boolean deleteObject(String catalog, String schema, String volume, String objectPath)
      throws SQLException {

    LOGGER.debug(
        String.format(
            "Entering deleteObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}",
            catalog, schema, volume, objectPath));

    String deleteObjectQuery = createDeleteObjectQuery(catalog, schema, volume, objectPath);

    boolean volumeOperationStatus = false;

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(deleteObjectQuery)) {
        LOGGER.info("SQL query executed successfully");
        if (resultSet.next()) {
          String volumeOperationStatusString =
              resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME);
          volumeOperationStatus =
              VOLUME_OPERATION_STATUS_SUCCEEDED.equals(volumeOperationStatusString);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed " + e);
      throw e;
    }

    return volumeOperationStatus;
  }
}
