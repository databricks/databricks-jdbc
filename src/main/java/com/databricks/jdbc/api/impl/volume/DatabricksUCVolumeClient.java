package com.databricks.jdbc.api.impl.volume;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_SUCCEEDED;
import static com.databricks.jdbc.common.util.StringUtil.escapeStringLiteral;

import com.databricks.jdbc.api.IDatabricksUCVolumeClient;
import com.databricks.jdbc.api.callback.IDatabricksResultSetHandle;
import com.databricks.jdbc.api.callback.IDatabricksStatementHandle;
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

  private static final String UC_VOLUME_COLUMN_VOLUME_NAME =
      "volume_name"; // Column name for the volume names within a schema

  private static class FilePath {
    public FilePath(String path) {
      int lastSlashIndex = path.lastIndexOf("/");
      folder = (lastSlashIndex >= 0) ? path.substring(0, lastSlashIndex) : "";
      basename = (lastSlashIndex >= 0) ? path.substring(lastSlashIndex + 1) : path;
    }

    public String folder;
    public String basename;
  }

  public DatabricksUCVolumeClient(Connection connection) {
    this.connection = connection;
  }

  private static String getVolumePath(String catalog, String schema, String volume) {
    // We need to escape '' to prevent SQL injection
    return escapeStringLiteral(String.format("/Volumes/%s/%s/%s/", catalog, schema, volume));
  }

  private static String getObjectFullPath(
      String catalog, String schema, String volume, String objectPath) {
    return getVolumePath(catalog, schema, volume) + escapeStringLiteral(objectPath);
  }

  private static String createListQuery(String catalog, String schema, String volume) {
    return String.format("LIST '%s'", getVolumePath(catalog, schema, volume));
  }

  private static String createListQuery(
      String catalog, String schema, String volume, String folder) {
    return (folder.isEmpty())
        ? createListQuery(catalog, schema, volume)
        : createListQuery(catalog, schema, volume + "/" + folder);
  }

  private static String createShowVolumesQuery(String catalog, String schema) {
    return String.format("SHOW VOLUMES IN %s.%s", catalog, schema);
  }

  private static String createGetObjectQuery(
      String catalog, String schema, String volume, String objectPath, String localPath) {
    return String.format(
        "GET '%s' TO '%s'",
        getObjectFullPath(catalog, schema, volume, objectPath), escapeStringLiteral(localPath));
  }

  private static String createGetObjectQueryForInputStream(
      String catalog, String schema, String volume, String objectPath) {
    return String.format(
        "GET '%s' TO '__input_stream__'", getObjectFullPath(catalog, schema, volume, objectPath));
  }

  private static String createPutObjectQuery(
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

  private static String createPutObjectQueryForInputStream(
      String catalog, String schema, String volume, String objectPath, boolean toOverwrite) {
    return String.format(
        "PUT '__input_stream__' INTO '%s'%s",
        getObjectFullPath(catalog, schema, volume, objectPath), toOverwrite ? " OVERWRITE" : "");
  }

  private static String createDeleteObjectQuery(
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

    if (prefix.isEmpty()) {
      return false;
    }

    LOGGER.debug(
        String.format(
            "Entering prefixExists method with parameters: catalog={%s}, schema={%s}, volume={%s}, prefix={%s}, caseSensitive={%s}",
            catalog, schema, volume, prefix, caseSensitive));

    FilePath filePath = new FilePath(prefix);

    String listFilesSQLQuery = createListQuery(catalog, schema, volume, filePath.folder);

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(listFilesSQLQuery)) {
        LOGGER.debug("SQL query executed successfully");
        boolean exists = false;
        while (resultSet.next()) {
          String fileName = resultSet.getString(UC_VOLUME_COLUMN_NAME);
          if (fileName.regionMatches(
              /* ignoreCase= */ !caseSensitive,
              /* targetOffset= */ 0,
              /* StringToCheck= */ filePath.basename,
              /* sourceOffset= */ 0,
              /* lengthToMatch= */ filePath.basename.length())) {
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

    if (objectPath.isEmpty()) {
      return false;
    }

    LOGGER.info(
        String.format(
            "Entering objectExists method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}, caseSensitive={%s}",
            catalog, schema, volume, objectPath, caseSensitive));

    FilePath filePath = new FilePath(objectPath);

    String listFilesSQLQuery = createListQuery(catalog, schema, volume, filePath.folder);

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(listFilesSQLQuery)) {
        LOGGER.info("SQL query executed successfully");
        boolean exists = false;
        while (resultSet.next()) {
          String fileName = resultSet.getString(UC_VOLUME_COLUMN_NAME);
          if (fileName.regionMatches(
              /* ignoreCase= */ !caseSensitive,
              /* targetOffset= */ 0,
              /* StringToCheck= */ filePath.basename,
              /* sourceOffset= */ 0,
              /* lengthToMatch= */ filePath.basename.length())) {
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

    if (volumeName.isEmpty()) {
      return false;
    }

    String showVolumesSQLQuery = createShowVolumesQuery(catalog, schema);

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(showVolumesSQLQuery)) {
        LOGGER.info("SQL query executed successfully");
        boolean exists = false;
        while (resultSet.next()) {
          String volume = resultSet.getString(UC_VOLUME_COLUMN_VOLUME_NAME);
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

  /**
   * This functions lists all the files that fall under the specified prefix within the target
   * folder in the specified volume. The prefix is checked with the word after the last / in the
   * input Ex - 1. foo/bar will list all the files within foo folder having bar as prefix | 2.
   * foo/bar/f will list all the files within the bar folder with prefix f | 3. foo/bar/ will list
   * all the files within the bar folder with all prefix
   *
   * @param catalog the catalog name of the cloud storage
   * @param schema the schema name of the cloud storage
   * @param volume the UC volume name of the cloud storage
   * @param prefix the prefix of the filenames to list. This includes the relative path from the
   *     volume as the root directory
   * @param caseSensitive a boolean indicating whether the check should be case-sensitive or not
   * @return List<String> a list of strings indicating the filenames that start with the specified
   *     prefix
   */
  @Override
  public List<String> listObjects(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException {

    LOGGER.info(
        String.format(
            "Entering listObjects method with parameters: catalog={%s}, schema={%s}, volume={%s}, prefix={%s}, caseSensitive={%s}",
            catalog, schema, volume, prefix, caseSensitive));

    FilePath filePath = new FilePath(prefix);

    String listFilesSQLQuery = createListQuery(catalog, schema, volume, filePath.folder);

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(listFilesSQLQuery)) {
        LOGGER.info("SQL query executed successfully");
        List<String> filenames = new ArrayList<>();
        while (resultSet.next()) {
          String fileName = resultSet.getString("name");
          if (filePath.basename.isEmpty()
              || fileName.regionMatches(
                  /* ignoreCase= */ !caseSensitive,
                  /* targetOffset= */ 0,
                  /* StringToCheck= */ filePath.basename,
                  /* sourceOffset= */ 0,
                  /* lengthToMatch= */ filePath.basename.length())) {
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
      IDatabricksStatementHandle databricksStatement =
          statement.unwrap(IDatabricksStatementHandle.class);
      databricksStatement.allowInputStreamForVolumeOperation(true);

      try (ResultSet resultSet = statement.executeQuery(getObjectQuery)) {
        LOGGER.info("GET query executed successfully");
        if (resultSet.next()) {
          return resultSet.unwrap(IDatabricksResultSetHandle.class).getVolumeOperationInputStream();
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

    boolean isOperationSucceeded = false;

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(putObjectQuery)) {
        LOGGER.info("PUT query executed successfully");
        if (resultSet.next()) {
          String volumeOperationStatusString =
              resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME);
          isOperationSucceeded =
              VOLUME_OPERATION_STATUS_SUCCEEDED.equals(volumeOperationStatusString);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("PUT query execution failed " + e);
      throw e;
    }

    return isOperationSucceeded;
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

    boolean isOperationSucceeded = false;

    try (Statement statement = connection.createStatement()) {
      IDatabricksStatementHandle databricksStatement =
          statement.unwrap(IDatabricksStatementHandle.class);
      databricksStatement.allowInputStreamForVolumeOperation(true);
      databricksStatement.setInputStreamForUCVolume(
          new InputStreamEntity(inputStream, contentLength));

      try (ResultSet resultSet = statement.executeQuery(putObjectQueryForInputStream)) {
        LOGGER.info("PUT query executed successfully");
        if (resultSet.next()) {
          String volumeOperationStatusString =
              resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME);
          isOperationSucceeded =
              VOLUME_OPERATION_STATUS_SUCCEEDED.equals(volumeOperationStatusString);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("PUT query execution failed " + e);
      throw e;
    }

    return isOperationSucceeded;
  }

  public boolean deleteObject(String catalog, String schema, String volume, String objectPath)
      throws SQLException {

    LOGGER.debug(
        String.format(
            "Entering deleteObject method with parameters: catalog={%s}, schema={%s}, volume={%s}, objectPath={%s}",
            catalog, schema, volume, objectPath));

    String deleteObjectQuery = createDeleteObjectQuery(catalog, schema, volume, objectPath);

    boolean isOperationSucceeded = false;

    try (Statement statement = connection.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery(deleteObjectQuery)) {
        LOGGER.info("SQL query executed successfully");
        if (resultSet.next()) {
          String volumeOperationStatusString =
              resultSet.getString(VOLUME_OPERATION_STATUS_COLUMN_NAME);
          isOperationSucceeded =
              VOLUME_OPERATION_STATUS_SUCCEEDED.equals(volumeOperationStatusString);
        }
      }
    } catch (SQLException e) {
      LOGGER.error("SQL query execution failed " + e);
      throw e;
    }

    return isOperationSucceeded;
  }
}
