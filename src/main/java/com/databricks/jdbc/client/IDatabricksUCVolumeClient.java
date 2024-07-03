package com.databricks.jdbc.client;

import java.sql.SQLException;
import java.util.List;

public interface IDatabricksUCVolumeClient {

  /**
   * prefixExists(): Determines if a specific prefix (folder-like structure) exists in the UC Volume
   * The prefix that we are looking for must be a part of the file name.
   *
   * @param catalog the catalog name of the cloud storage
   * @param schema the schema name of the cloud storage
   * @param volume the UC volume name of the cloud storage
   * @param prefix the prefix to check for existence along with the relative path from the volume as
   *     the root directory
   * @param caseSensitive a boolean indicating whether the check should be case-sensitive or not
   * @return a boolean indicating whether the prefix exists or not
   */
  boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException;

  /**
   * objectExists(): Determines if a specific object (file) exists in the UC Volume The object that
   * we are looking for must match the file name exactly
   *
   * @param catalog the catalog name of the cloud storage
   * @param schema the schema name of the cloud storage
   * @param volume the UC volume name of the cloud storage
   * @param objectPath the path of the object (file) from the volume as the root directory to check
   *     for existence within the volume (inside any sub-folder)
   * @param caseSensitive a boolean indicating whether the check should be case-sensitive or not
   * @return a boolean indicating whether the object exists or not
   */
  boolean objectExists(
      String catalog, String schema, String volume, String objectPath, boolean caseSensitive)
      throws SQLException;

  /**
   * volumeExists(): Determines if a specific volume exists in the given catalog and schema. The
   * volume that we are looking for must match the volume name exactly.
   *
   * @param catalog the catalog name of the cloud storage
   * @param schema the schema name of the cloud storage
   * @param volumeName the name of the volume to check for existence
   * @param caseSensitive a boolean indicating whether the check should be case-sensitive or not
   * @return a boolean indicating whether the volume exists or not
   */
  boolean volumeExists(String catalog, String schema, String volumeName, boolean caseSensitive)
      throws SQLException;

  /**
   * listObjects(): Lists all filenames in the UC Volume that start with a specified prefix. The
   * prefix that we are looking for must be a part of the file path from the volume as the root.
   *
   * @param catalog the catalog name of the cloud storage
   * @param schema the schema name of the cloud storage
   * @param volume the UC volume name of the cloud storage
   * @param prefix the prefix of the filenames to list. This includes the relative path from the
   *     volume as the root directory
   * @param caseSensitive a boolean indicating whether the check should be case-sensitive or not
   * @return a list of strings indicating the filenames that start with the specified prefix
   */
  List<String> listObjects(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException;

  /**
   * getObject(): Lists all filenames in the UC Volume that start with a specified prefix. The
   * prefix that we are looking for must be a part of the file path from the volume as the root.
   *
   * @param catalog the catalog name of the cloud storage
   * @param schema the schema name of the cloud storage
   * @param volume the UC volume name of the cloud storage
   * @param localPath the local path where the retrieved data is to be stored
   * @return a boolean value indicating status of the GET operation
   */
  boolean getObject(
          String catalog, String schema, String volume, String localPath)
          throws SQLException;
}
