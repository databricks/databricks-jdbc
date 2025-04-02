package com.databricks.jdbc.api;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import org.apache.http.entity.InputStreamEntity;

/**
 * Interface for interacting with Databricks Unity Catalog (UC) Volumes. Provides methods for
 * managing and accessing files and directories within UC Volumes, supporting operations such as
 * checking existence, listing contents, and performing CRUD (Create, Read, Update, Delete)
 * operations on objects stored in volumes.
 */
public interface IDatabricksVolumeClient {

  /**
   * Checks if a specific prefix (folder-like structure) exists in the UC Volume. The prefix must be
   * a part of the file name or path.
   *
   * @param catalog the catalog name in Unity Catalog
   * @param schema the schema name in the specified catalog
   * @param volume the volume name in the specified schema
   * @param prefix the prefix to check, including the relative path from the volume root
   * @param caseSensitive whether the prefix check should be case-sensitive
   * @return true if the prefix exists, false otherwise
   * @throws SQLException if a database access error occurs or the volume is inaccessible
   */
  boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException;

  /**
   * Checks if a specific object (file) exists in the UC Volume. The object path must exactly match
   * an existing file.
   *
   * @param catalog the catalog name in Unity Catalog
   * @param schema the schema name in the specified catalog
   * @param volume the volume name in the specified schema
   * @param objectPath the path of the object from the volume root
   * @param caseSensitive whether the path check should be case-sensitive
   * @return true if the object exists, false otherwise
   * @throws SQLException if a database access error occurs or the volume is inaccessible
   */
  boolean objectExists(
      String catalog, String schema, String volume, String objectPath, boolean caseSensitive)
      throws SQLException;

  /**
   * Checks if a specific volume exists in the given catalog and schema. The volume name must match
   * exactly.
   *
   * @param catalog the catalog name in Unity Catalog
   * @param schema the schema name in the specified catalog
   * @param volumeName the name of the volume to check
   * @param caseSensitive whether the volume name check should be case-sensitive
   * @return true if the volume exists, false otherwise
   * @throws SQLException if a database access error occurs
   */
  boolean volumeExists(String catalog, String schema, String volumeName, boolean caseSensitive)
      throws SQLException;

  /**
   * Lists all objects (files) in the UC Volume that start with a specified prefix. The prefix must
   * be a part of the file path from the volume root.
   *
   * @param catalog the catalog name in Unity Catalog
   * @param schema the schema name in the specified catalog
   * @param volume the volume name in the specified schema
   * @param prefix the prefix to filter objects by, including the relative path from volume root
   * @param caseSensitive whether the prefix matching should be case-sensitive
   * @return a list of object paths that match the specified prefix
   * @throws SQLException if a database access error occurs or the volume is inaccessible
   */
  List<String> listObjects(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException;

  /**
   * Downloads an object (file) from the UC Volume to a local path.
   *
   * @param catalog the catalog name in Unity Catalog
   * @param schema the schema name in the specified catalog
   * @param volume the volume name in the specified schema
   * @param objectPath the path of the object in the volume
   * @param localPath the local filesystem path where the object should be saved
   * @return true if the download was successful, false otherwise
   * @throws SQLException if a database access error occurs, the volume is inaccessible, or there
   *     are issues with the local filesystem
   */
  boolean getObject(
      String catalog, String schema, String volume, String objectPath, String localPath)
      throws SQLException;

  /**
   * Retrieves an object as an input stream from the UC Volume. The caller is responsible for
   * closing the returned input stream.
   *
   * @param catalog the catalog name in Unity Catalog
   * @param schema the schema name in the specified catalog
   * @param volume the volume name in the specified schema
   * @param objectPath the path of the object in the volume
   * @return an InputStreamEntity containing the object's data
   * @throws SQLException if a database access error occurs or the volume is inaccessible
   */
  InputStreamEntity getObject(String catalog, String schema, String volume, String objectPath)
      throws SQLException;

  /**
   * Uploads data from a local file to a specified path within a UC Volume.
   *
   * @param catalog the catalog name in Unity Catalog
   * @param schema the schema name in the specified catalog
   * @param volume the volume name in the specified schema
   * @param objectPath the destination path in the volume where the file should be uploaded
   * @param localPath the local filesystem path of the file to upload
   * @param toOverwrite whether to overwrite the object if it already exists
   * @return true if the upload was successful, false otherwise
   * @throws SQLException if a database access error occurs, the volume is inaccessible, or there
   *     are issues with the local filesystem
   */
  boolean putObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      String localPath,
      boolean toOverwrite)
      throws SQLException;

  /**
   * Uploads data from an input stream to a specified path within a UC Volume.
   *
   * @param catalog the catalog name in Unity Catalog
   * @param schema the schema name in the specified catalog
   * @param volume the volume name in the specified schema
   * @param objectPath the destination path in the volume where the data should be uploaded
   * @param inputStream the input stream containing the data to upload
   * @param contentLength the length of the data in bytes
   * @param toOverwrite whether to overwrite the object if it already exists
   * @return true if the upload was successful, false otherwise
   * @throws SQLException if a database access error occurs or the volume is inaccessible
   */
  boolean putObject(
      String catalog,
      String schema,
      String volume,
      String objectPath,
      InputStream inputStream,
      long contentLength,
      boolean toOverwrite)
      throws SQLException;

  /**
   * Deletes an object from a specified path within a UC Volume.
   *
   * @param catalog the catalog name in Unity Catalog
   * @param schema the schema name in the specified catalog
   * @param volume the volume name in the specified schema
   * @param objectPath the path of the object to delete
   * @return true if the deletion was successful, false otherwise
   * @throws SQLException if a database access error occurs or the volume is inaccessible
   */
  boolean deleteObject(String catalog, String schema, String volume, String objectPath)
      throws SQLException;
}
