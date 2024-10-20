package com.databricks.jdbc.api;

import java.sql.SQLException;

public interface IDBFSVolumeClient {

  /**
   * putObject(): Upload data from a local path to a specified path within a UC Volume.
   *
   * @param catalog the catalog name of the cloud storage
   * @param schema the schema name of the cloud storage
   * @param volume the UC volume name of the cloud storage
   * @param objectPath the destination path where the object (file) is to be uploaded from the
   *     volume as the root directory
   * @param localPath the local path from where the data is to be uploaded
   * @return a boolean value indicating status of the PUT operation
   */
  boolean putObject(
      String catalog, String schema, String volume, String objectPath, String localPath)
      throws SQLException;
}
