package com.databricks.jdbc.api;

import java.sql.SQLException;

/**
 * This interface is for performing volume operations on the Databricks File System (DBFS) volumes.
 * Compared to the existing UC Volume client interface, here we optimise the first step of getting
 * the pre signed url by not executing an SQL Query and instead directly calling the API to get the
 * pre signed url. This reduces the step of starting a warehouse to just get the pre signed url
 */
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
