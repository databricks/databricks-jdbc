package com.databricks.jdbc.client;

import java.sql.SQLException;

public interface IDatabricksUCVolumeClient {

  /**
   * prefixExists(): Determines if a specific prefix (folder-like structure) exists in the UC Volume
   * The prefix that we are looking for must be a part of the file name.
   *
   * @param catalog the catalog name of the cloud storage
   * @param schema the schema name of the cloud storage
   * @param volume the UC volume name of the cloud storage
   * @param prefix the prefix to check for existence
   * @param caseSensitive a boolean indicating whether the check should be case sensitive or not
   * @return a boolean indicating whether the prefix exists or not
   */
  boolean prefixExists(
      String catalog, String schema, String volume, String prefix, boolean caseSensitive)
      throws SQLException;
}
