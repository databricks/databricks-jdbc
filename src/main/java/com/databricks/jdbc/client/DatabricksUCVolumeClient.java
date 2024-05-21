package com.databricks.jdbc.client;

import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;

import java.sql.Connection;
import java.sql.SQLException;

public interface DatabricksUCVolumeClient {

    /**
     * putObject(): Uploads a file from an input stream to the UC Volume
     *
     * @param session           underlying session
     * @param volumeNamePattern must match to volume name in database (can be a regex pattern or
     *                          absolute name)
     * @param localFilePath     local file path where the input file is present
     * @return a boolean indicating whether the upload was successful or not
     */
    boolean putObject(
            IDatabricksSession session, String volumeNamePattern, String localFilePath) throws SQLException;

    /**
     * getObject(): Retrieves objects directly from the UC Volume as input streams for further processing within applications
     *
     * @param session           underlying session
     * @param volumeNamePattern must match to volume name in database (can be a regex pattern or
     *                          absolute name)
     * @param localFilePath     local file path where the file will be saved to
     * @return a boolean indicating whether the download was successful or not
     */
    boolean getObject(
            IDatabricksSession session, String volumeNamePattern, String localFilePath) throws SQLException;

    /**
     * prefixExists(): Determines if a specific prefix (folder-like structure) exists in the UC Volume
     *
     * @param con                  underlying JDBC Connection
     * @param catalogName          name of the destination catalog
     * @param schemaName           name of the destination schema within the destination catalog
     * @param prefix must match to principal name in database (can be a regex pattern or
     *                             absolute name)
     * @return a boolean indicating whether the prefix exists or not
     */
    boolean prefixExists(
            Connection con, String catalogName, String schemaName, String prefix) throws SQLException;
}



