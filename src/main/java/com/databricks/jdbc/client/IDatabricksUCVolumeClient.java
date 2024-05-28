package com.databricks.jdbc.client;

import com.databricks.jdbc.core.IDatabricksSession;

import java.sql.SQLException;
import java.sql.Statement;

public interface IDatabricksUCVolumeClient {

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
     * @param catalog
     * @param schema
     * @param volume
     * @param prefix
     * @param statement
     * @return a boolean indicating whether the prefix exists or not
     */
    boolean prefixExists(
            String catalog, String schema, String volume, String prefix, Statement statement) throws SQLException;


}