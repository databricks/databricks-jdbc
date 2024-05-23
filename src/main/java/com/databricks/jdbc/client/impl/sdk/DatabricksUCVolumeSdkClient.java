package com.databricks.jdbc.client.impl.sdk;

import static com.databricks.jdbc.client.impl.sdk.ResultConstants.TYPE_INFO_RESULT;

import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.DatabricksUCVolumeClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.commons.util.WildcardUtil;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.core.ImmutableSqlParameter;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;

/** Implementation for DatabricksUCVolumeClient using SDK client + SQL Exec API*/
public class DatabricksUCVolumeSdkClient implements DatabricksUCVolumeClient{

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSdkClient.class);

    private final DatabricksSdkClient sdkClient;

    public DatabricksUCVolumeSdkClient(DatabricksSdkClient sdkClient) {this.sdkClient = sdkClient;
    }

    @Override
    public boolean putObject(IDatabricksSession session, String volumeNamePattern, String localFilePath) throws SQLException {
        return false;
    }

    @Override
    public boolean getObject(IDatabricksSession session, String volumeNamePattern, String localFilePath) throws SQLException {
        return false;
    }

    @Override
    public boolean prefixExists(Connection con, String path, String prefix) throws SQLException {
        // Construct the DBFS path
        String dbfsPath = "/Volumes/" + path;

        // Construct the SQL command to list all files in the specified DBFS directory
        String listFilesSQL = "LIST '" + dbfsPath + "'";

        // Create a Statement
        Statement statement = con.createStatement();

        // Execute the SQL command and get a ResultSet
        ResultSet resultSet = statement.executeQuery(listFilesSQL);

        // Iterate over the ResultSet and check if the specified prefix exists in the file names
        while (resultSet.next()) {
            String fileName = resultSet.getString("name");
            if (fileName.startsWith(prefix)) {
                return true;
            }
        }

        // If the specified prefix was not found in the file names, return false
        return false;
    }
}
