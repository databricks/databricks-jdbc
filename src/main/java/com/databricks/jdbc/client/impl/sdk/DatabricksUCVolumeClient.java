package com.databricks.jdbc.client.impl.sdk;

import com.databricks.jdbc.client.IDatabricksUCVolumeClient;
import com.databricks.jdbc.core.IDatabricksSession;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation for DatabricksUCVolumeClient using SDK client + SQL Exec API*/
public class DatabricksUCVolumeClient implements IDatabricksUCVolumeClient{
    
    private final String jdbcUrl;

    private final String user;

    private final String password;

    private final Connection connection;

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSdkClient.class);
    

    public DatabricksUCVolumeClient(String jdbcUrl, String user, String password, Connection connection) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.connection = connection;
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
    public boolean prefixExists(String catalog, String schema, String volume, String prefix, Statement statement) throws SQLException {

        String listFilesSQL = "LIST '/Volumes/"  + catalog + "/" + schema + "/" + volume + "/'";

        ResultSet resultSet = statement.executeQuery(listFilesSQL);

        boolean exists = false;
        while (resultSet.next()) {
            String fileName = resultSet.getString("name");
            if (fileName.startsWith(prefix)) {
                exists = true;
                break;
            }
        }
        return exists;
    }

}
