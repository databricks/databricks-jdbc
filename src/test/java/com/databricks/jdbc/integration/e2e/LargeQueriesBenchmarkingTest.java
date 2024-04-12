package com.databricks.jdbc.integration.e2e;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;

public class LargeQueriesBenchmarkingTest {
    private Connection connection;

    private String SCHEMA_NAME = "jdbc_large_queries_benchmark_schema";
    private String TABLE_NAME = "large_queries_data";

    @BeforeEach
    void setUp() throws SQLException {
        connection = getValidJDBCConnection();
    }

    @Test
    void testLargeQueries() throws SQLException {
        // Currently connection is held by OSS driver
        long startTime = System.currentTimeMillis();
        measureLargeQueriesPerformance();
        tempWrite();
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken to execute large queries by OSS Driver: " + (endTime - startTime) + "ms");
//
//        connection.close();
//
//        DriverManager.deregisterDriver(new com.databricks.jdbc.driver.DatabricksDriver());
//        connection = DriverManager.getConnection(getJDBCUrl(), "token", getDatabricksToken());
//
//        startTime = System.currentTimeMillis();
//        measureLargeQueriesPerformance();
//        endTime = System.currentTimeMillis();
//        System.out.println("Time taken to execute large queries by Databricks Driver: " + (endTime - startTime) + "ms");
    }

    void measureLargeQueriesPerformance() {

    }

    void tempWrite() throws SQLException {
        Connection sourceConnection = DriverManager.getConnection("jdbc:databricks://benchmarking-prod-aws-us-west-2.cloud.databricks.com/default;AuthMech=3;transportMode=http;httpPath=sql/1.0/warehouses/28fe1ea1fb7869c2", "madhav", "dapi7b882f7eaad81183a3d5bdfec6345118");
        Connection targetConnection = connection;

        Statement sourceStatement = sourceConnection.createStatement();
        ResultSet resultSet = sourceStatement.executeQuery("SELECT * FROM " + "main.tpcds_sf100_delta.catalog_sales");
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Prepare insert statement using metadata
//        executeSQL("CREATE SCHEMA main.jdbc_large_queries_benchmark_schema");
//        executeSQL("CREATE TABLE main.jdbc_large_queries_benchmark_schema.large_queries_data");
        executeSQL("DROP TABLE main.jdbc_large_queries_benchmark_schema.large_queries_data");
        StringBuilder createTableSql = new StringBuilder("CREATE TABLE " + "main.jdbc_large_queries_benchmark_schema.large_queries_data" + " (");
        for (int i = 1; i <= columnCount; i++) {
            createTableSql.append(metaData.getColumnName(i))
                    .append(" ")
                    .append(metaData.getColumnTypeName(i));

            if (metaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                createTableSql.append(" NOT NULL");
            }

            if (i < columnCount) {
                createTableSql.append(", ");
            }
        }
        createTableSql.append(")");

        System.out.println(createTableSql);
        executeSQL(createTableSql.toString());
        StringBuilder insertSql = new StringBuilder("INSERT INTO " + "main.jdbc_large_queries_benchmark_schema.large_queries_data" + " (");
        StringBuilder valuesPlaceholder = new StringBuilder("VALUES (");

        for (int i = 1; i <= columnCount; i++) {
            insertSql.append(metaData.getColumnName(i));
            valuesPlaceholder.append("?");
            if (i < columnCount) {
                insertSql.append(", ");
                valuesPlaceholder.append(", ");
            }
        }
        insertSql.append(") ");
        valuesPlaceholder.append(")");
        insertSql.append(valuesPlaceholder);

        PreparedStatement targetStatement = targetConnection.prepareStatement(insertSql.toString());

        // Insert data into target
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                switch (metaData.getColumnType(i)) {
                    case java.sql.Types.INTEGER:
                        targetStatement.setInt(i, resultSet.getInt(i));
                        break;
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.CHAR:
                    case java.sql.Types.LONGVARCHAR:
                        targetStatement.setString(i, resultSet.getString(i));
                        break;
                    case java.sql.Types.DOUBLE:
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.REAL:
                        targetStatement.setDouble(i, resultSet.getDouble(i));
                        break;
                    case java.sql.Types.NUMERIC:
                    case java.sql.Types.DECIMAL:
                        targetStatement.setBigDecimal(i, resultSet.getBigDecimal(i));
                        break;
                    case java.sql.Types.BIT:
                    case java.sql.Types.BOOLEAN:
                        targetStatement.setBoolean(i, resultSet.getBoolean(i));
                        break;
                    case java.sql.Types.TINYINT:
                    case java.sql.Types.SMALLINT:
                        targetStatement.setShort(i, resultSet.getShort(i));
                        break;
                    case java.sql.Types.BIGINT:
                        targetStatement.setLong(i, resultSet.getLong(i));
                        break;
                    case java.sql.Types.DATE:
                        targetStatement.setDate(i, resultSet.getDate(i));
                        break;
                    case java.sql.Types.TIME:
                        targetStatement.setTime(i, resultSet.getTime(i));
                        break;
                    case java.sql.Types.TIMESTAMP:
                        targetStatement.setTimestamp(i, resultSet.getTimestamp(i));
                        break;
                    case java.sql.Types.BINARY:
                    case java.sql.Types.VARBINARY:
                    case java.sql.Types.LONGVARBINARY:
                        targetStatement.setBytes(i, resultSet.getBytes(i));
                        break;
                    // Handle more exotic types like arrays, structured types, and others
                    case java.sql.Types.ARRAY:
                        targetStatement.setArray(i, resultSet.getArray(i));
                        break;
                    case java.sql.Types.BLOB:
                        targetStatement.setBlob(i, resultSet.getBlob(i));
                        break;
                    case java.sql.Types.CLOB:
                        targetStatement.setClob(i, resultSet.getClob(i));
                        break;
                    // Default to setObject for types not explicitly handled above
                    default:
                        targetStatement.setObject(i, resultSet.getObject(i));
                        break;
                }
            }
            targetStatement.executeUpdate();
        }

    }
}
