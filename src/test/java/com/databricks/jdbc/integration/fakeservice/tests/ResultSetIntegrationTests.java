package com.databricks.jdbc.integration.fakeservice.tests;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.integration.fakeservice.AbstractFakeServiceIntegrationTests;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

/** Integration tests for ResultSet operations. */
public class ResultSetIntegrationTests extends AbstractFakeServiceIntegrationTests {

  @Test
  void testRetrievalOfBasicDataTypes() throws SQLException {
    String tableName = "basic_data_types_table_jay";
    setupDatabaseTable(tableName);
    String insertSQL =
        "INSERT INTO "
            + getFullyQualifiedTableName(tableName)
            + " (id, col1, col2) VALUES (1, 'value1', 'value2')";
    executeSQL(insertSQL);

    String query = "SELECT id, col1 FROM " + getFullyQualifiedTableName(tableName);
    ResultSet resultSet = executeQuery(query);

    while (resultSet.next()) {
      assertEquals(1, resultSet.getInt("id"), "ID should be of type Integer and value 1");
      assertEquals(
          "value1", resultSet.getString("col1"), "col1 should be of type String and value value1");
    }
    deleteTable(tableName);
  }
}
