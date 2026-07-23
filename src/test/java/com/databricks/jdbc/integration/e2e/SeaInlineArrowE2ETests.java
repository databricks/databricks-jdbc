package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getValidJDBCConnection;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.arrow.ChunkProvider;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Live end-to-end tests for bounded SEA inline Arrow results. */
@Tag("e2e")
public class SeaInlineArrowE2ETests {
  private static final long EXPECTED_ROWS = 204_800;
  private static final int PAYLOAD_BYTES = 1_024;

  @Test
  void test200MbMultiChunkResult() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("UseThriftClient", "0");
    properties.setProperty("UseBoundedSeaApi", "1");
    properties.setProperty("EnableQueryResultDownload", "0");
    properties.setProperty("EnableSQLExecDirectResults", "0");

    String query =
        "SELECT id, substring(repeat(sha2(CAST(id AS STRING), 256), 16), 1, "
            + PAYLOAD_BYTES
            + ") AS payload FROM range(0, "
            + EXPECTED_ROWS
            + ") ORDER BY id";

    try (Connection connection = getValidJDBCConnection(properties);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      DatabricksResultSet databricksResultSet = (DatabricksResultSet) resultSet;
      ChunkProvider provider =
          databricksResultSet
              .getChunkProvider()
              .orElseThrow(() -> new AssertionError("Expected an Arrow chunk provider"));
      assertEquals("SeaInlineArrowChunkProvider", provider.getClass().getSimpleName());

      long rowCount = 0;
      long checksum = 0;
      while (resultSet.next()) {
        long id = resultSet.getLong(1);
        assertEquals(rowCount, id, "Unexpected id at row " + rowCount);
        assertEquals(PAYLOAD_BYTES, resultSet.getString(2).length());
        checksum += id;
        rowCount++;
      }

      assertEquals(EXPECTED_ROWS, rowCount);
      assertEquals(EXPECTED_ROWS * (EXPECTED_ROWS - 1) / 2, checksum);
      assertTrue(provider.getChunkCount() > 1, "Expected a multi-chunk inline result");
    }
  }
}
