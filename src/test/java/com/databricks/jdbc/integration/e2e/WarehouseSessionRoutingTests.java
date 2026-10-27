package com.databricks.jdbc.integration.e2e;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksStatement;
import com.databricks.sdk.service.sql.StatementState;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.BitSet;
import java.util.Properties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class WarehouseSessionRoutingTests {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testHybridResultsAndSessionReuse(boolean async) throws Exception {
    String expectedRoute = requiredEnv("DATABRICKS_EXPECT_REALTIME_SESSION");
    assertTrue(expectedRoute.equals("true") || expectedRoute.equals("false"));
    boolean realtime = Boolean.parseBoolean(expectedRoute);
    Properties properties = new Properties();
    properties.setProperty("Auth_AccessToken", requiredEnv("DATABRICKS_TOKEN"));
    String url =
        "jdbc:databricks://"
            + requiredEnv("DATABRICKS_HOST")
            + ":443/default;ssl=1;AuthMech=11;Auth_Flow=0;UseThriftClient=0;"
            + "httpPath="
            + requiredEnv("DATABRICKS_HTTP_PATH")
            + ";ConnCatalog=main;ConnSchema=default;EnableTelemetry=0";
    try (Connection connection = DriverManager.getConnection(url, properties)) {
      DatabricksConnection internal = connection.unwrap(DatabricksConnection.class);
      assertTrue(internal.getConnectionContext().isSqlExecHybridResultsEnabled());
      assertEquals(realtime, internal.getSession().getSessionInfo().realtimeSession());
      try (Statement statement = connection.createStatement()) {
        statement.setQueryTimeout(180);
        String sql = "SELECT id, repeat('x', 1024) AS payload FROM range(25600)";
        ResultSet result;
        if (async) {
          IDatabricksStatement asynchronous = statement.unwrap(IDatabricksStatement.class);
          result = asynchronous.executeAsync(sql);
          long deadline = System.nanoTime() + Duration.ofSeconds(180).toNanos();
          while (true) {
            StatementState state =
                result.unwrap(IDatabricksResultSet.class).getStatementStatus().getState();
            if (state != StatementState.PENDING && state != StatementState.RUNNING) {
              assertEquals(StatementState.SUCCEEDED, state);
              break;
            }
            assertTrue(System.nanoTime() < deadline, "Async statement did not finish in 180s");
            Thread.sleep(100);
            result = asynchronous.getExecutionResult();
          }
        } else {
          result = statement.executeQuery(sql);
        }
        assertEquals(
            realtime,
            statement.unwrap(DatabricksStatement.class).getStatementId().sessionId() != null);
        try (ResultSet rows = result) {
          BitSet seen = new BitSet(25600);
          String expectedPayload = "x".repeat(1024);
          while (rows.next()) {
            int id = rows.getInt(1);
            assertTrue(id >= 0 && id < 25600);
            assertFalse(seen.get(id), "Duplicate row " + id);
            seen.set(id);
            assertEquals(expectedPayload, rows.getString(2));
          }
          assertEquals(25600, seen.cardinality());
        }
        try (ResultSet reused = statement.executeQuery("SELECT 1")) {
          assertTrue(reused.next());
          assertEquals(1, reused.getInt(1));
          assertFalse(reused.next());
        }
      }
      assertEquals(realtime, internal.getSession().getSessionInfo().realtimeSession());
    }
  }

  private static String requiredEnv(String name) {
    String value = System.getenv(name);
    assertNotNull(value, "Set " + name);
    assertFalse(value.isEmpty(), "Set " + name);
    return value;
  }
}
