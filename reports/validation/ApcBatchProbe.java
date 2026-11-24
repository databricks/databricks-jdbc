import com.databricks.jdbc.api.impl.DatabricksArray;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

/** Scratch-schema batch and complex-parameter probe with unconditional cleanup. */
public final class ApcBatchProbe {
  private static Connection connect(String mode, String extraOptions) throws Exception {
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";TelemetryLogLevel=OFF;UseThriftClient="
            + mode
            + extraOptions;
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", System.getenv("DATABRICKS_TOKEN"));
    return DriverManager.getConnection(url, properties);
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static long count(Connection connection, String table) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
      result.next();
      return result.getLong(1);
    }
  }

  public static void main(String[] args) throws Exception {
    Class.forName("com.databricks.client.jdbc.Driver");
    String schema = "main.apc_sea_validation_" + System.currentTimeMillis();
    String table = schema + ".batch_probe";
    boolean schemaCreated = false;
    try (Connection thrift = connect("1", "")) {
      execute(thrift, "CREATE SCHEMA " + schema);
      schemaCreated = true;
      execute(thrift, "CREATE TABLE " + table + " (id INT, value STRING)");

      try (Connection sea = connect("0", ";EnableNativeBatching=1")) {
        int[] nativeCounts;
        try (PreparedStatement statement =
            sea.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)")) {
          for (int id = 1; id <= 3; id++) {
            statement.setInt(1, id);
            statement.setString(2, "value-" + id);
            statement.addBatch();
          }
          nativeCounts = statement.executeBatch();
        }
        long nativeRows = count(sea, table);
        System.out.printf(
            "native_batch=pass count_entries=%d rows=%d%n", nativeCounts.length, nativeRows);

        boolean stoppedAfterFailure = false;
        try (Statement statement = sea.createStatement()) {
          statement.addBatch("INSERT INTO " + table + " VALUES (10, 'first')");
          statement.addBatch("INSERT INTO " + table + " VALUES (CAST('bad' AS INT), 'bad')");
          statement.addBatch("INSERT INTO " + table + " VALUES (12, 'third')");
          try {
            statement.executeBatch();
          } catch (BatchUpdateException expected) {
            try (ResultSet result =
                statement.executeQuery(
                    "SELECT SUM(CASE WHEN id = 10 THEN 1 ELSE 0 END),"
                        + " SUM(CASE WHEN id = 12 THEN 1 ELSE 0 END) FROM "
                        + table)) {
              result.next();
              stoppedAfterFailure = result.getLong(1) == 1 && result.getLong(2) == 0;
            }
          }
        }
        System.out.printf("statement_batch_stop=%s%n", stoppedAfterFailure ? "pass" : "fail");

        try (PreparedStatement statement = sea.prepareStatement("SELECT ?")) {
          statement.setObject(1, new DatabricksArray(Arrays.asList(1, 2), "ARRAY<INT>"));
          statement.executeQuery();
          System.out.println("complex_parameter=fail:no_exception");
        } catch (SQLException expected) {
          System.out.printf(
              "complex_parameter=exception class=%s sqlstate=%s vendor=%d%n",
              expected.getClass().getSimpleName(),
              expected.getSQLState(),
              expected.getErrorCode());
        }
      }
    } catch (SQLException error) {
      System.out.printf(
          "batch_probe=blocked class=%s sqlstate=%s vendor=%d%n",
          error.getClass().getSimpleName(), error.getSQLState(), error.getErrorCode());
    } finally {
      if (schemaCreated) {
        try (Connection thrift = connect("1", "")) {
          execute(thrift, "DROP SCHEMA " + schema + " CASCADE");
          System.out.println("cleanup=pass");
        } catch (SQLException error) {
          System.out.printf(
              "cleanup=fail class=%s sqlstate=%s vendor=%d%n",
              error.getClass().getSimpleName(), error.getSQLState(), error.getErrorCode());
        }
      }
    }
  }
}
