import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** Sanitized execution probes shared by APC SEA and Thrift controls. */
public final class ApcExecutionProbe {
  @FunctionalInterface
  private interface MetadataCall {
    ResultSet execute() throws SQLException;
  }

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

  private static void basic(String mode) throws Exception {
    try (Connection connection = connect(mode, "")) {
      boolean parameterPass;
      try (PreparedStatement statement =
              connection.prepareStatement("SELECT CAST(? AS BIGINT), CAST(? AS STRING)")) {
        statement.setLong(1, 42L);
        statement.setString(2, "parameter-value");
        try (ResultSet result = statement.executeQuery()) {
          parameterPass =
              result.next()
                  && result.getLong(1) == 42L
                  && "parameter-value".equals(result.getString(2))
                  && !result.next();
        }
      }

      String typeQuery =
          "SELECT CAST(true AS BOOLEAN), CAST(1 AS TINYINT), CAST(2 AS SMALLINT),"
              + " CAST(3 AS INT), CAST(4 AS BIGINT), CAST(1.5 AS FLOAT),"
              + " CAST(2.5 AS DOUBLE), CAST(12.34 AS DECIMAL(10,2)),"
              + " DATE '2026-11-23', TIMESTAMP '2026-11-23 12:34:56'";
      List<Integer> typeCodes = new ArrayList<>();
      List<String> valueClasses = new ArrayList<>();
      boolean valuesPass;
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(typeQuery)) {
        ResultSetMetaData metadata = result.getMetaData();
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
          typeCodes.add(metadata.getColumnType(column));
        }
        valuesPass =
            result.next()
                && result.getBoolean(1)
                && result.getByte(2) == 1
                && result.getShort(3) == 2
                && result.getInt(4) == 3
                && result.getLong(5) == 4
                && result.getBigDecimal(8).compareTo(new BigDecimal("12.34")) == 0;
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
          Object value = result.getObject(column);
          valueClasses.add(value == null ? "null" : value.getClass().getSimpleName());
        }
        valuesPass = valuesPass && !result.next();
      }

      boolean emptySchema;
      try (Statement statement = connection.createStatement();
          ResultSet result =
              statement.executeQuery("SELECT CAST(1 AS INT) AS empty_value WHERE false")) {
        emptySchema = result.getMetaData().getColumnCount() == 1 && !result.next();
      }

      System.out.printf(
          "mode=%s parameter=%s values=%s empty_schema=%s type_codes=%s value_classes=%s%n",
          mode,
          parameterPass ? "pass" : "fail",
          valuesPass ? "pass" : "fail",
          emptySchema ? "pass" : "fail",
          typeCodes,
          valueClasses);
    }
  }

  private static void large(String mode, boolean directResults) throws Exception {
    String options = directResults ? "" : ";EnableSQLExecDirectResults=0;PollInterval=1";
    try (Connection connection = connect(mode, options);
        Statement statement = connection.createStatement();
        ResultSet result =
            statement.executeQuery(
                "SELECT id FROM range(0, 100000) ORDER BY id")) {
      long rows = 0;
      long checksum = 0;
      while (result.next()) {
        long value = result.getLong(1);
        if (value != rows) {
          throw new AssertionError("Out-of-order row at " + rows);
        }
        rows++;
        checksum += value;
      }
      boolean passed = rows == 100000 && checksum == 4999950000L;
      System.out.printf(
          "mode=%s direct_results=%s rows=%d checksum=%d outcome=%s%n",
          mode, directResults, rows, checksum, passed ? "pass" : "fail");
    }
  }

  private static void timeout(String mode) throws Exception {
    try (Connection connection = connect(mode, "");
        Statement statement = connection.createStatement()) {
      statement.setQueryTimeout(1);
      try {
        statement.executeQuery("SELECT sleep(3)");
        System.out.printf("mode=%s timeout=fail:no_exception%n", mode);
      } catch (SQLException error) {
        System.out.printf(
            "mode=%s timeout=exception class=%s sqlstate=%s vendor=%d%n",
            mode, error.getClass().getSimpleName(), error.getSQLState(), error.getErrorCode());
      }
    }
  }

  private static void metadata(String mode) throws Exception {
    try (Connection connection = connect(mode, ";EnableSeaSyncMetadata=1")) {
      DatabaseMetaData metadata = connection.getMetaData();
      metadataOperation(mode, "catalogs", metadata::getCatalogs);
      metadataOperation(
          mode, "schemas", () -> metadata.getSchemas("system", "information_schema"));
      metadataOperation(
          mode,
          "tables",
          () -> metadata.getTables("system", "information_schema", "schemata", null));
      metadataOperation(
          mode,
          "columns",
          () -> metadata.getColumns("system", "information_schema", "schemata", "%"));
    }
  }

  private static void metadataOperation(String mode, String operation, MetadataCall call) {
    try (ResultSet result = call.execute()) {
      int resultColumns = result.getMetaData().getColumnCount();
      int rows = count(result, 1000);
      System.out.printf(
          "mode=%s metadata_operation=%s outcome=pass rows=%d result_columns=%d%n",
          mode, operation, rows, resultColumns);
    } catch (SQLException error) {
      System.out.printf(
          "mode=%s metadata_operation=%s outcome=exception class=%s sqlstate=%s vendor=%d%n",
          mode,
          operation,
          error.getClass().getSimpleName(),
          error.getSQLState(),
          error.getErrorCode());
    }
  }

  private static void setStatement(String mode) throws Exception {
    try (Connection connection = connect(mode, "");
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("SET spark.sql.ansi.enabled")) {
      boolean passed = result.getMetaData().getColumnCount() > 0 && result.next();
      System.out.printf(
          "mode=%s set=pass columns=%d has_row=%s%n",
          mode, result.getMetaData().getColumnCount(), passed);
    } catch (SQLException error) {
      System.out.printf(
          "mode=%s set=exception class=%s sqlstate=%s vendor=%d%n",
          mode, error.getClass().getSimpleName(), error.getSQLState(), error.getErrorCode());
    }
  }

  private static int count(ResultSet result, int limit) throws SQLException {
    try (ResultSet rows = result) {
      int count = 0;
      while (rows.next() && count < limit) {
        count++;
      }
      return count;
    }
  }

  public static void main(String[] args) throws Exception {
    Class.forName("com.databricks.client.jdbc.Driver");
    switch (args[1]) {
      case "basic":
        basic(args[0]);
        return;
      case "large":
        large(args[0], true);
        return;
      case "large-no-direct":
        large(args[0], false);
        return;
      case "timeout":
        timeout(args[0]);
        return;
      case "metadata":
        metadata(args[0]);
        return;
      case "set":
        setStatement(args[0]);
        return;
      default:
        throw new IllegalArgumentException("Unknown scenario");
    }
  }
}
