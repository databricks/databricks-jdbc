import com.databricks.jdbc.api.impl.DatabricksConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Sanitized standard and Real-Time SQL warehouse lifecycle probe. */
public final class ApcWarehouseLifecycleProbe {
  public static void main(String[] args) throws Exception {
    boolean expectedRealtime = Boolean.parseBoolean(args[0]);
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";UseThriftClient=0;TelemetryLogLevel=OFF"
            + ";EnableSQLExecDirectResults=0;PollInterval=1";
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", System.getenv("DATABRICKS_TOKEN"));
    Class.forName("com.databricks.client.jdbc.Driver");

    boolean observedRealtime;
    long rows = 0;
    long checksum = 0;
    boolean cancelPassed;
    boolean reusePassed;
    try (Connection connection = DriverManager.getConnection(url, properties)) {
      DatabricksConnection internal = connection.unwrap(DatabricksConnection.class);
      observedRealtime = internal.getSession().getSessionInfo().realtimeSession();
      try (Statement statement = connection.createStatement();
          ResultSet result =
              statement.executeQuery("SELECT id FROM range(0, 100000) ORDER BY id")) {
        while (result.next()) {
          long value = result.getLong(1);
          if (value != rows) {
            throw new AssertionError("Out-of-order warehouse row");
          }
          rows++;
          checksum += value;
        }
      }

      ExecutorService executor = Executors.newSingleThreadExecutor();
      try (Statement statement = connection.createStatement()) {
        Future<?> execution =
            executor.submit(
                () -> {
                  try (ResultSet ignored = statement.executeQuery("SELECT sleep(30)")) {
                    while (ignored.next()) {
                      // Consume any result if cancellation loses the race.
                    }
                  } catch (SQLException error) {
                    throw new RuntimeException(error);
                  }
                });
        Thread.sleep(2000);
        statement.cancel();
        try {
          execution.get(20, TimeUnit.SECONDS);
          cancelPassed = false;
        } catch (ExecutionException expected) {
          cancelPassed = expected.getCause() instanceof RuntimeException;
        }
      } finally {
        executor.shutdownNow();
      }

      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT 1")) {
        reusePassed = result.next() && result.getInt(1) == 1 && !result.next();
      }
    }

    boolean passed =
        observedRealtime == expectedRealtime
            && rows == 100000
            && checksum == 4999950000L
            && cancelPassed
            && reusePassed;
    System.out.printf(
        "expected_realtime=%s observed_realtime=%s rows=%d checksum=%d"
            + " cancel=%s reuse=%s outcome=%s%n",
        expectedRealtime,
        observedRealtime,
        rows,
        checksum,
        cancelPassed ? "pass" : "fail",
        reusePassed ? "pass" : "fail",
        passed ? "pass" : "fail");
    if (!passed) {
      throw new AssertionError("Warehouse lifecycle probe failed");
    }
  }
}
