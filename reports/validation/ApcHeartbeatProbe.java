import com.databricks.internal.sdk.core.ApiClient;
import com.databricks.internal.sdk.core.http.HttpClient;
import com.databricks.internal.sdk.core.http.Request;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/** Verifies that completed APC result sets do not schedule heartbeat requests. */
public final class ApcHeartbeatProbe {
  public static void main(String[] args) throws Exception {
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";UseThriftClient=0;TelemetryLogLevel=OFF;EnableHeartbeat=1"
            + ";HeartbeatIntervalSeconds=10";
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", System.getenv("DATABRICKS_TOKEN"));
    Class.forName("com.databricks.client.jdbc.Driver");
    AtomicInteger idleStatementGets = new AtomicInteger();

    try (Connection connection = DriverManager.getConnection(url, properties)) {
      Object proxy = ((DatabricksConnection) connection).getSession().getDatabricksClient();
      InvocationHandler handler = Proxy.getInvocationHandler(proxy);
      Field target = handler.getClass().getDeclaredField("target");
      target.setAccessible(true);
      DatabricksSdkClient client = (DatabricksSdkClient) target.get(handler);
      Field apiField = DatabricksSdkClient.class.getDeclaredField("apiClient");
      apiField.setAccessible(true);
      ApiClient apiClient = (ApiClient) apiField.get(client);
      Field transportField = ApiClient.class.getDeclaredField("httpClient");
      transportField.setAccessible(true);
      HttpClient delegate = apiClient.getHttpClient();
      transportField.set(
          apiClient,
          (HttpClient)
              request -> {
                if (Request.GET.equals(request.getMethod())
                    && request.getUrl().contains("/sql/statements/")) {
                  idleStatementGets.incrementAndGet();
                }
                return delegate.execute(request);
              });

      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT 1")) {
        if (!result.next() || result.getInt(1) != 1) {
          throw new AssertionError("SELECT 1 mismatch");
        }
        idleStatementGets.set(0);
        Thread.sleep(35000);
        System.out.printf(
            "heartbeat_idle_seconds=35 statement_gets=%d outcome=%s%n",
            idleStatementGets.get(), idleStatementGets.get() == 0 ? "pass" : "fail");
        if (idleStatementGets.get() != 0) {
          throw new AssertionError("APC result set scheduled heartbeat requests");
        }
      }
    }
  }
}
