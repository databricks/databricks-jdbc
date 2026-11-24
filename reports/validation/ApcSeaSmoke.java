import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/** Sanitized APC smoke probe that never prints identifiers or credentials. */
public final class ApcSeaSmoke {
  public static void main(String[] args) throws Exception {
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";TelemetryLogLevel=OFF;UseThriftClient="
            + args[0];
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", System.getenv("DATABRICKS_TOKEN"));
    DriverManager.setLoginTimeout(90);
    Class.forName("com.databricks.client.jdbc.Driver");
    try (Connection connection = DriverManager.getConnection(url, properties)) {
      DatabricksConnection dbConnection = (DatabricksConnection) connection;
      Object proxy = dbConnection.getSession().getDatabricksClient();
      InvocationHandler handler = Proxy.getInvocationHandler(proxy);
      Field target = handler.getClass().getDeclaredField("target");
      target.setAccessible(true);
      String client = target.get(handler).getClass().getSimpleName();
      boolean sessionIdPresent = dbConnection.getSession().getSessionId() != null;
      String connectionIdOutcome;
      try {
        String connectionId = ((IDatabricksConnection) connection).getConnectionId();
        connectionIdOutcome =
            connectionId.startsWith("s|a|")
                ? "SEA_APC"
                : connectionId.startsWith("t|") ? "THRIFT" : "OTHER";
      } catch (RuntimeException error) {
        connectionIdOutcome = "error:" + error.getClass().getSimpleName();
      }
      String selectOne;
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT 1 AS validation_value")) {
        selectOne =
            result.next() && result.getInt(1) == 1 && !result.next() ? "pass" : "mismatch";
      } catch (Exception error) {
        selectOne = "error:" + error.getClass().getSimpleName();
      }
      System.out.printf(
          "mode=%s client=%s session_id_present=%s connection_id=%s select_one=%s%n",
          args[0], client, sessionIdPresent, connectionIdOutcome, selectOne);
    }
  }
}
