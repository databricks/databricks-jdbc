import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContext;

// Observes the protocol chosen for each routing mode with the SAFE flag evaluated per connection.
// Prints results instead of asserting so one run records every mode's outcome.
public final class ApcFlagOnRoutes {
  public static void main(String[] args) throws Exception {
    String mode = args[0];
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String options;
    switch (mode) {
      case "flag-default":
        options = "";
        break;
      case "thrift-override":
        options = ";UseThriftClient=1";
        break;
      case "sea-override":
        options = ";UseThriftClient=0";
        break;
      case "sea-pattern":
        options = ";TreatMetadataCatalogNameAsPattern=1";
        break;
      default:
        throw new IllegalArgumentException("unknown mode " + mode);
    }
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";TelemetryLogLevel=OFF"
            + options;
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", System.getenv("DATABRICKS_TOKEN"));
    DriverManager.setLoginTimeout(90);
    Class.forName("com.databricks.client.jdbc.Driver");
    try (Connection connection = DriverManager.getConnection(url, properties)) {
      DatabricksConnection dbConnection = (DatabricksConnection) connection;
      String id = ((IDatabricksConnection) connection).getConnectionId();
      String protocol =
          id.startsWith("s|a|") ? "SEA_APC" : id.startsWith("t|") ? "THRIFT" : "OTHER";
      Object proxy = dbConnection.getSession().getDatabricksClient();
      InvocationHandler handler = Proxy.getInvocationHandler(proxy);
      Field target = handler.getClass().getDeclaredField("target");
      target.setAccessible(true);
      String client = target.get(handler).getClass().getSimpleName();
      String selectOne;
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT 1 AS validation_value")) {
        boolean passed = result.next() && result.getInt(1) == 1 && !result.next();
        selectOne = passed ? "pass" : "mismatch";
      } catch (SQLException e) {
        selectOne = "fail sqlstate=" + e.getSQLState() + " vendor=" + e.getErrorCode();
      }
      String reason =
          ((DatabricksConnectionContext) dbConnection.getSession().getConnectionContext())
              .getRoutingReason();
      System.out.printf(
          "mode=%s protocol=%s client=%s reason=%s select_one=%s%n",
          mode, protocol, client, reason, selectOne);
    }
  }
}
