import com.databricks.jdbc.api.impl.DatabricksConnection;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/** Sanitized bounded-SEA routing probe that avoids connection-ID construction. */
public final class ApcBoundedRoute {
  public static void main(String[] args) throws Exception {
    String boundedOption = args[0];
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";TelemetryLogLevel=OFF;UseThriftClient=0;EnableQueryResultDownload=0"
            + ";UseBoundedSeaApi="
            + boundedOption;
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", System.getenv("DATABRICKS_TOKEN"));
    Class.forName("com.databricks.client.jdbc.Driver");

    try (Connection connection = DriverManager.getConnection(url, properties)) {
      DatabricksConnection dbConnection = (DatabricksConnection) connection;
      Object proxy = dbConnection.getSession().getDatabricksClient();
      InvocationHandler handler = Proxy.getInvocationHandler(proxy);
      Field target = handler.getClass().getDeclaredField("target");
      target.setAccessible(true);
      String client = target.get(handler).getClass().getSimpleName();
      boolean cloudFetch = dbConnection.getSession().getConnectionContext().isCloudFetchEnabled();
      boolean bounded = dbConnection.getSession().getConnectionContext().isBoundedSeaApiEnabled();
      boolean passed;
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT 1")) {
        passed = result.next() && result.getInt(1) == 1 && !result.next();
      }
      System.out.printf(
          "option=%s client=%s cloud_fetch=%s bounded=%s select_one=%s%n",
          boundedOption, client, cloudFetch, bounded, passed ? "pass" : "fail");
    }
  }
}
