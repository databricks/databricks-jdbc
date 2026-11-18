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

public final class ApcRound2Routes {
  public static void main(String[] args) throws Exception {
    String mode = args[0];
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String options =
        mode.equals("absent")
            ? ""
            : ";UseThriftClient=0;EnableQueryResultDownload=0;UseBoundedSeaApi=" + mode;
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
      boolean cloudFetch = dbConnection.getSession().getConnectionContext().isCloudFetchEnabled();
      boolean bounded = dbConnection.getSession().getConnectionContext().isBoundedSeaApiEnabled();
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT 1 AS validation_value")) {
        boolean passed = result.next() && result.getInt(1) == 1 && !result.next();
        if (!passed) throw new AssertionError("SELECT 1 mismatch");
      }
      System.out.printf(
          "mode=%s protocol=%s client=%s cloud_fetch=%s bounded=%s select_one=pass%n",
          mode, protocol, client, cloudFetch, bounded);
      if (mode.equals("absent")
          && (!protocol.equals("THRIFT") || !client.equals("DatabricksThriftServiceClient")))
        throw new AssertionError("Absent flag selected SEA");
      if (!mode.equals("absent") && (!protocol.equals("SEA_APC") || cloudFetch))
        throw new AssertionError("AC8 route/config mismatch");
    }
  }
}
