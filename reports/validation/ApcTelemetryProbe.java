import com.databricks.jdbc.api.impl.DatabricksConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/** Checks public connection/session diagnostics without printing identifiers or credentials. */
public final class ApcTelemetryProbe {
  public static void main(String[] args) throws Exception {
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String token = System.getenv("DATABRICKS_TOKEN");
    String[] segments = path.split("/");
    String workspaceId = segments.length > 4 ? segments[4] : "";
    String clusterId = segments.length > 5 ? segments[5] : "";
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";UseThriftClient=0;TelemetryLogLevel=INFO";
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", token);
    Class.forName("com.databricks.client.jdbc.Driver");

    try (Connection connection = DriverManager.getConnection(url, properties)) {
      DatabricksConnection dbConnection = (DatabricksConnection) connection;
      String connectionDiagnostics = connection.toString();
      String sessionDiagnostics = dbConnection.getSession().toString();
      String contextDiagnostics = dbConnection.getSession().getConnectionContext().toString();
      String diagnostics = connectionDiagnostics + sessionDiagnostics + contextDiagnostics;
      boolean containsToken = !token.isEmpty() && diagnostics.contains(token);
      boolean containsWorkspace = !workspaceId.isEmpty() && diagnostics.contains(workspaceId);
      boolean containsCluster = !clusterId.isEmpty() && diagnostics.contains(clusterId);
      boolean selectOne;
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT 1")) {
        selectOne = result.next() && result.getInt(1) == 1 && !result.next();
      }
      System.out.printf(
          "diagnostics_token=%s diagnostics_workspace=%s diagnostics_cluster=%s"
              + " workspace_in_connection=%s workspace_in_session=%s workspace_in_context=%s"
              + " select_one=%s%n",
          containsToken,
          containsWorkspace,
          containsCluster,
          connectionDiagnostics.contains(workspaceId),
          sessionDiagnostics.contains(workspaceId),
          contextDiagnostics.contains(workspaceId),
          selectOne ? "pass" : "fail");
    }
  }
}
