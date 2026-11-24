import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.databricks.internal.sdk.core.DatabricksError;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;

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
      case "sea-explicit-pattern":
        options = ";UseThriftClient=0;TreatMetadataCatalogNameAsPattern=1";
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
    String orgId = System.getenv("DATABRICKS_ORG_ID");
    if (orgId != null) {
      properties.setProperty("http.header.x-databricks-org-id", orgId);
    }
    DriverManager.setLoginTimeout(90);
    Class.forName("com.databricks.client.jdbc.Driver");
    try (Connection connection = DriverManager.getConnection(url, properties)) {
      DatabricksConnection dbConnection = (DatabricksConnection) connection;
      DatabricksConnectionContext context =
          (DatabricksConnectionContext) dbConnection.getConnectionContext();
      Object proxy = dbConnection.getSession().getDatabricksClient();
      InvocationHandler handler = Proxy.getInvocationHandler(proxy);
      Field target = handler.getClass().getDeclaredField("target");
      target.setAccessible(true);
      String client = target.get(handler).getClass().getSimpleName();
      String protocol =
          client.equals("DatabricksSdkClient")
              ? context.isAllPurposeCluster() ? "SEA_APC" : "SEA_WAREHOUSE"
              : client.equals("DatabricksThriftServiceClient") ? "THRIFT" : "OTHER";
      boolean apcFlag =
          DatabricksDriverFeatureFlagsContextFactory.getInstance(context)
              .getBoolean(
                  "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbcApc");
      boolean warehouseFlag =
          DatabricksDriverFeatureFlagsContextFactory.getInstance(context)
              .getBoolean(
                  "databricks.partnerplatform.clientConfigsFeatureFlags.enableSqlExecForJdbc");
      String selectOne;
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT 1 AS validation_value")) {
        boolean passed = result.next() && result.getInt(1) == 1 && !result.next();
        selectOne = passed ? "pass" : "mismatch";
      } catch (SQLException e) {
        selectOne = "fail sqlstate=" + e.getSQLState() + " vendor=" + e.getErrorCode();
      }
      System.out.printf(
          "mode=%s protocol=%s client=%s route_reason=%s apc_flag=%s warehouse_flag=%s"
              + " select_one=%s%n",
          mode,
          protocol,
          client,
          readRoutingReason(context),
          apcFlag,
          warehouseFlag,
          selectOne);
    } catch (SQLException error) {
      String message = error.getMessage() == null ? "" : error.getMessage();
      Throwable root = error;
      while (root.getCause() != null) {
        root = root.getCause();
      }
      int sdkStatus = root instanceof DatabricksError ? ((DatabricksError) root).getStatusCode() : 0;
      String sdkErrorCode =
          root instanceof DatabricksError ? ((DatabricksError) root).getErrorCode() : "none";
      String detail = sanitize(root.getMessage());
      String status =
          message.contains("401")
              ? "401"
              : message.contains("403")
                  ? "403"
                  : message.contains("404")
                      ? "404"
                      : message.contains("503") ? "503" : "unknown";
      String category =
          message.contains("UnknownHost")
              ? "unknown_host"
              : message.contains("SSL")
                  ? "ssl"
                  : message.contains("timed out") ? "timeout" : "other";
      System.out.printf(
          "mode=%s connection=fail class=%s root_class=%s sqlstate=%s"
              + " vendor=%d http_status=%s sdk_status=%d sdk_error=%s category=%s detail=%s%n",
          mode,
          error.getClass().getSimpleName(),
          root.getClass().getSimpleName(),
          error.getSQLState(),
          error.getErrorCode(),
          status,
          sdkStatus,
          sdkErrorCode,
          category,
          detail);
    }
  }

  private static String readRoutingReason(DatabricksConnectionContext context) {
    try {
      Field field = DatabricksConnectionContext.class.getDeclaredField("routingReason");
      field.setAccessible(true);
      return (String) field.get(context);
    } catch (ReflectiveOperationException error) {
      return "not_exposed";
    }
  }

  private static String sanitize(String message) {
    String sanitized = message == null ? "" : message.replaceAll("[\\r\\n]+", " ");
    for (String variable :
        new String[] {"DATABRICKS_TOKEN", "DATABRICKS_ORG_ID", "DATABRICKS_HTTP_PATH"}) {
      String value = System.getenv(variable);
      if (value != null && !value.isEmpty()) {
        sanitized = sanitized.replace(value, "<redacted>");
      }
    }
    return sanitized
        .replaceAll(
            "(?i)\\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\b",
            "<uuid>")
        .replaceAll("\\b[0-9]{8,}\\b", "<numeric-id>");
  }
}
