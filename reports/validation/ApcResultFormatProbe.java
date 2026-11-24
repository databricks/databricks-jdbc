import com.databricks.internal.fasterxml.jackson.databind.JsonNode;
import com.databricks.internal.fasterxml.jackson.databind.ObjectMapper;
import com.databricks.internal.sdk.core.ApiClient;
import com.databricks.internal.sdk.core.http.HttpClient;
import com.databricks.internal.sdk.core.http.Response;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/** Captures result-format names without retaining request or response identifiers. */
public final class ApcResultFormatProbe {
  private static final ObjectMapper JSON = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";UseThriftClient=0;TelemetryLogLevel=OFF";
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", System.getenv("DATABRICKS_TOKEN"));
    Class.forName("com.databricks.client.jdbc.Driver");
    AtomicReference<String> requestFormat = new AtomicReference<>("missing");
    AtomicReference<String> manifestFormat = new AtomicReference<>("missing");

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
                boolean statementRequest = request.getUrl().contains("/api/2.0/sql/statements");
                if (statementRequest && request.getBodyString() != null) {
                  JsonNode requestBody = JSON.readTree(request.getBodyString());
                  if (requestBody.has("format")) {
                    requestFormat.set(requestBody.path("format").asText());
                  }
                }
                Response response = delegate.execute(request);
                if (!statementRequest || response.getBody() == null) {
                  return response;
                }
                byte[] bytes = response.getBody().readAllBytes();
                JsonNode responseBody = JSON.readTree(bytes);
                if (responseBody.path("manifest").has("format")) {
                  manifestFormat.set(responseBody.path("manifest").path("format").asText());
                }
                return new Response(
                    response.getRequest(),
                    response.getUrl(),
                    response.getStatusCode(),
                    response.getStatus(),
                    response.getAllHeaders(),
                    new ByteArrayInputStream(bytes));
              });

      int rows = 0;
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery("SELECT 1")) {
        while (result.next()) {
          rows++;
        }
      }
      boolean passed =
          rows == 1
              && "ARROW_BATCH".equals(requestFormat.get())
              && "ARROW_BATCH".equals(manifestFormat.get());
      System.out.printf(
          "query_kind=non_metadata request_format=%s manifest_format=%s"
              + " rows=%d outcome=%s%n",
          requestFormat.get(), manifestFormat.get(), rows, passed ? "pass" : "fail");
      if (!passed) {
        throw new AssertionError("APC result-format probe failed");
      }
    }
  }
}
