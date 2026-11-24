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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/** Captures sanitized SEA metadata wire properties for independently scoped JDBC operations. */
public final class ApcMetadataWireProbe {
  private static final ObjectMapper JSON = new ObjectMapper();

  private static final class Evidence {
    private String requestFormat = "missing";
    private String metadataOperation = "missing";
    private String syncHeader = "missing";
    private String requireNativeHeader = "missing";
    private String manifestFormat = "missing";
    private boolean attachment;
    private boolean columnarData;
    private boolean nativeMetadata;
  }

  @FunctionalInterface
  private interface MetadataCall {
    ResultSet execute() throws Exception;
  }

  public static void main(String[] args) throws Exception {
    boolean syncMetadata = args.length == 0 || Boolean.parseBoolean(args[0]);
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";UseThriftClient=0;EnableSeaSyncMetadata="
            + (syncMetadata ? "1" : "0")
            + ";EnableThriftNativeMetadata=1;TelemetryLogLevel=OFF";
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", System.getenv("DATABRICKS_TOKEN"));
    Class.forName("com.databricks.client.jdbc.Driver");

    AtomicReference<String> currentOperation = new AtomicReference<>("none");
    Map<String, Evidence> evidence = new LinkedHashMap<>();
    try (Connection connection = DriverManager.getConnection(url, properties)) {
      installCapture(connection, currentOperation, evidence);
      DatabaseMetaData metadata = connection.getMetaData();
      run(
          "catalogs",
          currentOperation,
          evidence,
          metadata::getCatalogs);
      run(
          "schemas",
          currentOperation,
          evidence,
          () -> metadata.getSchemas("system", "information_schema"));
      run(
          "tables",
          currentOperation,
          evidence,
          () -> metadata.getTables("system", "information_schema", "schemata", null));
      run(
          "columns",
          currentOperation,
          evidence,
          () -> metadata.getColumns("system", "information_schema", "schemata", "%"));
    }
  }

  private static void installCapture(
      Connection connection,
      AtomicReference<String> currentOperation,
      Map<String, Evidence> evidence)
      throws Exception {
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
              String operation = currentOperation.get();
              Evidence current = evidence.computeIfAbsent(operation, ignored -> new Evidence());
              boolean statementRequest = request.getUrl().contains("/api/2.0/sql/statements");
              if (statementRequest && request.getBodyString() != null) {
                JsonNode requestBody = JSON.readTree(request.getBodyString());
                if (requestBody.has("format")) {
                  current.requestFormat = requestBody.path("format").asText();
                }
                current.metadataOperation =
                    header(request.getHeaders(), "X-Databricks-Metadata-Operation-Type");
                current.syncHeader =
                    header(request.getHeaders(), "X-Databricks-Sea-Can-Run-Fully-Sync");
                current.requireNativeHeader =
                    header(request.getHeaders(), "X-Databricks-Require-Thrift-Native-Metadata");
              }
              Response response = delegate.execute(request);
              if (!statementRequest || response.getBody() == null) {
                return response;
              }
              byte[] bytes = response.getBody().readAllBytes();
              JsonNode responseBody = JSON.readTree(bytes);
              JsonNode format = responseBody.path("manifest").path("format");
              if (!format.isMissingNode()) {
                current.manifestFormat = format.asText();
              }
              current.attachment |= present(responseBody.findValue("attachment"));
              current.columnarData |= present(responseBody.findValue("columnar_data"));
              JsonNode nativeResult = responseBody.findValue("is_native_metadata_result");
              current.nativeMetadata |= nativeResult != null && nativeResult.asBoolean();
              return new Response(
                  response.getRequest(),
                  response.getUrl(),
                  response.getStatusCode(),
                  response.getStatus(),
                  response.getAllHeaders(),
                  new ByteArrayInputStream(bytes));
            });
  }

  private static void run(
      String operation,
      AtomicReference<String> currentOperation,
      Map<String, Evidence> evidence,
      MetadataCall call)
      throws Exception {
    currentOperation.set(operation);
    int rows = 0;
    int columns;
    try (ResultSet result = call.execute()) {
      columns = result.getMetaData().getColumnCount();
      while (result.next()) {
        rows++;
      }
    }
    Evidence current = evidence.computeIfAbsent(operation, ignored -> new Evidence());
    System.out.printf(
        "operation=%s outcome=pass rows=%d columns=%d request_format=%s"
            + " metadata_header=%s sync_header=%s require_native_header=%s"
            + " manifest_format=%s attachment=%s columnar_data=%s native_metadata=%s%n",
        operation,
        rows,
        columns,
        current.requestFormat,
        current.metadataOperation,
        current.syncHeader,
        current.requireNativeHeader,
        current.manifestFormat,
        current.attachment,
        current.columnarData,
        current.nativeMetadata);
  }

  private static String header(Map<String, String> headers, String name) {
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(name)) {
        return entry.getValue();
      }
    }
    return "missing";
  }

  private static boolean present(JsonNode node) {
    return node != null && !node.isMissingNode() && !node.isNull();
  }
}
