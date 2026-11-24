import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.internal.fasterxml.jackson.databind.JsonNode;
import com.databricks.internal.fasterxml.jackson.databind.ObjectMapper;
import com.databricks.internal.fasterxml.jackson.databind.node.ObjectNode;
import com.databricks.internal.sdk.core.ApiClient;
import com.databricks.internal.sdk.core.http.HttpClient;
import com.databricks.internal.sdk.core.http.Request;
import com.databricks.internal.sdk.core.http.Response;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class ApcAc27Wire {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final List<ObjectNode> POLLS = new ArrayList<>();
  private static int running;
  private static int terminal;
  private static long runningPayloadBytes;

  private static String sha256(byte[] bytes) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
    StringBuilder hex = new StringBuilder();
    for (byte b : digest) hex.append(String.format("%02x", b & 0xff));
    return hex.toString();
  }

  private static void redact(JsonNode node) {
    if (node.isObject()) {
      ObjectNode object = (ObjectNode) node;
      List<String> names = new ArrayList<>();
      object.fieldNames().forEachRemaining(names::add);
      for (String name : names) {
        if (name.equals("statement_id")
            || name.equals("session_id")
            || name.equals("cluster_id")
            || name.equals("warehouse_id")
            || name.equals("id")
            || name.equals("request_id")) {
          object.put(name, "<redacted>");
        } else {
          redact(object.get(name));
        }
      }
    } else if (node.isArray()) {
      for (JsonNode item : node) redact(item);
    }
  }

  private static long resultPayloadBytes(JsonNode response) throws Exception {
    JsonNode result = response.path("result");
    long bytes = 0;
    for (String key :
        new String[] {"attachment", "arrow_batches", "columnar_data", "external_links"}) {
      JsonNode value = result.get(key);
      if (value != null && !value.isNull()) {
        bytes += JSON.writeValueAsBytes(value).length;
      }
    }
    return bytes;
  }

  public static void main(String[] args) throws Exception {
    String host = System.getenv("DATABRICKS_HOST").replaceFirst("^https?://", "");
    String path = System.getenv("DATABRICKS_HTTP_PATH");
    String url =
        "jdbc:databricks://"
            + host
            + ":443/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + path
            + ";UseThriftClient=0;TelemetryLogLevel=OFF;EnableSQLExecDirectResults=0;PollInterval=1";
    Properties properties = new Properties();
    properties.setProperty("UID", "token");
    properties.setProperty("PWD", System.getenv("DATABRICKS_TOKEN"));
    Class.forName("com.databricks.client.jdbc.Driver");
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
                Response response = delegate.execute(request);
                if (!Request.GET.equals(request.getMethod())
                    || !request.getUrl().contains("/sql/statements/")
                    || request.getUrl().contains("/result/chunks/")) return response;
                String targetPath = request.getUri().getPath();
                if (!targetPath.matches("/api/2\\.0/sql/statements/[^/]+")) {
                  throw new java.io.IOException("Unexpected polling path shape");
                }
                byte[] bytes = response.getBody().readAllBytes();
                JsonNode body = JSON.readTree(bytes);
                String state = body.path("status").path("state").asText();
                long payloadBytes;
                try {
                  payloadBytes = resultPayloadBytes(body);
                } catch (Exception error) {
                  throw new java.io.IOException(error);
                }
                ObjectNode record = JSON.createObjectNode();
                record.put("request_method", request.getMethod());
                record.put("request_path", targetPath.replaceAll("[^/]+$", "<redacted>"));
                record.put(
                    "request_body_bytes",
                    request.getBodyString() == null
                        ? 0
                        : request.getBodyString().getBytes(StandardCharsets.UTF_8).length);
                record.put(
                    "request_query_contains_backend_compute",
                    request.getUrl().contains("backend_compute"));
                record.put("response_status", response.getStatusCode());
                record.put("response_state", state);
                record.put("response_body_bytes", bytes.length);
                try {
                  record.put("response_sha256", sha256(bytes));
                } catch (Exception error) {
                  throw new java.io.IOException(error);
                }
                record.put("result_payload_bytes", payloadBytes);
                if (state.equals("RUNNING")) {
                  running++;
                  runningPayloadBytes += payloadBytes;
                  JsonNode sanitized = body.deepCopy();
                  redact(sanitized);
                  record.set("response_body", sanitized);
                } else if (!state.equals("PENDING")) {
                  terminal++;
                }
                POLLS.add(record);
                return new Response(
                    response.getRequest(),
                    response.getUrl(),
                    response.getStatusCode(),
                    response.getStatus(),
                    response.getAllHeaders(),
                    new ByteArrayInputStream(bytes));
              });
      long expected = 100000;
      String query =
          "SELECT id, substring(repeat(sha2(CAST(id AS STRING), 256), 8), 1, 128) AS payload, delay"
              + " FROM range(0, "
              + expected
              + ") CROSS JOIN (SELECT sleep("
              + args[0]
              + ") AS delay) ORDER BY id";
      long rows = 0;
      long checksum = 0;
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(query)) {
        while (result.next()) {
          long id = result.getLong(1);
          if (id != rows || result.getString(2).length() != 128)
            throw new AssertionError("Result mismatch at row " + rows);
          checksum += id;
          rows++;
        }
      }
      if (rows != expected || checksum != expected * (expected - 1) / 2)
        throw new AssertionError("Count/checksum mismatch");
      System.out.printf(
          "running_polls=%d terminal_polls=%d running_payload_bytes=%d rows=%d checksum=%d%n",
          running, terminal, runningPayloadBytes, rows, checksum);
      if (running < 3 || runningPayloadBytes != 0 || terminal < 1)
        throw new AssertionError("AC27 release gate failed");
      ObjectNode output = JSON.createObjectNode();
      output.put("running_polls", running);
      output.put("terminal_polls", terminal);
      output.put("running_result_payload_bytes", runningPayloadBytes);
      output.put("rows", rows);
      output.put("checksum", checksum);
      output.set("polls", JSON.valueToTree(POLLS));
      System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(output));
    }
  }
}
