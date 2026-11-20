package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class RedactingRequestTest {
  @Test
  void sdkDebugLogHidesApcTargetWhileTransportReceivesIt() throws Exception {
    String cluster = "secret-cluster-918273";
    String body =
        "{\"backend_compute\":{\"all_purpose_cluster_compute\":{\"cluster_id\":\""
            + cluster
            + "\"}}}";
    List<String> wireBodies = new ArrayList<>();
    List<String> wireUrls = new ArrayList<>();
    ApiClient client =
        DatabricksSdkClient.buildApiClient(
            new DatabricksConfig()
                .setHost("https://example.com")
                .setAuthType("pat")
                .setToken("test-token")
                .setHttpClient(
                    request -> {
                      if (request.getUri().getPath().startsWith("/api/2.0/sql/")) {
                        wireBodies.add(request.getBodyString());
                        wireUrls.add(request.getUri().toString());
                      }
                      return new Response(request, 204, "No Content", Map.of());
                    }),
            null,
            true);
    Logger logger = Logger.getLogger(ApiClient.class.getName());
    Level previousLevel = logger.getLevel();
    StringBuilder logged = new StringBuilder();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            logged.append(record.getMessage()).append('\n');
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    handler.setLevel(Level.ALL);
    logger.setLevel(Level.FINE);
    logger.addHandler(handler);
    try {
      client.execute(
          new RedactingRequest(Request.POST, "/api/2.0/sql/sessions/", body), Void.class);
      client.execute(
          new RedactingRequest(Request.POST, "/api/2.0/sql/statements/", body), Void.class);
      client.execute(
          new RedactingRequest(Request.POST, "/api/2.0/sql/statements/id/cancel", body),
          Void.class);
      client.execute(
          new RedactingRequest(Request.DELETE, "/api/2.0/sql/sessions/id", body), Void.class);
      client.execute(
          new RedactingRequest(
              Request.GET,
              "/api/2.0/sql/statements/id?backend_compute.all_purpose_cluster_compute.cluster_id="
                  + cluster,
              null),
          Void.class);
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
    }
    assertEquals(5, wireBodies.size());
    for (int i = 0; i < 4; i++) {
      assertEquals(body, wireBodies.get(i));
    }
    assertTrue(wireUrls.get(4).contains(cluster));
    assertTrue(logged.toString().contains("[REDACTED]"));
    assertFalse(logged.toString().contains(cluster));
  }
}
