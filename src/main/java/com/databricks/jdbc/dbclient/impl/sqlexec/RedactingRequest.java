package com.databricks.jdbc.dbclient.impl.sqlexec;

import com.databricks.sdk.core.http.Request;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;

/** Keeps the wire request intact while hiding APC targets and statement IDs from SDK logs. */
class RedactingRequest extends Request {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String CLUSTER_QUERY =
      "backend_compute.all_purpose_cluster_compute.cluster_id=";
  private static final String STATEMENT_PATH = "/api/2.0/sql/statements/";
  private volatile boolean wireBodyVisible = true;
  private final String logBody;

  RedactingRequest(String method, String url, String body) {
    super(method, url, body);
    this.logBody = redactBody(body);
  }

  RedactingRequest(Request request) {
    this(request.getMethod(), request.getUrl(), request.getBodyString());
    withHeaders(request.getHeaders());
    request
        .getQuery()
        .forEach((key, values) -> values.forEach(value -> withQueryParam(key, value)));
    request.getRedirectionBehavior().ifPresent(this::setRedirectionBehavior);
  }

  void showWireBody() {
    wireBodyVisible = true;
  }

  void hideWireBody() {
    wireBodyVisible = false;
  }

  @Override
  public String getBodyString() {
    return wireBodyVisible ? super.getBodyString() : logBody;
  }

  @Override
  public String getRequestLine() {
    URI uri = getUri();
    String path = uri.getRawPath();
    if (path.startsWith(STATEMENT_PATH) && path.length() > STATEMENT_PATH.length()) {
      int idEnd = path.indexOf('/', STATEMENT_PATH.length());
      path = STATEMENT_PATH + "[REDACTED]" + (idEnd < 0 ? "" : path.substring(idEnd));
    }
    String query = uri.getRawQuery();
    if (query == null) {
      return getMethod() + " " + path;
    }
    StringBuilder redacted = new StringBuilder();
    for (String entry : query.split("&", -1)) {
      if (redacted.length() > 0) {
        redacted.append('&');
      }
      redacted.append(entry.startsWith(CLUSTER_QUERY) ? CLUSTER_QUERY + "[REDACTED]" : entry);
    }
    return getMethod() + " " + path + "?" + redacted;
  }

  private static String redactBody(String body) {
    if (body == null) {
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(body);
      JsonNode cluster = root.path("backend_compute").path("all_purpose_cluster_compute");
      if (cluster instanceof ObjectNode) {
        ((ObjectNode) cluster).put("cluster_id", "[REDACTED]");
        if (root instanceof ObjectNode) {
          ObjectNode request = (ObjectNode) root;
          if (request.has("statementId")) {
            request.put("statementId", "[REDACTED]");
          }
          if (request.has("statement_id")) {
            request.put("statement_id", "[REDACTED]");
          }
          if (request.has("statement")) {
            request.put("statement", "[REDACTED]");
            if (request.has("parameters")) {
              request.put("parameters", "[REDACTED]");
            }
            if (request.has("parameter_sets")) {
              request.put("parameter_sets", "[REDACTED]");
            }
          }
        }
      }
      return MAPPER.writeValueAsString(root);
    } catch (Exception e) {
      return "[REDACTED]";
    }
  }
}
