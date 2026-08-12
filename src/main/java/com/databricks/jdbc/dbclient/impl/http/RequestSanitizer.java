package com.databricks.jdbc.dbclient.impl.http;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.http.client.methods.HttpUriRequest;

public class RequestSanitizer {
  // Signature/credential params in AWS, Azure (sig) and GCS presigned URLs.
  private static final Set<String> SENSITIVE_QUERY_PARAMS =
      new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

  static {
    SENSITIVE_QUERY_PARAMS.addAll(
        List.of(
            "X-Amz-Security-Token",
            "X-Amz-Signature",
            "X-Amz-Credential",
            "sig",
            "X-Goog-Signature",
            "X-Goog-Credential"));
  }

  public static String sanitizeRequest(HttpUriRequest request) {
    try {
      URI uri = new URI(request.getURI().toString());
      String sanitizedQuery = sanitizeQuery(uri.getRawQuery());
      URI sanitizedUri =
          new URI(
              uri.getScheme(),
              uri.getAuthority(),
              uri.getPath(),
              sanitizedQuery,
              uri.getFragment());
      return sanitizedUri.toString();
    } catch (URISyntaxException e) {
      return "Error sanitizing URI: " + e.getMessage();
    }
  }

  private static String sanitizeQuery(String query) {
    if (query == null) {
      return null;
    }

    StringBuilder sanitizedQuery = new StringBuilder();
    String[] pairs = query.split("&");
    for (String pair : pairs) {
      int idx = pair.indexOf("=");
      String key = (idx > 0) ? pair.substring(0, idx) : pair;
      if (SENSITIVE_QUERY_PARAMS.contains(key)) {
        sanitizedQuery.append(key).append("=REDACTED");
      } else {
        sanitizedQuery.append(pair);
      }
      sanitizedQuery.append("&");
    }

    // Remove the trailing '&' if present
    if (sanitizedQuery.length() > 0) {
      sanitizedQuery.setLength(sanitizedQuery.length() - 1);
    }

    return sanitizedQuery.toString();
  }
}
