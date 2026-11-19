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
            "AWSAccessKeyId",
            "Signature",
            "sig",
            "X-Goog-Signature",
            "X-Goog-Credential"));
  }

  public static String sanitizeRequest(HttpUriRequest request) {
    try {
      URI uri = new URI(request.getURI().toString());
      boolean presigned = isPresigned(uri.getRawQuery());
      // Presigned/external-link URLs embed secrets in the path or query; redact both. Ordinary API
      // paths stay visible so warehouse/Thrift diagnostics remain useful.
      String path = presigned ? "/REDACTED" : uri.getPath();
      String query =
          presigned
              ? (uri.getRawQuery() == null ? null : "REDACTED")
              : sanitizeQuery(uri.getRawQuery());
      URI sanitizedUri =
          new URI(uri.getScheme(), uri.getAuthority(), path, query, uri.getFragment());
      return sanitizedUri.toString();
    } catch (URISyntaxException e) {
      return "Error sanitizing URI: REDACTED";
    }
  }

  private static boolean isPresigned(String query) {
    if (query == null || query.isEmpty()) {
      return false;
    }
    for (String pair : query.split("&")) {
      int idx = pair.indexOf('=');
      String key = idx >= 0 ? pair.substring(0, idx) : pair;
      if (SENSITIVE_QUERY_PARAMS.contains(key)) {
        return true;
      }
    }
    return false;
  }

  private static String sanitizeQuery(String query) {
    if (query == null) {
      return null;
    }
    StringBuilder sanitizedQuery = new StringBuilder();
    String[] pairs = query.split("&");
    for (String pair : pairs) {
      int idx = pair.indexOf('=');
      String key = idx > 0 ? pair.substring(0, idx) : pair;
      if (SENSITIVE_QUERY_PARAMS.contains(key)) {
        sanitizedQuery.append(key).append("=REDACTED");
      } else {
        sanitizedQuery.append(pair);
      }
      sanitizedQuery.append('&');
    }
    if (sanitizedQuery.length() > 0) {
      sanitizedQuery.setLength(sanitizedQuery.length() - 1);
    }
    return sanitizedQuery.toString();
  }
}
