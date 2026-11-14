package com.databricks.jdbc.dbclient.impl.http;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.client.methods.HttpUriRequest;

public class RequestSanitizer {
  public static String sanitizeRequest(HttpUriRequest request) {
    try {
      URI uri = new URI(request.getURI().toString());
      // Query values can carry bearer credentials under provider-specific or future parameter
      // names (including AWS Signature V2). Never include any of them in logs or errors.
      String sanitizedQuery = uri.getRawQuery() == null ? null : "REDACTED";
      URI sanitizedUri =
          new URI(
              uri.getScheme(),
              null,
              uri.getHost(),
              uri.getPort(),
              "/REDACTED",
              sanitizedQuery,
              null);
      return sanitizedUri.toString();
    } catch (URISyntaxException e) {
      return "Error sanitizing URI: REDACTED";
    }
  }
}
