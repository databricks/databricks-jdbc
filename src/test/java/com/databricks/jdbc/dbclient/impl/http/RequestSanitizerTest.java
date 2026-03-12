package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.junit.jupiter.api.Test;

public class RequestSanitizerTest {

  @Test
  public void testSanitizeRequest_withSensitiveParams() {
    String originalUri =
        "https://example.com/api?X-Amz-Security-Token=token123&X-Amz-Signature=signature123&X-Amz-Credential=credential123";
    HttpUriRequest request = new HttpGet(originalUri);

    String sanitizedUri = RequestSanitizer.sanitizeRequest(request);

    String expectedUri =
        "https://example.com/api?X-Amz-Security-Token=REDACTED&X-Amz-Signature=REDACTED&X-Amz-Credential=REDACTED";
    assertEquals(expectedUri, sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withNoSensitiveParams() {
    String originalUri = "https://example.com/api?param1=value1&param2=value2";
    HttpUriRequest request = new HttpGet(originalUri);

    String sanitizedUri = RequestSanitizer.sanitizeRequest(request);

    assertEquals(originalUri, sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withMixedParams() {
    String originalUri =
        "https://example.com/api?param1=value1&X-Amz-Security-Token=token123&param2=value2&X-Amz-Signature=signature123";
    HttpUriRequest request = new HttpGet(originalUri);

    String sanitizedUri = RequestSanitizer.sanitizeRequest(request);

    String expectedUri =
        "https://example.com/api?param1=value1&X-Amz-Security-Token=REDACTED&param2=value2&X-Amz-Signature=REDACTED";
    assertEquals(expectedUri, sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withEmptyQuery() {
    String originalUri = "https://example.com/api";
    HttpUriRequest request = new HttpGet(originalUri);

    String sanitizedUri = RequestSanitizer.sanitizeRequest(request);

    assertEquals(originalUri, sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withErrorInUri() {
    HttpUriRequest mockRequest = mock(HttpUriRequest.class);
    URI mockUri = mock(URI.class);

    when(mockRequest.getURI()).thenReturn(mockUri);
    when(mockUri.toString()).thenReturn("https://example.com/path with spaces");

    String result = RequestSanitizer.sanitizeRequest(mockRequest);
    assertTrue(result.startsWith("Error sanitizing URI:"));
  }
}
