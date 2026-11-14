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

    String expectedUri = "https://example.com/REDACTED?REDACTED";
    assertEquals(expectedUri, sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withAzureSasSignature() {
    String originalUri =
        "https://acct.blob.core.windows.net/c/chunk.arrow?sv=2021-08-06&se=2026-06-18T00:00Z&sr=b&sp=r&sig=secretSasSignature";
    String sanitizedUri = RequestSanitizer.sanitizeRequest(new HttpGet(originalUri));

    // The complete presigned query is redacted.
    String expectedUri = "https://acct.blob.core.windows.net/REDACTED?REDACTED";
    assertEquals(expectedUri, sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withGcsV4Signature() {
    String originalUri =
        "https://storage.googleapis.com/bucket/chunk.arrow?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=svc&X-Goog-Signature=secretSig";
    String sanitizedUri = RequestSanitizer.sanitizeRequest(new HttpGet(originalUri));

    String expectedUri = "https://storage.googleapis.com/REDACTED?REDACTED";
    assertEquals(expectedUri, sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withLowercaseGcsSignature() {
    // GCS tooling may emit lowercase param names; matching must be case-insensitive.
    String originalUri =
        "https://storage.googleapis.com/bucket/chunk.arrow?x-goog-credential=svc&x-goog-signature=secretSig";
    String sanitizedUri = RequestSanitizer.sanitizeRequest(new HttpGet(originalUri));

    String expectedUri = "https://storage.googleapis.com/REDACTED?REDACTED";
    assertEquals(expectedUri, sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withNoSensitiveParams() {
    String originalUri = "https://example.com/api?param1=value1&param2=value2";
    HttpUriRequest request = new HttpGet(originalUri);

    String sanitizedUri = RequestSanitizer.sanitizeRequest(request);

    assertEquals("https://example.com/REDACTED?REDACTED", sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withMixedParams() {
    String originalUri =
        "https://example.com/api?param1=value1&X-Amz-Security-Token=token123&param2=value2&X-Amz-Signature=signature123";
    HttpUriRequest request = new HttpGet(originalUri);

    String sanitizedUri = RequestSanitizer.sanitizeRequest(request);

    String expectedUri = "https://example.com/REDACTED?REDACTED";
    assertEquals(expectedUri, sanitizedUri);
  }

  @Test
  public void testSanitizeRequest_withLegacySignature() {
    String url =
        "https://storage.example.test/private?AWSAccessKeyId=access&Signature=secret&Expires=123";
    String sanitized = RequestSanitizer.sanitizeRequest(new HttpGet(url));
    assertEquals("https://storage.example.test/REDACTED?REDACTED", sanitized);
  }

  @Test
  public void testSanitizeRequest_withEmptyQuery() {
    String originalUri = "https://example.com/api";
    HttpUriRequest request = new HttpGet(originalUri);

    String sanitizedUri = RequestSanitizer.sanitizeRequest(request);

    assertEquals("https://example.com/REDACTED", sanitizedUri);
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
