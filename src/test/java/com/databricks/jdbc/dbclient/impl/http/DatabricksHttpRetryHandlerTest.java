package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.dbclient.impl.http.DatabricksHttpRetryHandler.isRequestMethodRetryable;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksRetryHandlerException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HttpCoreContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksHttpRetryHandlerTest {

  private static final String RETRY_INTERVAL_KEY = "retryInterval";
  private static final String TEMP_UNAVAILABLE_ACCUMULATED_TIME_KEY =
      "tempUnavailableAccumulatedTime";
  private static final String RATE_LIMIT_ACCUMULATED_TIME_KEY = "rateLimitAccumulatedTime";
  private static final String RETRY_AFTER_HEADER = "Retry-After";

  @Mock private IDatabricksConnectionContext mockConnectionContext;
  private HttpClientContext httpContext;
  private DatabricksHttpRetryHandler retryHandler;
  private List<Long> sleepDurations;

  private class TestDatabricksHttpRetryHandler extends DatabricksHttpRetryHandler {
    TestDatabricksHttpRetryHandler(IDatabricksConnectionContext connectionContext) {
      super(connectionContext);
    }

    @Override
    protected void doSleepForDelay(long delayMillis) {
      sleepDurations.add(delayMillis);
    }
  }

  @BeforeEach
  public void setUp() {
    retryHandler = new TestDatabricksHttpRetryHandler(mockConnectionContext);
    sleepDurations = new ArrayList<>();
    // Use real HttpContext instead of mock
    httpContext = HttpClientContext.create();
    // Initialize retry counts
    httpContext.setAttribute(RETRY_INTERVAL_KEY, 0);
    httpContext.setAttribute(TEMP_UNAVAILABLE_ACCUMULATED_TIME_KEY, 0L);
    httpContext.setAttribute(RATE_LIMIT_ACCUMULATED_TIME_KEY, 0L);
  }

  @Test
  void processWithNonRetryableStatusCode() throws IOException {
    HttpResponse response = createResponse(HttpStatus.SC_OK);
    retryHandler.process(response, httpContext);
    // No exception should be thrown, and no attributes should be set
    assertEquals(0, httpContext.getAttribute(RETRY_INTERVAL_KEY));
  }

  @Test
  void processWithRetryableStatusCodeAndRetryAfterHeader() {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    HttpResponse response = createResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, "5");

    DatabricksRetryHandlerException exception =
        assertThrows(
            DatabricksRetryHandlerException.class,
            () -> retryHandler.process(response, httpContext));

    assertEquals(HttpStatus.SC_SERVICE_UNAVAILABLE, exception.getErrCode());
    assertEquals(5, httpContext.getAttribute(RETRY_INTERVAL_KEY));
  }

  @Test
  void testServiceUnavailableWithVaryingRetryAfterValues() throws IOException {
    when(mockConnectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    when(mockConnectionContext.getTemporarilyUnavailableRetryTimeout()).thenReturn(30);
    HttpRequest request = createRequest("GET", "/api/data");
    httpContext.setAttribute(HttpCoreContext.HTTP_REQUEST, request);

    // First attempt: 503 with Retry-After: 5
    HttpResponse response1 = createResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, "5");
    assertThrows(
        DatabricksRetryHandlerException.class, () -> retryHandler.process(response1, httpContext));
    assertTrue(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_SERVICE_UNAVAILABLE),
            1,
            httpContext));

    // Second attempt: 503 with Retry-After: 10
    HttpResponse response2 = createResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, "10");
    assertThrows(
        DatabricksRetryHandlerException.class, () -> retryHandler.process(response2, httpContext));
    assertTrue(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_SERVICE_UNAVAILABLE),
            2,
            httpContext));

    // Third attempt: 503 with Retry-After: 20 (should exceed timeout)
    HttpResponse response3 = createResponse(HttpStatus.SC_SERVICE_UNAVAILABLE, "20");
    assertThrows(
        DatabricksRetryHandlerException.class, () -> retryHandler.process(response3, httpContext));
    assertFalse(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_SERVICE_UNAVAILABLE),
            3,
            httpContext));

    // Verify sleep durations match Retry-After values
    assertEquals(2, sleepDurations.size());
    assertEquals(5000L, sleepDurations.get(0));
    assertEquals(10000L, sleepDurations.get(1));

    // Verify accumulated time tracking
    Long finalAccumulatedTime =
        (Long) httpContext.getAttribute(TEMP_UNAVAILABLE_ACCUMULATED_TIME_KEY);
    assertEquals(15L, finalAccumulatedTime); // 5 + 10
  }

  @Test
  void testRateLimitWithConstantRetryAfter() throws IOException {
    when(mockConnectionContext.shouldRetryRateLimitError()).thenReturn(true);
    when(mockConnectionContext.getRateLimitRetryTimeout()).thenReturn(45);
    HttpRequest request = createRequest("POST", "/api/data");
    httpContext.setAttribute(HttpCoreContext.HTTP_REQUEST, request);

    // Configure a constant Retry-After of 15 seconds
    // First attempt
    HttpResponse response1 = createResponse(HttpStatus.SC_TOO_MANY_REQUESTS, "15");
    assertThrows(
        DatabricksRetryHandlerException.class, () -> retryHandler.process(response1, httpContext));
    assertTrue(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_TOO_MANY_REQUESTS),
            1,
            httpContext));

    // Second attempt
    HttpResponse response2 = createResponse(HttpStatus.SC_TOO_MANY_REQUESTS, "15");
    assertThrows(
        DatabricksRetryHandlerException.class, () -> retryHandler.process(response2, httpContext));
    assertTrue(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_TOO_MANY_REQUESTS),
            2,
            httpContext));

    // Third attempt
    HttpResponse response3 = createResponse(HttpStatus.SC_TOO_MANY_REQUESTS, "15");
    assertThrows(
        DatabricksRetryHandlerException.class, () -> retryHandler.process(response3, httpContext));
    assertTrue(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_TOO_MANY_REQUESTS),
            3,
            httpContext));

    // Fourth attempt - should exceed timeout (45 seconds)
    HttpResponse response4 = createResponse(HttpStatus.SC_TOO_MANY_REQUESTS, "15");
    assertThrows(
        DatabricksRetryHandlerException.class, () -> retryHandler.process(response4, httpContext));
    assertFalse(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_TOO_MANY_REQUESTS),
            4,
            httpContext));

    // Verify sleep durations are constant
    assertEquals(3, sleepDurations.size());
    sleepDurations.forEach(duration -> assertEquals(15000L, duration));

    // Verify accumulated time tracking
    Long finalAccumulatedTime = (Long) httpContext.getAttribute(RATE_LIMIT_ACCUMULATED_TIME_KEY);
    assertEquals(45L, finalAccumulatedTime); // 15 * 3
  }

  @Test
  void testExponentialBackoffDelay() {
    // Test exponential backoff for non-503/429 status codes
    long delay1 =
        DatabricksHttpRetryHandler.calculateDelayInMillis(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, 1, 0);
    long delay2 =
        DatabricksHttpRetryHandler.calculateDelayInMillis(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, 2, 0);
    long delay3 =
        DatabricksHttpRetryHandler.calculateDelayInMillis(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, 3, 0);

    // Verify exponential growth
    assertTrue(delay2 > delay1);
    assertTrue(delay3 > delay2);
    assertEquals(2000L, delay1); // 1 * 2^1
    assertEquals(4000L, delay2); // 1 * 2^2
    assertEquals(8000L, delay3); // 1 * 2^3
  }

  @Test
  void testExponentialBackoffMaxLimit() {
    // Test that exponential backoff respects max retry interval
    long delay =
        DatabricksHttpRetryHandler.calculateDelayInMillis(
            HttpStatus.SC_INTERNAL_SERVER_ERROR, 10, 0);
    assertEquals(10000L, delay); // Should be capped at MAX_RETRY_INTERVAL (10 seconds)
  }

  @Test
  void testRetryAfterHeaderPrecedence() {
    // For 503/429, Retry-After header should take precedence over exponential backoff
    long delay503 =
        DatabricksHttpRetryHandler.calculateDelayInMillis(HttpStatus.SC_SERVICE_UNAVAILABLE, 5, 3);
    long delay429 =
        DatabricksHttpRetryHandler.calculateDelayInMillis(HttpStatus.SC_TOO_MANY_REQUESTS, 5, 3);

    assertEquals(3000, delay503); // Should use Retry-After value
    assertEquals(3000, delay429); // Should use Retry-After value
  }

  @Test
  void testIsRequestMethodRetryable() {
    assertTrue(isRequestMethodRetryable("GET"), "GET requests should be allowed for retry");
    assertFalse(isRequestMethodRetryable("HEAD"), "HEAD requests should not be allowed for retry");
    assertTrue(isRequestMethodRetryable("PUT"), "PUT requests should be allowed for retry");
    assertTrue(isRequestMethodRetryable("POST"), "POST requests should be allowed for retry");
    assertFalse(
        isRequestMethodRetryable("DELETE"), "DELETE requests should not be allowed for retry");
    assertFalse(
        isRequestMethodRetryable("PATCH"), "PATCH requests should not be allowed for retry");
  }

  @Test
  void testRetryRequestWithInvalidRetryInterval() {
    // Set up context with -1 retry interval (invalid)
    httpContext.setAttribute(RETRY_INTERVAL_KEY, -1);

    // Test for Service Unavailable (503)
    DatabricksRetryHandlerException exception503 =
        new DatabricksRetryHandlerException(
            "Service Unavailable", HttpStatus.SC_SERVICE_UNAVAILABLE);

    boolean shouldRetry503 = retryHandler.retryRequest(exception503, 1, httpContext);
    assertFalse(shouldRetry503, "Should return false when retryInterval is -1 for status 503");

    // Test for Too Many Requests (429)
    DatabricksRetryHandlerException exception429 =
        new DatabricksRetryHandlerException("Too Many Requests", HttpStatus.SC_TOO_MANY_REQUESTS);

    boolean shouldRetry429 = retryHandler.retryRequest(exception429, 1, httpContext);
    assertFalse(shouldRetry429, "Should return false when retryInterval is -1 for status 429");
  }

  @Test
  void testApiRetriableCodesWithoutRetryAfterHeader() throws IOException {
    when(mockConnectionContext.getApiRetriableHttpCodes())
        .thenReturn(java.util.Set.of(HttpStatus.SC_INTERNAL_SERVER_ERROR));
    when(mockConnectionContext.getApiRetryTimeout()).thenReturn(120);

    HttpRequest request = createRequest("GET", "/api/data");
    httpContext.setAttribute(HttpCoreContext.HTTP_REQUEST, request);

    // 503 WITHOUT Retry-After header - should use exponential backoff
    HttpResponse response = createResponse(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    assertThrows(
        DatabricksRetryHandlerException.class, () -> retryHandler.process(response, httpContext));

    assertTrue(
        retryHandler.retryRequest(
            new DatabricksRetryHandlerException("Test", HttpStatus.SC_INTERNAL_SERVER_ERROR),
            1,
            httpContext));

    // Verify exponential backoff delay was used
    long expectedDelay = DatabricksHttpRetryHandler.calculateExponentialBackoff(1);
    assertEquals(1, sleepDurations.size());
    assertEquals(expectedDelay, sleepDurations.get(0));
  }

  private HttpResponse createResponse(int statusCode) {
    return createResponse(statusCode, null);
  }

  private HttpResponse createResponse(int statusCode, String retryAfter) {
    BasicHttpResponse response =
        new BasicHttpResponse(
            new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, ""));
    if (retryAfter != null) {
      response.setHeader(RETRY_AFTER_HEADER, retryAfter);
    }
    return response;
  }

  private HttpRequest createRequest(String method, String uri) {
    return new BasicHttpRequest(
        new BasicRequestLine(method, uri, new ProtocolVersion("HTTP", 1, 1)));
  }
}
