package com.databricks.jdbc.exception;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

class DatabricksRateLimitExceptionTest {

  @Test
  void testConstructorWithMessageAndStatusCode() {
    String message = "Rate limit exceeded";
    int statusCode = 429;

    DatabricksRateLimitException exception = new DatabricksRateLimitException(message, statusCode);

    assertEquals(message, exception.getMessage());
    assertEquals(statusCode, exception.getStatusCode());
    assertEquals(DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED.name(), exception.getSQLState());
    assertNull(exception.getCause());
  }

  @Test
  void testConstructorWithMessageCauseAndStatusCode() {
    String message = "Rate limit exceeded";
    Throwable cause = new RuntimeException("Underlying cause");
    int statusCode = 429;

    DatabricksRateLimitException exception =
        new DatabricksRateLimitException(message, cause, statusCode);

    assertEquals(message, exception.getMessage());
    assertEquals(cause, exception.getCause());
    assertEquals(statusCode, exception.getStatusCode());
    assertEquals(DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED.name(), exception.getSQLState());
  }

  @Test
  void testHttpTooManyRequestsConstant() {
    // Verify the constant is correctly defined
    assertEquals(429, DatabricksRateLimitException.HTTP_TOO_MANY_REQUESTS);
    assertEquals(
        HttpStatus.SC_TOO_MANY_REQUESTS, DatabricksRateLimitException.HTTP_TOO_MANY_REQUESTS);
  }

  @Test
  void testWithConstant() {
    DatabricksRateLimitException exception =
        new DatabricksRateLimitException(
            "Rate limit exceeded", DatabricksRateLimitException.HTTP_TOO_MANY_REQUESTS);

    assertEquals(DatabricksRateLimitException.HTTP_TOO_MANY_REQUESTS, exception.getStatusCode());
    assertEquals(429, exception.getStatusCode());
  }

  @Test
  void testExceptionInheritance() {
    DatabricksRateLimitException exception =
        new DatabricksRateLimitException("Rate limit exceeded", 429);

    // Verify it's a DatabricksSQLException
    assertTrue(exception instanceof DatabricksSQLException);

    // Verify it can be caught as DatabricksSQLException
    try {
      throw exception;
    } catch (DatabricksSQLException e) {
      assertEquals("Rate limit exceeded", e.getMessage());
      assertEquals(DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED.name(), e.getSQLState());
    }
  }

  @Test
  void testMultipleInstances() {
    // Verify multiple instances can be created with different values
    DatabricksRateLimitException exception1 = new DatabricksRateLimitException("First error", 429);
    DatabricksRateLimitException exception2 = new DatabricksRateLimitException("Second error", 429);

    assertEquals("First error", exception1.getMessage());
    assertEquals("Second error", exception2.getMessage());
    assertEquals(429, exception1.getStatusCode());
    assertEquals(429, exception2.getStatusCode());
  }

  @Test
  void testWithCauseChain() {
    // Test with a cause chain
    RuntimeException rootCause = new RuntimeException("Root cause");
    IllegalStateException intermediateCause = new IllegalStateException("Intermediate", rootCause);
    DatabricksRateLimitException exception =
        new DatabricksRateLimitException("Rate limit error", intermediateCause, 429);

    assertEquals("Rate limit error", exception.getMessage());
    assertEquals(intermediateCause, exception.getCause());
    assertEquals(rootCause, exception.getCause().getCause());
    assertEquals(429, exception.getStatusCode());
  }

  @Test
  void testGetStatusCodeReturnsCorrectValue() {
    DatabricksRateLimitException exception = new DatabricksRateLimitException("Test message", 429);
    assertEquals(429, exception.getStatusCode());
  }

  @Test
  void testExceptionMessage() {
    String detailedMessage =
        "createSession failed with HTTP 429 (rate limit exceeded) after retries. "
            + "Warehouse id: abc123, Error: Too many requests";

    DatabricksRateLimitException exception = new DatabricksRateLimitException(detailedMessage, 429);

    assertTrue(exception.getMessage().contains("rate limit exceeded"));
    assertTrue(exception.getMessage().contains("Warehouse id: abc123"));
    assertEquals(429, exception.getStatusCode());
  }

  @Test
  void testSQLStateIsRateLimitExceeded() {
    DatabricksRateLimitException exception = new DatabricksRateLimitException("Rate limit", 429);

    assertEquals(DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED.name(), exception.getSQLState());
  }

  @Test
  void testNullCauseIsHandled() {
    DatabricksRateLimitException exception =
        new DatabricksRateLimitException("Rate limit", null, 429);

    assertEquals("Rate limit", exception.getMessage());
    assertNull(exception.getCause());
    assertEquals(429, exception.getStatusCode());
  }
}
