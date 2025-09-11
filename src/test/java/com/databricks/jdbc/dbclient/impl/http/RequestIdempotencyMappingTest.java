package com.databricks.jdbc.dbclient.impl.http;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.HTTPRequestType;
import com.databricks.jdbc.dbclient.impl.http.HttpRequestTypeBasedRetryHandler.RequestIdempotency;
import java.lang.reflect.Field;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class RequestIdempotencyMappingTest {

  @Test
  public void testAllHTTPRequestTypesAreMapped() throws Exception {
    // Get the private REQUEST_IDEMPOTENCY_MAP using reflection
    Field mapField =
        HttpRequestTypeBasedRetryHandler.class.getDeclaredField("REQUEST_IDEMPOTENCY_MAP");
    mapField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<HTTPRequestType, RequestIdempotency> idempotencyMap =
        (Map<HTTPRequestType, RequestIdempotency>) mapField.get(null);

    // Verify that all HTTPRequestType values are mapped
    for (HTTPRequestType requestType : HTTPRequestType.values()) {
      assertTrue(
          idempotencyMap.containsKey(requestType),
          "Request type " + requestType + " should be mapped to an idempotency");
    }
  }

  @Test
  public void testIdempotentRequestTypes() {
    // Test that idempotent request types are correctly classified
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_OPEN_SESSION));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_CLOSE_SESSION));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_METADATA));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_CLOSE_OPERATION));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_CANCEL_OPERATION));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_FETCH_RESULTS));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.CLOUD_FETCH));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_LIST));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_SHOW_VOLUMES));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_GET));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_DELETE));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.AUTH));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.TELEMETRY_PUSH));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.OTHER));
  }

  @Test
  public void testNonIdempotentRequestTypes() {
    // Test that non-idempotent request types are correctly classified
    assertEquals(
        RequestIdempotency.NON_IDEMPOTENT,
        getIdempotency(HTTPRequestType.THRIFT_EXECUTE_STATEMENT));
    assertEquals(RequestIdempotency.NON_IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_PUT));
    assertEquals(RequestIdempotency.NON_IDEMPOTENT, getIdempotency(HTTPRequestType.UNKNOWN));
  }

  @Test
  public void testIdempotencyLogicalConsistency() {
    // Test that idempotency assignments make logical sense

    // Safe read operations should be idempotent
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_FETCH_RESULTS));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.CLOUD_FETCH));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_GET));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_LIST));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_SHOW_VOLUMES));

    // Session management operations should be idempotent (safe to retry)
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_OPEN_SESSION));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_CLOSE_SESSION));

    // Metadata operations should be idempotent
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_METADATA));

    // Cancel and close operations should be idempotent
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_CANCEL_OPERATION));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_CLOSE_OPERATION));

    // Delete operations should be idempotent (DELETE is idempotent in HTTP)
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_DELETE));

    // Authentication should be idempotent
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.AUTH));

    // Telemetry push should be idempotent (data can be sent multiple times)
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.TELEMETRY_PUSH));

    // Statement execution should be non-idempotent (may have side effects)
    assertEquals(
        RequestIdempotency.NON_IDEMPOTENT,
        getIdempotency(HTTPRequestType.THRIFT_EXECUTE_STATEMENT));

    // File/volume PUT operations should be non-idempotent (may overwrite data)
    assertEquals(RequestIdempotency.NON_IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_PUT));

    // UNKNOWN should be conservative (non-idempotent)
    assertEquals(RequestIdempotency.NON_IDEMPOTENT, getIdempotency(HTTPRequestType.UNKNOWN));
  }

  @Test
  public void testDefaultBehaviorForUnknownRequestType() {
    // Test that unknown request types default to non-idempotent (safer default)
    // This tests the getOrDefault behavior in the actual handler

    // We can't directly test with a non-existent enum value, but we can test
    // that UNKNOWN defaults to non-idempotent
    assertEquals(RequestIdempotency.NON_IDEMPOTENT, getIdempotency(HTTPRequestType.UNKNOWN));
  }

  @Test
  public void testIdempotencyEnumValues() {
    // Test the RequestIdempotency enum itself
    assertEquals(2, RequestIdempotency.values().length);
    assertNotNull(RequestIdempotency.valueOf("IDEMPOTENT"));
    assertNotNull(RequestIdempotency.valueOf("NON_IDEMPOTENT"));

    assertNotEquals(RequestIdempotency.IDEMPOTENT, RequestIdempotency.NON_IDEMPOTENT);
  }

  @Test
  public void testMappingCompleteness() {
    // Verify that we have exactly the expected number of mappings
    int totalHttpRequestTypes = HTTPRequestType.values().length;

    int idempotentCount = 0;
    int nonIdempotentCount = 0;

    for (HTTPRequestType requestType : HTTPRequestType.values()) {
      if (getIdempotency(requestType) == RequestIdempotency.IDEMPOTENT) {
        idempotentCount++;
      } else {
        nonIdempotentCount++;
      }
    }

    assertEquals(
        totalHttpRequestTypes,
        idempotentCount + nonIdempotentCount,
        "All request types should be classified as either idempotent or non-idempotent");

    // Based on current mapping:
    // Idempotent: 14 types (most operations)
    // Non-idempotent: 3 types (THRIFT_EXECUTE_STATEMENT, VOLUME_PUT, UNKNOWN)
    assertEquals(14, idempotentCount, "Expected 14 idempotent request types");
    assertEquals(3, nonIdempotentCount, "Expected 3 non-idempotent request types");
  }

  @Test
  public void testVolumeOperationsIdempotency() {
    // Volume operations should follow HTTP semantics:
    // GET, DELETE, LIST -> idempotent
    // PUT -> non-idempotent (may overwrite)
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_GET));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_DELETE));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_LIST));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_SHOW_VOLUMES));
    assertEquals(RequestIdempotency.NON_IDEMPOTENT, getIdempotency(HTTPRequestType.VOLUME_PUT));
  }

  @Test
  public void testThriftOperationsIdempotency() {
    // Most Thrift operations should be idempotent except for statement execution
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_OPEN_SESSION));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_CLOSE_SESSION));
    assertEquals(RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_METADATA));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_CLOSE_OPERATION));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_CANCEL_OPERATION));
    assertEquals(
        RequestIdempotency.IDEMPOTENT, getIdempotency(HTTPRequestType.THRIFT_FETCH_RESULTS));

    // Statement execution can have side effects, so it's non-idempotent
    assertEquals(
        RequestIdempotency.NON_IDEMPOTENT,
        getIdempotency(HTTPRequestType.THRIFT_EXECUTE_STATEMENT));
  }

  // Helper method to access the private getIdempotency method
  private RequestIdempotency getIdempotency(HTTPRequestType requestType) {
    try {
      java.lang.reflect.Method method =
          HttpRequestTypeBasedRetryHandler.class.getDeclaredMethod(
              "getIdempotency", HTTPRequestType.class);
      method.setAccessible(true);
      return (RequestIdempotency) method.invoke(null, requestType);
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke getIdempotency method", e);
    }
  }
}
