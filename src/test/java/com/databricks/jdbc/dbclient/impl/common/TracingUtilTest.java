package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

public class TracingUtilTest {

  private static final Pattern TRACEPARENT_PATTERN =
      Pattern.compile("^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$");

  @Test
  void testGenerateTraceparentGeneratesValidFormat() {
    String traceHeader = TracingUtil.generateTraceparent();
    assertNotNull(traceHeader);
    assertTrue(
        TRACEPARENT_PATTERN.matcher(traceHeader).matches(),
        "Trace header should match W3C format: " + traceHeader);
  }

  @Test
  void testValidateTraceparent() {
    String validTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    assertTrue(TracingUtil.isValidTraceparent(validTraceparent));

    // Test invalid formats
    assertFalse(TracingUtil.isValidTraceparent(null));
    assertFalse(TracingUtil.isValidTraceparent(""));
    assertFalse(TracingUtil.isValidTraceparent("invalid-format"));
    assertFalse(TracingUtil.isValidTraceparent("00-invalid-00f067aa0ba902b7-01"));
    assertFalse(
        TracingUtil.isValidTraceparent(
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")); // wrong version
  }

  @Test
  void testExtractTraceId() {
    String validTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    assertEquals("4bf92f3577b34da6a3ce929d0e0e4736", TracingUtil.extractTraceId(validTraceparent));

    // Invalid traceparent should return null
    assertNull(TracingUtil.extractTraceId("invalid-format"));
    assertNull(TracingUtil.extractTraceId(null));
  }

  @Test
  void testExtractTraceFlags() {
    String validTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    assertEquals("01", TracingUtil.extractTraceFlags(validTraceparent));

    // Invalid traceparent should return null
    assertNull(TracingUtil.extractTraceFlags("invalid-format"));
    assertNull(TracingUtil.extractTraceFlags(null));
  }

  @Test
  void testGenerateTraceparentWithTraceId() {
    String traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
    String traceFlags = "01";

    String header1 = TracingUtil.generateTraceparentWithTraceId(traceId, traceFlags);
    String header2 = TracingUtil.generateTraceparentWithTraceId(traceId, traceFlags);

    // Both should have same trace ID
    assertTrue(header1.contains(traceId));
    assertTrue(header2.contains(traceId));

    // But different span IDs
    String spanId1 = extractSpanId(header1);
    String spanId2 = extractSpanId(header2);
    assertNotEquals(spanId1, spanId2);
  }

  @Test
  void testBuildTraceparent() {
    String traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
    String spanId = "00f067aa0ba902b7";
    String traceFlags = "01";

    String result = TracingUtil.buildTraceparent(traceId, spanId, traceFlags);
    assertEquals("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", result);
  }

  @Test
  void testGenerateIds() {
    // Test trace ID generation
    String traceId = TracingUtil.generateTraceId();
    assertNotNull(traceId);
    assertEquals(32, traceId.length());
    assertTrue(traceId.matches("[0-9a-f]{32}"));

    // Test span ID generation
    String spanId = TracingUtil.generateSpanId();
    assertNotNull(spanId);
    assertEquals(16, spanId.length());
    assertTrue(spanId.matches("[0-9a-f]{16}"));

    // IDs should be different each time
    assertNotEquals(traceId, TracingUtil.generateTraceId());
    assertNotEquals(spanId, TracingUtil.generateSpanId());
  }

  private String extractSpanId(String traceparent) {
    var matcher = TRACEPARENT_PATTERN.matcher(traceparent);
    assertTrue(matcher.matches());
    return matcher.group(3);
  }
}
