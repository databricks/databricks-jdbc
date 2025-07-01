package com.databricks.jdbc.dbclient.impl.common;

import java.security.SecureRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility class to support W3C Trace Context request tracing */
public final class TracingUtil {

  public static final String TRACE_HEADER = "traceparent";
  public static final String TRACE_STATE_HEADER = "tracestate";

  private static final String SEED_CHARACTERS = "0123456789abcdef";
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  // W3C Trace Context format: version-trace-id-parent-id-trace-flags
  private static final Pattern TRACEPARENT_PATTERN =
      Pattern.compile("^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$");

  private static final String VERSION = "00";
  private static final String DEFAULT_FLAGS = "01"; // sampled

  private TracingUtil() {
    // Utility class
  }

  /**
   * Validates a W3C traceparent header format.
   *
   * @param traceparent The traceparent header to validate
   * @return true if valid, false otherwise
   */
  public static boolean isValidTraceparent(String traceparent) {
    if (traceparent == null || traceparent.isEmpty()) {
      return false;
    }

    Matcher matcher = TRACEPARENT_PATTERN.matcher(traceparent.toLowerCase());
    if (!matcher.matches()) {
      return false;
    }

    // Only support version 00 for now
    String version = matcher.group(1);
    return VERSION.equals(version);
  }

  /**
   * Extracts the trace ID from a valid traceparent header.
   *
   * @param traceparent The traceparent header
   * @return The trace ID or null if invalid
   */
  public static String extractTraceId(String traceparent) {
    if (!isValidTraceparent(traceparent)) {
      return null;
    }

    Matcher matcher = TRACEPARENT_PATTERN.matcher(traceparent.toLowerCase());
    if (matcher.matches()) {
      return matcher.group(2);
    }
    return null;
  }

  /**
   * Extracts the trace flags from a valid traceparent header.
   *
   * @param traceparent The traceparent header
   * @return The trace flags or null if invalid
   */
  public static String extractTraceFlags(String traceparent) {
    if (!isValidTraceparent(traceparent)) {
      return null;
    }

    Matcher matcher = TRACEPARENT_PATTERN.matcher(traceparent.toLowerCase());
    if (matcher.matches()) {
      return matcher.group(4);
    }
    return null;
  }

  /**
   * Generates a new trace ID.
   *
   * @return A 32-character hex trace ID
   */
  public static String generateTraceId() {
    return randomSegment(32);
  }

  /**
   * Generates a new span ID.
   *
   * @return A 16-character hex span ID
   */
  public static String generateSpanId() {
    return randomSegment(16);
  }

  /**
   * Builds a W3C compliant traceparent header.
   *
   * @param traceId The trace ID (32 hex characters)
   * @param spanId The span ID (16 hex characters)
   * @param traceFlags The trace flags (2 hex characters)
   * @return The formatted traceparent header
   */
  public static String buildTraceparent(String traceId, String spanId, String traceFlags) {
    return String.format("%s-%s-%s-%s", VERSION, traceId, spanId, traceFlags);
  }

  /**
   * Generates a complete traceparent header with new IDs.
   *
   * @return A new traceparent header
   */
  public static String generateTraceparent() {
    return buildTraceparent(generateTraceId(), generateSpanId(), DEFAULT_FLAGS);
  }

  /**
   * Generates a traceparent header with the given trace ID and flags, and a new span ID.
   *
   * @param traceId The trace ID to use
   * @param traceFlags The trace flags to use
   * @return A traceparent header with new span ID
   */
  public static String generateTraceparentWithTraceId(String traceId, String traceFlags) {
    return buildTraceparent(traceId, generateSpanId(), traceFlags);
  }

  private static String randomSegment(int length) {
    StringBuilder result = new StringBuilder(length);
    byte[] bytes = new byte[length / 2];
    SECURE_RANDOM.nextBytes(bytes);

    for (byte b : bytes) {
      result.append(SEED_CHARACTERS.charAt((b >> 4) & 0xF));
      result.append(SEED_CHARACTERS.charAt(b & 0xF));
    }

    return result.toString();
  }
}
