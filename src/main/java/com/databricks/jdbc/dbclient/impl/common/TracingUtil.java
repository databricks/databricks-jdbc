package com.databricks.jdbc.dbclient.impl.common;

/** Utility class to support request tracing */
public final class TracingUtil {

  public static final String TRACE_HEADER = "traceparent";

  private static final String SEED_CHARACTERS = "0123456789abcdef";
  private static final int SEED_CHARACTERS_LENGTH = SEED_CHARACTERS.length();

  public static String getTraceHeader() {
    // Construct the string with the specified format
    return String.format("00-%s-%s-01", randomSegment(32), randomSegment(16));
  }

  private static String randomSegment(int length) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < length; i++) {
      result.append(
          SEED_CHARACTERS.charAt((int) Math.floor(Math.random() * SEED_CHARACTERS_LENGTH)));
    }
    return result.toString();
  }
}
