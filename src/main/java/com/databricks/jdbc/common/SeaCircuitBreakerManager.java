package com.databricks.jdbc.common;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpStatus;

/**
 * Manages a circuit breaker for SEA (SQL Execution API) session creation.
 *
 * <p>When SEA session creation fails with HTTP 429 (rate limit exceeded) after exhausting retries,
 * this circuit breaker opens for 24 hours, forcing all subsequent connections to use Thrift client
 * instead of SEA.
 *
 * <p>This prevents cascading failures and gives the SEA service time to recover from rate limiting.
 *
 * <p>Thread-safe: Uses volatile for visibility across threads without synchronization overhead.
 */
public class SeaCircuitBreakerManager {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(SeaCircuitBreakerManager.class);

  /** HTTP status code for rate limiting */
  public static final int HTTP_TOO_MANY_REQUESTS = HttpStatus.SC_TOO_MANY_REQUESTS; // 429

  /** Duration the circuit breaker stays open after a 429 failure (24 hours in milliseconds) */
  private static final long CIRCUIT_BREAK_DURATION_MS = 24 * 60 * 60 * 1000;

  /**
   * Timestamp of the last 429 failure during SEA session creation. -1 indicates no failure has
   * occurred. Volatile ensures visibility across threads.
   */
  private static volatile long last429FailureTimestamp = -1;

  /** Private constructor to prevent instantiation */
  private SeaCircuitBreakerManager() {}

  /**
   * Records a 429 failure during SEA session creation, opening the circuit breaker for 24 hours.
   *
   * <p>This should be called only when session creation (not statement execution) fails with 429
   * after all retries have been exhausted.
   *
   * <p>Thread-safe via volatile write semantics.
   */
  public static void record429Failure() {
    long currentTime = System.currentTimeMillis();
    last429FailureTimestamp = currentTime;
    LOGGER.warn(
        "SEA circuit breaker OPENED due to 429 rate limit failure. "
            + "Will use Thrift client for the next 24 hours. Timestamp: {}",
        currentTime);
  }

  /**
   * Checks if the circuit breaker is currently open (within 24 hours of last 429 failure).
   *
   * @return true if the circuit is open and connections should bypass SEA and use Thrift directly,
   *     false otherwise
   */
  public static boolean isCircuitOpen() {
    long lastFailure = last429FailureTimestamp;
    if (lastFailure == -1) {
      return false; // Never failed
    }
    long elapsed = System.currentTimeMillis() - lastFailure;
    boolean isOpen = elapsed < CIRCUIT_BREAK_DURATION_MS;

    if (!isOpen) {
      // Circuit just closed - log it once
      LOGGER.trace("SEA circuit breaker CLOSED after 24 hours. Will check feature flag again.");
    }

    return isOpen;
  }

  /**
   * Gets the time remaining (in milliseconds) until the circuit breaker closes.
   *
   * @return milliseconds until circuit closes, or 0 if circuit is closed or never opened
   */
  public static long getTimeRemainingMs() {
    long lastFailure = last429FailureTimestamp;
    if (lastFailure == -1) {
      return 0;
    }
    long elapsed = System.currentTimeMillis() - lastFailure;
    if (elapsed >= CIRCUIT_BREAK_DURATION_MS) {
      return 0;
    }
    return CIRCUIT_BREAK_DURATION_MS - elapsed;
  }

  /**
   * Gets the time remaining until circuit closes, formatted as human-readable string.
   *
   * @return formatted time remaining (e.g., "23 hours 45 minutes"), or "closed" if not open
   */
  public static String getTimeRemainingFormatted() {
    long remainingMs = getTimeRemainingMs();
    if (remainingMs == 0) {
      return "closed";
    }
    long hours = TimeUnit.MILLISECONDS.toHours(remainingMs);
    long minutes = TimeUnit.MILLISECONDS.toMinutes(remainingMs) % 60;
    return String.format("%d hours %d minutes", hours, minutes);
  }

  /**
   * Resets the circuit breaker state. FOR TESTING PURPOSES ONLY.
   *
   * <p>This method should only be called from test code to reset state between tests.
   */
  @VisibleForTesting
  public static void reset() {
    last429FailureTimestamp = -1;
    LOGGER.debug("SEA circuit breaker has been reset (test only)");
  }
}
