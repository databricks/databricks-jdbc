package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksTelemetryException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.http.client.HttpResponseException;

/**
 * CircuitBreakerManager is a singleton that manages circuit breakers for different hosts. It
 * initializes circuit breakers with a predefined configuration and provides methods to retrieve or
 * reset them.
 */
public class CircuitBreakerManager {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(CircuitBreakerManager.class);

  // Singleton instance of CircuitBreakerManager
  private static final CircuitBreakerManager INSTANCE = new CircuitBreakerManager();

  // Method to get the singleton instance
  public static CircuitBreakerManager getInstance() {
    return INSTANCE;
  }

  private final Map<String, CircuitBreaker> breakerPerHost = new ConcurrentHashMap<>();
  private final CircuitBreakerConfig config;

  // Private constructor to prevent instantiation
  private CircuitBreakerManager() {
    // Initialize config hard coded for now
    config =
        CircuitBreakerConfig.custom()
            .failureRateThreshold(50) // Opens if 50%+ fail
            .minimumNumberOfCalls(20) // Minimum sample size
            .slidingWindowSize(30) // Keep recent 30 calls in window
            .waitDurationInOpenState(Duration.ofSeconds(30)) // Cool-down before retrying
            .permittedNumberOfCallsInHalfOpenState(3) // Retry with 3 test calls
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            // Exceptions that should be treated as failures
            .recordExceptions(
                // Server unavailability errors
                ConnectException.class,
                SocketTimeoutException.class,
                NoRouteToHostException.class,
                UnknownHostException.class,
                // Resource exhausted errors
                RejectedExecutionException.class,
                // Memory errors also can lean to open state
                OutOfMemoryError.class,
                // HTTP errors that indicate server issues
                HttpResponseException.class,
                // Databricks specific exceptions
                DatabricksTelemetryException.class)
            .ignoreExceptions(
                // Exceptions that can be ignored, the logic is that due to these exception,
                // the execution will not even reach the server, so no point in opening the
                // circuit breaker
                DatabricksParsingException.class,
                IllegalArgumentException.class,
                NullPointerException.class)
            .build();
    LOGGER.debug("CircuitBreakerManager initialized");
  }

  /**
   * Retrieves the CircuitBreaker for the specified host. If it does not exist, it creates a new one
   * with the default configuration.
   *
   * @param host The host for which to retrieve the CircuitBreaker.
   * @return The CircuitBreaker instance for the specified host.
   */
  public CircuitBreaker getCircuitBreaker(String host) {
    return breakerPerHost.computeIfAbsent(
        host,
        h -> {
          CircuitBreaker breaker = CircuitBreaker.of("telemetry-client-" + h, config);
          breaker
              .getEventPublisher()
              .onStateTransition(
                  event -> {
                    LOGGER.debug(
                        "CircuitBreaker for host [{}] transitioned from {} to {}",
                        h,
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState());
                  });
          return breaker;
        });
  }

  @VisibleForTesting
  void resetCircuitBreaker(String host) {
    breakerPerHost.remove(host);
  }
}
