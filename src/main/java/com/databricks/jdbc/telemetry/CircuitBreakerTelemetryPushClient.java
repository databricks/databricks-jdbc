package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.apache.arrow.util.VisibleForTesting;

/**
 * TelemetryClient wrapper that implements circuit breaker pattern using Resilience4j. This wrapper
 * handles server unavailability and resource exhausted errors by temporarily stopping telemetry
 * requests when the service is experiencing issues.
 */
public class CircuitBreakerTelemetryPushClient implements ITelemetryPushClient {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(CircuitBreakerTelemetryPushClient.class);
  private static final String TEST_CLIENT_NAME = "telemetry-client-test";

  private final ITelemetryPushClient delegate;
  private final CircuitBreaker circuitBreaker;
  private final String host;

  CircuitBreakerTelemetryPushClient(ITelemetryPushClient delegate, String host) {
    this.delegate = delegate;
    this.host = host;
    this.circuitBreaker = CircuitBreakerManager.getInstance().getCircuitBreaker(host);
  }

  @VisibleForTesting
  CircuitBreakerTelemetryPushClient(
      ITelemetryPushClient delegate, String host, CircuitBreakerConfig config) {
    this.delegate = delegate;
    this.host = host;
    this.circuitBreaker = CircuitBreaker.of(TEST_CLIENT_NAME, config);
  }

  @Override
  public void pushEvent(TelemetryRequest request) {
    try {
      circuitBreaker.executeCallable(
          () -> {
            delegate.pushEvent(request);
            return null;
          });
    } catch (Exception e) {
      LOGGER.debug("Failed to export telemetry for host [{}]: {}", host, e.getMessage());

      if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
        LOGGER.debug("CircuitBreaker for host [{}] is OPEN - dropping telemetry", host);
      }
    }
  }

  /**
   * Get the current state of the circuit breaker.
   *
   * @return The current circuit breaker state
   */
  @VisibleForTesting
  CircuitBreaker.State getCircuitBreakerState() {
    return circuitBreaker.getState();
  }

  /**
   * Get metrics about the circuit breaker.
   *
   * @return Circuit breaker metrics
   */
  @VisibleForTesting
  CircuitBreaker.Metrics getCircuitBreakerMetrics() {
    return circuitBreaker.getMetrics();
  }
}
