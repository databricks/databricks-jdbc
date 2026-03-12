package com.databricks.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.exception.DatabricksTelemetryException;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.net.ConnectException;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CircuitBreakerTelemetryPushClientTest {

  @Mock private ITelemetryPushClient mockDelegate;

  private CircuitBreakerTelemetryPushClient circuitBreakerClient;
  private TelemetryRequest testEvent;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    testEvent = new TelemetryRequest();
    // Reset the breaker in the manager
    CircuitBreakerManager.getInstance().resetCircuitBreaker("test-host");
    circuitBreakerClient = new CircuitBreakerTelemetryPushClient(mockDelegate, "test-host");
  }

  @Test
  void testSuccessfulPushEvent() throws Exception {
    // Given: delegate works normally
    doNothing().when(mockDelegate).pushEvent(any(TelemetryRequest.class));

    // When: exporting an event
    circuitBreakerClient.pushEvent(testEvent);

    // Then: delegate should be called
    verify(mockDelegate, times(1)).pushEvent(testEvent);
    assertEquals(CircuitBreaker.State.CLOSED, circuitBreakerClient.getCircuitBreakerState());
  }

  @Test
  void testCircuitRemainsClosed() throws Exception {
    // Given: delegate throws exceptions
    doThrow(new ConnectException("Connection failed"))
        .when(mockDelegate)
        .pushEvent(any(TelemetryRequest.class));

    // When: making multiple calls that fail
    for (int i = 0; i < 19; i++) {
      circuitBreakerClient.pushEvent(testEvent);
    }

    // Then: circuit breaker should remain closed after not enough failures
    assertEquals(CircuitBreaker.State.CLOSED, circuitBreakerClient.getCircuitBreakerState());

    // And: subsequent calls should not reach the delegate
    circuitBreakerClient.pushEvent(testEvent);

    // Should be open now
    assertEquals(CircuitBreaker.State.OPEN, circuitBreakerClient.getCircuitBreakerState());

    verify(mockDelegate, atMost(20)).pushEvent(any(TelemetryRequest.class));
    CircuitBreaker.Metrics metrics = circuitBreakerClient.getCircuitBreakerMetrics();
    assertEquals(20, metrics.getNumberOfFailedCalls());
    assertEquals(0, metrics.getNumberOfSuccessfulCalls());
  }

  @Test
  void testCircuitBreakerOpensOnFailures() throws Exception {
    // Given: delegate throws exceptions
    doThrow(new ConnectException("Connection failed"))
        .when(mockDelegate)
        .pushEvent(any(TelemetryRequest.class));

    // When: making multiple calls that fail
    for (int i = 0; i < 20; i++) {
      circuitBreakerClient.pushEvent(testEvent);
    }

    // Then: circuit breaker should open after enough failures
    assertEquals(CircuitBreaker.State.OPEN, circuitBreakerClient.getCircuitBreakerState());

    // And: subsequent calls should not reach the delegate
    circuitBreakerClient.pushEvent(testEvent);
    verify(mockDelegate, atMost(20)).pushEvent(any(TelemetryRequest.class));
    CircuitBreaker.Metrics metrics = circuitBreakerClient.getCircuitBreakerMetrics();
    assertEquals(20, metrics.getNumberOfFailedCalls());
    assertEquals(0, metrics.getNumberOfSuccessfulCalls());
  }

  @Test
  void testCircuitBreakerRecovery() throws Exception {
    CircuitBreakerConfig config =
        CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .minimumNumberOfCalls(10)
            .slidingWindowSize(20)
            .waitDurationInOpenState(Duration.ofSeconds(1)) // shorter duration
            .permittedNumberOfCallsInHalfOpenState(3)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .build();
    circuitBreakerClient = new CircuitBreakerTelemetryPushClient(mockDelegate, "test-host", config);

    // STEP 1: Fail 25 times to trip breaker to OPEN
    doThrow(new ConnectException("Simulated failure"))
        .when(mockDelegate)
        .pushEvent(any(TelemetryRequest.class));

    for (int i = 0; i < 25; i++) {
      circuitBreakerClient.pushEvent(testEvent);
    }

    // Validate it's OPEN
    assertEquals(CircuitBreaker.State.OPEN, circuitBreakerClient.getCircuitBreakerState());

    // STEP 2: Wait for breaker to transition to HALF_OPEN
    Thread.sleep(1100); // Wait slightly more than waitDurationInOpenState

    // STEP 3: Allow success on HALF_OPEN calls
    reset(mockDelegate); // Clear failure behavior
    doNothing().when(mockDelegate).pushEvent(any(TelemetryRequest.class));

    for (int i = 0; i < 3; i++) {
      circuitBreakerClient.pushEvent(testEvent);
    }

    // STEP 4: Validate that breaker transitions back to CLOSED
    assertEquals(CircuitBreaker.State.CLOSED, circuitBreakerClient.getCircuitBreakerState());
  }

  @Test
  void testCircuitBreakerOpensOnTelemetryExceptionFailures() throws Exception {
    // Given: delegate throws DatabricksTelemetryException (e.g., representing 429 mapping)
    doThrow(new DatabricksTelemetryException("rate-limited"))
        .when(mockDelegate)
        .pushEvent(any(TelemetryRequest.class));

    // When: making multiple calls that fail
    for (int i = 0; i < 20; i++) {
      circuitBreakerClient.pushEvent(testEvent);
    }

    // Then: circuit breaker should open after enough failures
    assertEquals(CircuitBreaker.State.OPEN, circuitBreakerClient.getCircuitBreakerState());
  }

  @Test
  void testMetricsAreAvailable() {
    // When: getting metrics
    CircuitBreaker.Metrics metrics = circuitBreakerClient.getCircuitBreakerMetrics();

    // Then: metrics should not be null
    assertNotNull(metrics);
    assertEquals(0, metrics.getNumberOfFailedCalls());
    assertEquals(0, metrics.getNumberOfSuccessfulCalls());
  }
}
