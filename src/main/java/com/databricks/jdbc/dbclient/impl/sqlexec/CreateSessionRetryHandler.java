package com.databricks.jdbc.dbclient.impl.sqlexec;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.utils.Timer;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import org.apache.http.HttpStatus;

final class CreateSessionRetryHandler {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(CreateSessionRetryHandler.class);
  private static final Timer TIMER =
      new Timer() {
        @Override
        public void sleep(long milliseconds) throws InterruptedException {
          Thread.sleep(milliseconds);
        }

        @Override
        public long getCurrentTime() {
          return System.nanoTime() / 1_000_000;
        }
      };

  private final IDatabricksConnectionContext context;
  private final Timer timer;
  private final DoubleSupplier jitter;
  private final String warehouseId;
  private RetryBudget rateLimitBudget;
  private RetryBudget unavailableBudget;
  private long attempts;
  private boolean exhaustionLogged;

  CreateSessionRetryHandler(IDatabricksConnectionContext context, String warehouseId) {
    this(context, TIMER, () -> ThreadLocalRandom.current().nextDouble(0.8, 1.2), warehouseId);
  }

  CreateSessionRetryHandler(
      IDatabricksConnectionContext context,
      Timer timer,
      DoubleSupplier jitter,
      String warehouseId) {
    this.context = context;
    this.timer = timer;
    this.jitter = jitter;
    this.warehouseId = warehouseId;
  }

  Response execute(HttpClient transport, Request request, DatabricksConfig config)
      throws IOException {
    while (true) {
      attempts++;
      Response response = transport.execute(request);
      int status = response.getStatusCode();
      RetryBudget budget;
      if (status == HttpStatus.SC_TOO_MANY_REQUESTS && context.shouldRetryRateLimitError()) {
        if (rateLimitBudget == null) {
          rateLimitBudget =
              new RetryBudget(timer.getCurrentTime(), context.getRateLimitRetryTimeout());
        }
        budget = rateLimitBudget;
      } else if (status == HttpStatus.SC_SERVICE_UNAVAILABLE
          && context.shouldRetryTemporarilyUnavailableError()) {
        if (unavailableBudget == null) {
          unavailableBudget =
              new RetryBudget(
                  timer.getCurrentTime(), context.getTemporarilyUnavailableRetryTimeout());
        }
        budget = unavailableBudget;
      } else {
        return response;
      }
      Optional<Long> retryAfter =
          ApiClient.getBackoffFromRetryAfterHeader(response).filter(value -> value > 0);
      long delay =
          retryAfter.orElseGet(
              () -> (long) ((1000L << Math.min(budget.retries, 5)) * jitter.getAsDouble()));
      long elapsed = timer.getCurrentTime() - budget.started;
      long remaining = budget.timeoutMillis - elapsed;
      if (delay > remaining) {
        exhaustionLogged = true;
        LOGGER.warn(
            "CreateSession {}: attempts={}, elapsedMs={}, budgetMs={}, lastStatus={}, delayMs={}, remainingMs={}, warehouseId={}",
            retryAfter.isPresent()
                ? "Retry-After exceeds remaining retry budget"
                : "retry budget exhausted",
            attempts,
            elapsed,
            budget.timeoutMillis,
            status,
            delay,
            remaining,
            warehouseId);
        return response;
      }
      LOGGER.debug(
          "Retrying CreateSession: status={}, delayMs={}, attempt={}, remainingMs={}, warehouseId={}",
          status,
          delay,
          attempts + 1,
          remaining,
          warehouseId);
      if (response.getBody() != null) {
        try {
          response.getBody().close();
        } catch (IOException exception) {
          LOGGER.debug(
              "Unable to close retried CreateSession response: {}",
              exception.getClass().getSimpleName());
        }
      }
      try {
        timer.sleep(delay);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        LOGGER.debug(
            "Current thread was interrupted during CreateSession retry: warehouseId={}",
            warehouseId);
        throw new DatabricksException("Current thread was interrupted", exception);
      }
      budget.retries = Math.min(budget.retries + 1, 5);
      request.withHeaders(config.authenticate());
    }
  }

  boolean hasLoggedExhaustion() {
    return exhaustionLogged;
  }

  private static final class RetryBudget {
    final long started;
    final long timeoutMillis;
    int retries;

    RetryBudget(long started, int timeoutSeconds) {
      this.started = started;
      this.timeoutMillis = timeoutSeconds * 1000L;
    }
  }
}
