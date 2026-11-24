package com.databricks.jdbc.dbclient.impl.sqlexec;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.utils.Timer;
import java.io.IOException;
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
  private RetryBudget rateLimitBudget;
  private RetryBudget unavailableBudget;
  private long attempts;

  CreateSessionRetryHandler(IDatabricksConnectionContext context) {
    this(context, TIMER, () -> ThreadLocalRandom.current().nextDouble(0.8, 1.2));
  }

  CreateSessionRetryHandler(
      IDatabricksConnectionContext context, Timer timer, DoubleSupplier jitter) {
    this.context = context;
    this.timer = timer;
    this.jitter = jitter;
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
      long delay =
          ApiClient.getBackoffFromRetryAfterHeader(response)
              .filter(value -> value > 0)
              .orElseGet(
                  () -> (long) ((1000L << Math.min(budget.retries, 5)) * jitter.getAsDouble()));
      if (delay > budget.timeoutMillis - (timer.getCurrentTime() - budget.started)) {
        warnExhausted(budget, status);
        return response;
      }
      try {
        timer.sleep(delay);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return response;
      }
      if (timer.getCurrentTime() - budget.started >= budget.timeoutMillis) {
        warnExhausted(budget, status);
        return response;
      }
      budget.retries = Math.min(budget.retries + 1, 5);
      if (response.getBody() != null) {
        try {
          response.getBody().close();
        } catch (IOException exception) {
          LOGGER.debug("Unable to close retried CreateSession response", exception);
        }
      }
      request.withHeaders(config.authenticate());
    }
  }

  private void warnExhausted(RetryBudget budget, int status) {
    LOGGER.warn(
        "CreateSession retry budget exhausted: attempts={}, elapsedMs={}, budgetMs={}, lastStatus={}",
        attempts,
        timer.getCurrentTime() - budget.started,
        budget.timeoutMillis,
        status);
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
