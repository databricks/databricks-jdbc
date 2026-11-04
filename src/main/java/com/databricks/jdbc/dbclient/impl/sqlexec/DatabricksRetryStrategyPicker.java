package com.databricks.jdbc.dbclient.impl.sqlexec;

import com.databricks.jdbc.common.util.NetworkRetryUtil;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.retry.IdempotentRequestRetryStrategy;
import com.databricks.sdk.core.retry.NonIdempotentRequestRetryStrategy;
import com.databricks.sdk.core.retry.RetryStrategy;
import com.databricks.sdk.core.retry.RetryStrategyPicker;
import java.io.IOException;
import java.util.regex.Pattern;

/** Selects SEA retry policies using paths, before the SDK prepends the workspace host. */
final class DatabricksRetryStrategyPicker implements RetryStrategyPicker {
  private static final RetryStrategy IDEMPOTENT = new IdempotentRequestRetryStrategy();
  private static final RetryStrategy NON_IDEMPOTENT = new NonIdempotentRequestRetryStrategy();
  private static final String SQL_PATH = "/api/2\\.0/sql/";
  private static final Pattern SESSION_COLLECTION =
      Pattern.compile(SQL_PATH + "(?:(?:statements/)?sessions|warehouses/[^/]+/sessions)/?");
  private static final Pattern SESSION =
      Pattern.compile(SQL_PATH + "(?:(?:statements/)?sessions|warehouses/[^/]+/sessions)/[^/]+/?");
  private static final String STATEMENT_PATH =
      SQL_PATH + "(?:warehouses/[^/]+/sessions/[^/]+/)?statements/[^/]+";
  private static final Pattern STATEMENT = Pattern.compile(STATEMENT_PATH + "/?");
  private static final Pattern STATEMENT_READ =
      Pattern.compile(STATEMENT_PATH + "(?:/status|/result/chunks/[^/]+)?/?");
  private static final Pattern CANCEL = Pattern.compile(STATEMENT_PATH + "/cancel/?");

  @Override
  public RetryStrategy getRetryStrategy(Request request) {
    String path = request.getUri().getPath();
    boolean idempotent;
    switch (request.getMethod()) {
      case Request.GET:
        idempotent = STATEMENT_READ.matcher(path).matches();
        break;
      case Request.DELETE:
        idempotent = SESSION.matcher(path).matches() || STATEMENT.matcher(path).matches();
        break;
      case Request.POST:
        idempotent = SESSION_COLLECTION.matcher(path).matches() || CANCEL.matcher(path).matches();
        break;
      default:
        idempotent = false;
    }
    RetryStrategy delegate = idempotent ? IDEMPOTENT : NON_IDEMPOTENT;
    return error -> {
      // SEA redirects can signal protocol selection and must reach the caller immediately.
      if (error.getStatusCode() >= 300 && error.getStatusCode() < 400) {
        return false;
      }
      if (error.getCause() instanceof IOException) {
        if (NetworkRetryUtil.isRetryableConnectionFailure(error.getCause())) {
          return true;
        }
        // Only idempotent requests can replay an ambiguous read/write failure.
        return idempotent
            && !NetworkRetryUtil.isSslFailure(error.getCause())
            && delegate.isRetriable(error);
      }
      return delegate.isRetriable(error);
    };
  }
}
