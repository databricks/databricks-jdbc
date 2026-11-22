package com.databricks.jdbc.dbclient.impl.sqlexec;

import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.retry.IdempotentRequestRetryStrategy;
import com.databricks.sdk.core.retry.RequestBasedRetryStrategyPicker;
import com.databricks.sdk.core.retry.RetryStrategy;
import com.databricks.sdk.core.retry.RetryStrategyPicker;
import java.util.regex.Pattern;
import org.apache.http.HttpStatus;

/** Extends the SDK's request classification to warehouse-scoped SQL Execution API paths. */
final class WarehouseRetryStrategyPicker implements RetryStrategyPicker {
  private static final RetryStrategy IDEMPOTENT = new IdempotentRequestRetryStrategy();
  private static final RetryStrategy CREATE_SESSION_RETRY =
      new IdempotentRequestRetryStrategy() {
        @Override
        public boolean isRetriable(DatabricksError error) {
          // JDBC handles the backend-negotiation redirect as a terminal CreateSession response.
          return error.getStatusCode() != HttpStatus.SC_TEMPORARY_REDIRECT
              && super.isRetriable(error);
        }
      };

  private final String host;
  private final RetryStrategyPicker delegate;
  private final Pattern createSession;
  private final Pattern deleteSession;
  private final Pattern statement;
  private final Pattern getStatement;

  WarehouseRetryStrategyPicker(String host) {
    this.host = host;
    this.delegate = new RequestBasedRetryStrategyPicker(host);
    String sessions = Pattern.quote(host) + "/api/2.0/sql/warehouses/[^/?#]+/sessions";
    String session = sessions + "/[^/?#]+";
    String statementPath = session + "/statements/[^/?#]+";
    createSession = Pattern.compile(sessions + "/?", Pattern.CASE_INSENSITIVE);
    deleteSession = Pattern.compile(session, Pattern.CASE_INSENSITIVE);
    statement = Pattern.compile(statementPath, Pattern.CASE_INSENSITIVE);
    getStatement =
        Pattern.compile(statementPath + "(?:/status|/result/chunks/.*)?", Pattern.CASE_INSENSITIVE);
  }

  @Override
  public RetryStrategy getRetryStrategy(Request request) {
    String url = request.getUrl();
    // ApiClient selects the strategy before it prepends the host to relative request URLs.
    if (url.startsWith("/")) {
      url = host + url;
    }
    String method = request.getMethod();
    if (Request.POST.equals(method) && createSession.matcher(url).matches()) {
      return CREATE_SESSION_RETRY;
    }
    if ((Request.DELETE.equals(method)
            && (deleteSession.matcher(url).matches() || statement.matcher(url).matches()))
        || (Request.GET.equals(method) && getStatement.matcher(url).matches())) {
      return IDEMPOTENT;
    }
    return delegate.getRetryStrategy(request);
  }
}
