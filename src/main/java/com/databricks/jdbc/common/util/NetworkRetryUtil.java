package com.databricks.jdbc.common.util;

import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import java.net.BindException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.conn.ConnectTimeoutException;

public final class NetworkRetryUtil {
  private static final int DEFAULT_RETRY_TIMEOUT_SECONDS =
      Integer.parseInt(DatabricksJdbcUrlParams.SOCKET_TIMEOUT.getDefaultValue());

  private NetworkRetryUtil() {}

  public static boolean hasRetryTimeRemaining(
      long elapsedNanos, long delayMillis, int socketTimeoutSeconds) {
    int timeoutSeconds =
        socketTimeoutSeconds > 0 ? socketTimeoutSeconds : DEFAULT_RETRY_TIMEOUT_SECONDS;
    return elapsedNanos
        < TimeUnit.SECONDS.toNanos(timeoutSeconds) - TimeUnit.MILLISECONDS.toNanos(delayMillis);
  }

  /** Connection failures that occur before a statement can reach the server. */
  public static boolean isRetryableConnectionFailure(Throwable exception) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = exception;
        cause != null && visited.add(cause);
        cause = cause.getCause()) {
      if (cause instanceof ConnectException
          || cause instanceof ConnectTimeoutException
          || cause instanceof NoRouteToHostException
          || cause instanceof PortUnreachableException
          || cause instanceof BindException
          || cause instanceof UnknownHostException) {
        return true;
      }
      if (cause instanceof SocketException
          && ("Network is unreachable".equalsIgnoreCase(cause.getMessage())
              || "Network is unreachable (connect failed)".equalsIgnoreCase(cause.getMessage()))) {
        return true;
      }
      if (cause instanceof SSLHandshakeException
          && "Remote host terminated the handshake".equalsIgnoreCase(cause.getMessage())) {
        return true;
      }
    }
    return false;
  }

  public static boolean isSslFailure(Throwable exception) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable cause = exception;
        cause != null && visited.add(cause);
        cause = cause.getCause()) {
      if (cause instanceof SSLException) {
        return true;
      }
    }
    return false;
  }
}
