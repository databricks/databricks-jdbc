package com.databricks.jdbc.common.util;

import java.net.BindException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.conn.ConnectTimeoutException;

public final class NetworkRetryUtil {
  private NetworkRetryUtil() {}

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
          && "Network is unreachable".equalsIgnoreCase(cause.getMessage())) {
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
