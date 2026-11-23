package com.databricks.jdbc.dbclient.impl.common;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpClientConnection;
import org.apache.http.config.Registry;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/** Cancels only the connection leased by the current status poll. */
final class PollingConnectionManager extends PoolingHttpClientConnectionManager {
  PollingConnectionManager() {}

  PollingConnectionManager(Registry<ConnectionSocketFactory> registry) {
    super(registry);
  }

  @Override
  public ConnectionRequest requestConnection(HttpRoute route, Object state) {
    ConnectionRequest request = super.requestConnection(route, state);
    return new ConnectionRequest() {
      @Override
      public boolean cancel() {
        return request.cancel();
      }

      @Override
      public HttpClientConnection get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, ConnectionPoolTimeoutException {
        if (!PollingDeadline.registerAbort(request::cancel)) {
          throw new InterruptedException("Status poll deadline expired");
        }
        HttpClientConnection connection = request.get(timeout, unit);
        boolean active =
            PollingDeadline.registerAbort(
                () -> {
                  try {
                    connection.shutdown();
                  } catch (IOException ignored) {
                  }
                });
        if (!active) {
          releaseConnection(connection, state, 0, TimeUnit.MILLISECONDS);
          throw new InterruptedException("Status poll deadline expired");
        }
        return connection;
      }
    };
  }

  @Override
  public void releaseConnection(
      HttpClientConnection connection, Object state, long keepAlive, TimeUnit unit) {
    PollingDeadline.clearAbort();
    super.releaseConnection(connection, state, keepAlive, unit);
  }
}
