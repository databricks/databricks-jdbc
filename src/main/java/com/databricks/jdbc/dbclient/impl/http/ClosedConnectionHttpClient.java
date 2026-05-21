package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.concurrent.Future;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

/**
 * Sentinel HTTP client returned for connections that have been closed. All operations throw {@link
 * DatabricksHttpException} with a clear message, so callers that accidentally store and use a
 * post-close client get an immediate, diagnosable failure instead of a silent null.
 *
 * <p>Symmetric with {@link com.databricks.jdbc.telemetry.NoopTelemetryClient} which serves the same
 * role for telemetry clients.
 */
public final class ClosedConnectionHttpClient implements IDatabricksHttpClient {

  static final ClosedConnectionHttpClient INSTANCE = new ClosedConnectionHttpClient();

  private ClosedConnectionHttpClient() {}

  @Override
  public CloseableHttpResponse execute(HttpUriRequest request) throws DatabricksHttpException {
    throw new DatabricksHttpException(
        "Connection has been closed; HTTP client is no longer usable",
        DatabricksDriverErrorCode.INVALID_STATE);
  }

  @Override
  public CloseableHttpResponse execute(HttpUriRequest request, boolean supportGzipEncoding)
      throws DatabricksHttpException {
    throw new DatabricksHttpException(
        "Connection has been closed; HTTP client is no longer usable",
        DatabricksDriverErrorCode.INVALID_STATE);
  }

  @Override
  public <T> Future<T> executeAsync(
      AsyncRequestProducer requestProducer,
      AsyncResponseConsumer<T> responseConsumer,
      FutureCallback<T> callback) {
    throw new IllegalStateException("Connection has been closed; HTTP client is no longer usable");
  }
}
