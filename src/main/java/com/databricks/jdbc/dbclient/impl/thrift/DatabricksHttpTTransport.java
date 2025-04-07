package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.common.util.DatabricksAuthUtil.initializeConfigWithToken;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.ValidationUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.TracingUtil;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class DatabricksHttpTTransport extends TTransport {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksHttpTTransport.class);
  private static final Map<String, String> DEFAULT_HEADERS =
      Map.of(
          "Content-Type", "application/x-thrift",
          "Accept", "application/x-thrift");
  private final IDatabricksHttpClient httpClient;
  private final String url;
  private Map<String, String> customHeaders = Collections.emptyMap();
  private final ByteArrayOutputStream requestBuffer;
  private ByteArrayInputStream responseBuffer;
  private final IDatabricksConnectionContext connectionContext;
  DatabricksConfig databricksConfig;

  public DatabricksHttpTTransport(
      IDatabricksHttpClient httpClient,
      String url,
      DatabricksConfig databricksConfig,
      IDatabricksConnectionContext connectionContext) {
    this.httpClient = httpClient;
    this.url = url;
    this.requestBuffer = new ByteArrayOutputStream();
    this.responseBuffer = null;
    this.databricksConfig = databricksConfig;
    this.connectionContext = connectionContext;
  }

  @Override
  public boolean isOpen() {
    // HTTP Client doesn't maintain an open connection.
    return true;
  }

  @Override
  public void open() throws TTransportException {
    // Opening is not required for HTTP transport
  }

  @Override
  public void close() {
    // No-op
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (responseBuffer == null) {
      LOGGER.error("Response buffer is empty, no response.");
      throw new TTransportException("Response buffer is empty, no response.");
    }
    int numBytes = responseBuffer.read(buf, off, len);
    if (numBytes == -1) {
      LOGGER.error("No data available to read.");
      throw new TTransportException("No more data available.");
    }
    return numBytes;
  }

  @Override
  public void write(byte[] buf, int off, int len) {
    requestBuffer.write(buf, off, len);
  }

  @Override
  public void flush() throws TTransportException {
    refreshHeadersIfRequired();

    HttpPost request = new HttpPost(this.url);
    DEFAULT_HEADERS.forEach(request::addHeader);
    customHeaders.forEach(request::addHeader);

    // Overriding with URL defined headers
    this.connectionContext.getCustomHeaders().forEach(request::setHeader);

    if (connectionContext.isRequestTracingEnabled()) {
      String traceHeader = TracingUtil.getTraceHeader();
      LOGGER.debug("Thrift tracing header: " + traceHeader);

      request.addHeader(TracingUtil.TRACE_HEADER, traceHeader);
    }

    // Set the request entity
    request.setEntity(new ByteArrayEntity(requestBuffer.toByteArray()));

    // Execute the request and handle the response
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      ValidationUtil.checkHTTPError(response);

      // Read the response
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        byte[] responseBytes = EntityUtils.toByteArray(entity);
        responseBuffer = new ByteArrayInputStream(responseBytes);
      }
    } catch (DatabricksHttpException | IOException e) {
      String errorMessage = "Failed to flush data to server: " + e.getMessage();
      LOGGER.error(e, errorMessage);
      throw new TTransportException(TTransportException.UNKNOWN, errorMessage);
    }

    // Reset the request buffer
    requestBuffer.reset();
  }

  @Override
  public TConfiguration getConfiguration() {
    return null;
  }

  @Override
  public void updateKnownMessageSize(long size) throws TTransportException {}

  @Override
  public void checkReadBytesAvailable(long numBytes) throws TTransportException {}

  /** Refreshes the custom headers by re-authenticating if necessary. */
  private void refreshHeadersIfRequired() {
    Map<String, String> refreshedHeaders = databricksConfig.authenticate();
    customHeaders =
        refreshedHeaders != null ? new HashMap<>(refreshedHeaders) : Collections.emptyMap();
  }

  void resetAccessToken(String newAccessToken) {
    this.databricksConfig = initializeConfigWithToken(newAccessToken, databricksConfig);
    this.databricksConfig.resolve();
  }

  @VisibleForTesting
  void setResponseBuffer(ByteArrayInputStream responseBuffer) {
    this.responseBuffer = responseBuffer;
  }
}
