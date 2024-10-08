package com.databricks.jdbc.dbclient.impl.thrift;

import com.databricks.jdbc.common.util.ValidationUtil;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class DatabricksHttpTTransport extends TTransport {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksHttpTTransport.class);
  private final DatabricksHttpClient httpClient;
  private final String url;
  private Map<String, String> customHeaders = Collections.emptyMap();
  private final ByteArrayOutputStream requestBuffer;
  private InputStream inputStream = null;
  private CloseableHttpResponse response = null;
  private static final Map<String, String> DEFAULT_HEADERS =
      Collections.unmodifiableMap(getDefaultHeaders());

  public DatabricksHttpTTransport(DatabricksHttpClient httpClient, String url) {
    this.httpClient = httpClient;
    this.httpClient.closeExpiredAndIdleConnections();
    this.url = url;
    this.requestBuffer = new ByteArrayOutputStream();
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
    this.httpClient.closeExpiredAndIdleConnections();
    if (inputStream != null) {
      try {
        inputStream.close();
      } catch (IOException e) {
        LOGGER.error(
            String.format("Failed to close inputStream with error {%s}. Skipping the close.", e));
      }
      inputStream = null;
    }
    if (response != null) {
      try {
        response.close();
      } catch (IOException e) {
        LOGGER.error(String.format("Failed to close response with error {%s}", e.toString()));
      }
      response = null;
    }
  }

  public void setCustomHeaders(Map<String, String> headers) {
    if (headers != null) {
      customHeaders = new HashMap<>(headers);
    } else {
      customHeaders = Collections.emptyMap();
    }
  }

  @VisibleForTesting
  Map<String, String> getCustomHeaders() {
    return customHeaders;
  }

  @VisibleForTesting
  InputStream getInputStream() {
    return inputStream;
  }

  @VisibleForTesting
  void setInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (inputStream == null) {
      throw new TTransportException("Response buffer is empty, no request.");
    }
    try {
      int ret = inputStream.read(buf, off, len);
      if (ret == -1) {
        throw new TTransportException("No more data available.");
      }
      return ret;
    } catch (IOException e) {
      LOGGER.error(String.format("Failed to read inputStream with error {%s}", e.toString()));
      throw new TTransportException(e);
    }
  }

  @Override
  public void write(byte[] buf, int off, int len) {
    requestBuffer.write(buf, off, len);
  }

  @Override
  public void flush() throws TTransportException {
    try {
      HttpPost request = new HttpPost(this.url);
      DEFAULT_HEADERS.forEach(request::addHeader);
      if (customHeaders != null) {
        customHeaders.forEach(request::addHeader);
      }
      request.setEntity(new ByteArrayEntity(requestBuffer.toByteArray()));
      response = httpClient.execute(request);
      ValidationUtil.checkHTTPError(response);
      inputStream = response.getEntity().getContent();
      requestBuffer.reset();
    } catch (DatabricksHttpException | IOException e) {
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof DatabricksHttpException) {
          throw new TTransportException(
              TTransportException.UNKNOWN, "Failed to flush data to server: " + cause.getMessage());
        }
        cause = cause.getCause();
      }
      httpClient.closeExpiredAndIdleConnections();

      String errorMessage = "Failed to flush data to server: " + e.getMessage();
      LOGGER.error(errorMessage);
      throw new TTransportException(TTransportException.UNKNOWN, errorMessage);
    }
  }

  @Override
  public TConfiguration getConfiguration() {
    return null;
  }

  @Override
  public void updateKnownMessageSize(long size) throws TTransportException {}

  @Override
  public void checkReadBytesAvailable(long numBytes) throws TTransportException {}

  private static Map<String, String> getDefaultHeaders() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/x-thrift");
    headers.put("Accept", "application/x-thrift");
    return headers;
  }
}
