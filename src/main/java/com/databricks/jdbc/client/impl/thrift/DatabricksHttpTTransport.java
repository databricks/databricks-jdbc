package com.databricks.jdbc.client.impl.thrift;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class DatabricksHttpTTransport extends TTransport {
  private final DatabricksHttpClient httpClient;
  private final String url;
  private Map<String, String> customHeaders = Collections.emptyMap();
  private final ByteArrayOutputStream requestBuffer;
  private InputStream inputStream = null;
  private static final Map<String, String> DEFAULT_HEADERS =
      Collections.unmodifiableMap(getDefaultHeaders());

  public DatabricksHttpTTransport(DatabricksHttpClient httpClient, String url) {
    this.httpClient = httpClient;
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
    if (null != inputStream) {
      try {
        inputStream.close();
      } catch (IOException e) {
      }
      inputStream = null;
    }
  }

  public void setCustomHeaders(Map<String, String> headers) {
    if (headers != null) {
      customHeaders = new HashMap<>(headers);
    } else {
      customHeaders = Collections.emptyMap();
    }
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (inputStream == null) {
      throw new TTransportException("Response buffer is empty, no request.");
    }

    checkReadBytesAvailable(len);

    try {
      int ret = inputStream.read(buf, off, len);
      if (ret == -1) {
        throw new TTransportException("No more data available.");
      }
      return ret;
    } catch (IOException iox) {
      throw new TTransportException(iox);
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
      HttpResponse response = httpClient.execute(request);
      requestBuffer.reset();
    } catch (DatabricksHttpException e) {
      throw new TTransportException(
          TTransportException.UNKNOWN, "Failed to flush data to server: " + e.getMessage());
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
    headers.put("User-Agent", "Java/THttpClient/HC");
    return headers;
  }
}
