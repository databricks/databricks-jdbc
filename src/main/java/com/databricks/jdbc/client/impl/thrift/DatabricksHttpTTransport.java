package com.databricks.jdbc.client.impl.thrift;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class DatabricksHttpTTransport extends TTransport {
  private final DatabricksHttpClient httpClient;
  private final String url;
  private ByteArrayOutputStream requestBuffer;
  private byte[] responseBuffer;
  private int responseBufferPosition;

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
    // Closing is not required for stateless HTTP connections.
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (responseBuffer == null) {
      throw new TTransportException(
          TTransportException.NOT_OPEN, "Response buffer is empty, did you forget to call flush?");
    }

    int bytesToRead = Math.min(len, responseBuffer.length - responseBufferPosition);
    System.arraycopy(responseBuffer, responseBufferPosition, buf, off, bytesToRead);
    responseBufferPosition += bytesToRead;
    return bytesToRead;
  }

  @Override
  public void write(byte[] buf, int off, int len) {
    requestBuffer.write(buf, off, len);
  }

  @Override
  public void flush() throws TTransportException {
    try {
      HttpPost request = new HttpPost(this.url);
      request.setEntity(new ByteArrayEntity(requestBuffer.toByteArray()));
      // Set additional headers or configurations if required
      HttpResponse response = httpClient.execute(request);
      responseBuffer = EntityUtils.toByteArray(response.getEntity());
      responseBufferPosition = 0;
      requestBuffer.reset();
    } catch (IOException | DatabricksHttpException e) {
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
}
