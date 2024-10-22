package com.databricks.jdbc.api.impl.volume;

import java.io.IOException;
import java.io.InputStream;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;

public class VolumeInputStream extends InputStream {

  private final InputStream httpContent;
  private final HttpEntity httpEntity;

  public VolumeInputStream(HttpEntity httpEntity) throws IOException {
    this.httpContent = httpEntity.getContent();
    this.httpEntity = httpEntity;
  }

  @Override
  public int available() throws IOException {
    return httpContent.available();
  }

  @Override
  public void mark(int readLimit) {
    this.httpContent.mark(readLimit);
  }

  @Override
  public boolean markSupported() {
    return this.httpContent.markSupported();
  }

  @Override
  public int read() throws IOException {
    return httpContent.read();
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    return httpContent.read(bytes);
  }

  @Override
  public int read(byte[] bytes, int off, int len) throws IOException {
    return httpContent.read(bytes, off, len);
  }

  @Override
  public void reset() throws IOException {
    this.httpContent.reset();
  }

  @Override
  public long skip(long n) throws IOException {
    return this.httpContent.skip(n);
  }

  @Override
  public void close() throws IOException {
    // Make sure close the stream
    EntityUtils.consume(httpEntity);
  }
}
