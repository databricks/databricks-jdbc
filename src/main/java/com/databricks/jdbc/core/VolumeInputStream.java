package com.databricks.jdbc.core;

import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.LoggingUtil;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;

public class VolumeInputStream extends InputStream {

  private final InputStream httpContent;
  private final IExecutionResult resultHandler;
  private final IDatabricksStatement statement;
  private final HttpEntity httpEntity;

  VolumeInputStream(
      HttpEntity httpEntity, IExecutionResult resultHandler, IDatabricksStatement statement)
      throws IOException {
    this.httpContent = httpEntity.getContent();
    this.httpEntity = httpEntity;
    this.resultHandler = resultHandler;
    this.statement = statement;
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
    EntityUtils.consume(httpEntity);
    try {
      this.resultHandler.close();
      this.statement.close(true);
    } catch (SQLException e) {
      // Ignore exception while closing
      LoggingUtil.log(
          LogLevel.ERROR,
          "Exception while release volume resources: " + e.getMessage(),
          VolumeInputStream.class.getName());
    }
  }
}
