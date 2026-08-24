package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;

/** In-memory {@link Clob} used for JDBC CLOB construction and parameter binding. */
public final class DatabricksClob implements Clob {
  private StringBuilder value = new StringBuilder();

  @Override
  public long length() throws SQLException {
    ensureOpen();
    return value.length();
  }

  @Override
  public String getSubString(long pos, int length) throws SQLException {
    ensureOpen();
    if (length < 0) {
      throw validationError("Invalid CLOB substring length: " + length);
    }
    int start = substringPosition(pos, length);
    int end = (int) Math.min((long) start + length, value.length());
    return value.substring(start, end);
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    ensureOpen();
    return new StringReader(value.toString());
  }

  @Override
  public InputStream getAsciiStream() throws SQLException {
    ensureOpen();
    return new ByteArrayInputStream(value.toString().getBytes(StandardCharsets.US_ASCII));
  }

  @Override
  public long position(String searchstr, long start) throws SQLException {
    ensureOpen();
    if (searchstr == null) {
      throw validationError("CLOB search string cannot be null");
    }
    if (start < 1) {
      throw validationError("CLOB search position must be at least 1");
    }
    if (start > (long) value.length() + 1) {
      return -1;
    }
    int result = value.indexOf(searchstr, (int) start - 1);
    return result < 0 ? -1 : result + 1L;
  }

  @Override
  public long position(Clob searchstr, long start) throws SQLException {
    ensureOpen();
    if (searchstr == null) {
      throw validationError("CLOB search value cannot be null");
    }
    if (start < 1) {
      throw validationError("CLOB search position must be at least 1");
    }
    long searchLength = searchstr.length();
    if (searchLength > value.length()) {
      return -1;
    }
    if (searchLength == 0) {
      return position("", start);
    }
    return position(searchstr.getSubString(1, (int) searchLength), start);
  }

  @Override
  public int setString(long pos, String str) throws SQLException {
    ensureOpen();
    if (str == null) {
      throw validationError("CLOB value cannot be null");
    }
    return setString(pos, str, 0, str.length());
  }

  @Override
  public int setString(long pos, String str, int offset, int len) throws SQLException {
    ensureOpen();
    if (str == null) {
      throw validationError("CLOB value cannot be null");
    }
    if (offset < 0 || len < 0 || (long) offset + len > str.length()) {
      throw validationError("Invalid CLOB string range");
    }
    int start = writePosition(pos);
    String replacement = str.substring(offset, offset + len);
    int end = (int) Math.min((long) start + len, value.length());
    long resultingLength = (long) value.length() - (end - start) + replacement.length();
    if (resultingLength > Integer.MAX_VALUE) {
      throw validationError("CLOB value is too large");
    }
    value.replace(start, end, replacement);
    return len;
  }

  @Override
  public OutputStream setAsciiStream(long pos) throws SQLException {
    ensureOpen();
    writePosition(pos);
    return new OutputStream() {
      private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      private long nextPosition = pos;
      private boolean closed;

      @Override
      public void write(int b) throws IOException {
        ensureNotClosed();
        buffer.write(b);
      }

      @Override
      public void write(byte[] bytes, int offset, int length) throws IOException {
        ensureNotClosed();
        buffer.write(bytes, offset, length);
      }

      @Override
      public void flush() throws IOException {
        ensureNotClosed();
        if (buffer.size() == 0) {
          return;
        }
        String pending = buffer.toString(StandardCharsets.US_ASCII);
        try {
          DatabricksClob.this.setString(nextPosition, pending);
        } catch (SQLException e) {
          throw new IOException("Unable to write CLOB ASCII stream", e);
        }
        nextPosition += pending.length();
        buffer.reset();
      }

      @Override
      public void close() throws IOException {
        if (closed) {
          return;
        }
        flush();
        closed = true;
      }

      private void ensureNotClosed() throws IOException {
        if (closed) {
          throw new IOException("CLOB ASCII stream is closed");
        }
      }
    };
  }

  @Override
  public Writer setCharacterStream(long pos) throws SQLException {
    ensureOpen();
    writePosition(pos);
    return new Writer() {
      private final StringBuilder buffer = new StringBuilder();
      private long nextPosition = pos;
      private boolean closed;

      @Override
      public void write(char[] cbuf, int off, int len) throws IOException {
        if (closed) {
          throw new IOException("CLOB character stream is closed");
        }
        buffer.append(cbuf, off, len);
      }

      @Override
      public void flush() throws IOException {
        if (closed) {
          throw new IOException("CLOB character stream is closed");
        }
        if (buffer.length() == 0) {
          return;
        }
        String pending = buffer.toString();
        try {
          DatabricksClob.this.setString(nextPosition, pending);
        } catch (SQLException e) {
          throw new IOException("Unable to write CLOB character stream", e);
        }
        nextPosition += pending.length();
        buffer.setLength(0);
      }

      @Override
      public void close() throws IOException {
        if (closed) {
          return;
        }
        flush();
        closed = true;
      }
    };
  }

  @Override
  public void truncate(long len) throws SQLException {
    ensureOpen();
    if (len < 0 || len > value.length()) {
      throw validationError("Invalid CLOB truncate length: " + len);
    }
    value.setLength((int) len);
  }

  @Override
  public void free() {
    value = null;
  }

  @Override
  public Reader getCharacterStream(long pos, long length) throws SQLException {
    ensureOpen();
    int start = readPosition(pos);
    if (length < 0 || length > Integer.MAX_VALUE || (long) start + length > value.length()) {
      throw validationError("Invalid CLOB stream range");
    }
    return new StringReader(value.substring(start, start + (int) length));
  }

  private int readPosition(long pos) throws SQLException {
    if (pos < 1 || pos > value.length() || pos > Integer.MAX_VALUE) {
      throw validationError("Invalid CLOB position: " + pos);
    }
    return (int) pos - 1;
  }

  private int substringPosition(long pos, int length) throws SQLException {
    long maximumPosition = (long) value.length() + (length == 0 ? 1 : 0);
    if (pos < 1 || pos > maximumPosition || pos > Integer.MAX_VALUE) {
      throw validationError("Invalid CLOB position: " + pos);
    }
    return (int) pos - 1;
  }

  private int writePosition(long pos) throws SQLException {
    if (pos < 1 || pos > (long) value.length() + 1 || pos > Integer.MAX_VALUE) {
      throw validationError("Invalid CLOB position: " + pos);
    }
    return (int) pos - 1;
  }

  private void ensureOpen() throws SQLException {
    if (value == null) {
      throw new DatabricksSQLException(
          "CLOB has been freed",
          DatabricksDriverErrorCode.INVALID_STATE.name(),
          DatabricksDriverErrorCode.INVALID_STATE);
    }
  }

  private static DatabricksSQLException validationError(String message) {
    return new DatabricksSQLException(
        message,
        DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR.name(),
        DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
  }
}
