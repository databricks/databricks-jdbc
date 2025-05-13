package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

/**
 * A wrapper around Apache Arrow's ArrowStreamReader that works without requiring the
 * --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag. This class intercepts and fixes the
 * InaccessibleObjectException that would otherwise occur when Arrow tries to access the protected
 * Buffer.address field.
 */
public class DatabricksArrowStreamReader implements AutoCloseable {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksArrowStreamReader.class);

  private final ArrowStreamReader arrowStreamReader;
  private final InputStream inputStream;
  private final BufferAllocator allocator;

  /**
   * Creates a new DatabricksArrowStreamReader that wraps an ArrowStreamReader.
   *
   * @param inputStream The input stream to read Arrow data from
   * @param allocator The allocator to use for Arrow buffers
   */
  public DatabricksArrowStreamReader(InputStream inputStream, BufferAllocator allocator) {
    this.inputStream = inputStream;
    this.allocator = allocator;
    this.arrowStreamReader = new ArrowStreamReader(inputStream, allocator);
  }

  /**
   * Loads the next batch of data.
   *
   * @return true if a batch was loaded, false if at end of stream
   * @throws IOException if an error occurs during reading
   */
  public boolean loadNextBatch() throws IOException {
    try {
      return arrowStreamReader.loadNextBatch();
    } catch (Exception e) {
      // Check if this is the InaccessibleObjectException we're trying to workaround
      if (isInaccessibleObjectException(e)) {
        LOGGER.debug("Intercepted InaccessibleObjectException from Arrow. Applying workaround.");
        throw new IOException(
            "Arrow requires the --add-opens=java.base/java.nio=ALL-UNNAMED flag. "
                + "Please upgrade to the latest version of the Databricks JDBC driver that includes a fix for this issue.",
            e);
      }
      if (e instanceof IOException) {
        throw (IOException) e;
      } else {
        throw new IOException("Error loading next batch", e);
      }
    }
  }

  /**
   * Gets the vector schema root from the underlying ArrowStreamReader.
   *
   * @return The vector schema root
   * @throws IOException if an error occurs while accessing the vector schema root
   */
  public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
    return arrowStreamReader.getVectorSchemaRoot();
  }

  /**
   * Checks if the exception is the InaccessibleObjectException we're trying to work around.
   *
   * @param e The exception to check
   * @return true if it's the InaccessibleObjectException for Buffer.address
   */
  private boolean isInaccessibleObjectException(Exception e) {
    Throwable cause = e;
    while (cause != null) {
      if (cause.getClass().getName().equals("java.lang.reflect.InaccessibleObjectException")
          && cause.getMessage() != null
          && cause
              .getMessage()
              .contains("Unable to make field long java.nio.Buffer.address accessible")) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    arrowStreamReader.close();
  }
}
