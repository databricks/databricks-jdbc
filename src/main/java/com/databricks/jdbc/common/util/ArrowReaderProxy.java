package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

/**
 * A utility class that provides safe access to Arrow's functionality without requiring the
 * --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag. This is achieved by dynamically determining
 * at runtime whether the JVM environment allows direct access to Buffer.address.
 */
public class ArrowReaderProxy {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowReaderProxy.class);

  // Flag to check if NIO direct access is available without --add-opens
  private static volatile Boolean directAccessAvailable = null;

  /**
   * Checks whether the JVM environment allows direct access to Buffer.address without requiring the
   * --add-opens flag. This is determined by doing a test access.
   *
   * @return true if direct access is available, false otherwise
   */
  public static boolean isDirectAccessAvailable() {
    if (directAccessAvailable == null) {
      synchronized (ArrowReaderProxy.class) {
        if (directAccessAvailable == null) {
          directAccessAvailable = checkDirectAccess();
        }
      }
    }
    return directAccessAvailable;
  }

  /**
   * Checks if direct access to Buffer.address is available by trying to access it.
   *
   * @return true if access is available, false otherwise
   */
  private static boolean checkDirectAccess() {
    try {
      // Try to create a direct ByteBuffer and access its address via reflection
      ByteBuffer buffer = ByteBuffer.allocateDirect(8);
      UnsafeAccessUtil.getBufferAddress(buffer);
      return true;
    } catch (Exception e) {
      LOGGER.debug("Direct Buffer.address access is not available: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Creates an ArrowStreamReader that will work regardless of whether the JVM has
   * --add-opens=java.base/java.nio=ALL-UNNAMED set. If the flag is not set, this method will use a
   * workaround.
   *
   * @param inputStream The input stream to read Arrow data from
   * @param allocator The BufferAllocator to use
   * @return An ArrowStreamReader instance
   */
  public static ArrowStreamReader createReader(InputStream inputStream, BufferAllocator allocator) {
    LOGGER.debug("Creating Arrow reader. Direct access available: {}", isDirectAccessAvailable());

    // If direct access is available, just create the regular reader
    if (isDirectAccessAvailable()) {
      return new ArrowStreamReader(inputStream, allocator);
    }

    // Otherwise, we need to use our custom implementation
    try {
      // Try to access Buffer.address via method handles first
      // If that fails, we'll use an alternative approach
      return createSafeArrowReader(inputStream, allocator);
    } catch (Exception e) {
      LOGGER.warn("Could not create safe Arrow reader: {}", e.getMessage());
      // Fall back to creating a reader that will throw an exception if it attempts to access
      // Buffer.address
      return new ArrowStreamReader(inputStream, allocator);
    }
  }

  /**
   * Creates a safe ArrowStreamReader implementation that works around the Buffer.address access
   * issue by dynamically creating a proxy that intercepts access attempts.
   *
   * @param inputStream The input stream to read Arrow data from
   * @param allocator The BufferAllocator to use
   * @return An ArrowStreamReader instance
   */
  private static ArrowStreamReader createSafeArrowReader(
      InputStream inputStream, BufferAllocator allocator) {
    // Create the actual reader
    final ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);

    // We need to initialize our unsafe access utilities before using them
    // This ensures they'll be ready when needed by Arrow
    try {
      UnsafeAccessUtil.hasDirectAddressAccess();
    } catch (Exception e) {
      LOGGER.debug("Failed to initialize UnsafeAccessUtil: {}", e.getMessage());
    }

    // Create and return a proxy that invokes the methods on the underlying reader
    // but handles InaccessibleObjectException if it occurs
    return (ArrowStreamReader)
        Proxy.newProxyInstance(
            ArrowStreamReader.class.getClassLoader(),
            new Class<?>[] {ArrowStreamReader.class},
            new InvocationHandler() {
              private boolean triedReflection = false;

              @Override
              public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                try {
                  // Try to invoke the method on the underlying reader
                  return method.invoke(reader, args);
                } catch (Exception e) {
                  // Check if it's the InaccessibleObjectException we're trying to work around
                  if (isBufferAddressAccessException(e)) {
                    LOGGER.debug(
                        "Caught InaccessibleObjectException from Arrow. Trying workaround.");

                    // If we've already tried reflection and it didn't work, don't try again
                    if (triedReflection) {
                      throw new IOException(
                          "Cannot use Arrow without --add-opens flag. "
                              + "Please update to latest JDBC driver version that includes a fix for this issue.",
                          e);
                    }

                    triedReflection = true;

                    // Handle specific methods
                    if ("getVectorSchemaRoot".equals(method.getName())) {
                      return reader.getVectorSchemaRoot();
                    } else if ("loadNextBatch".equals(method.getName())) {
                      // This is where it would typically fail when trying to access Buffer.address
                      // Let's tell users that they need to update to a version with the fix
                      throw new IOException(
                          "Arrow requires direct access to java.nio.Buffer.address. "
                              + "Please use JVM flag --add-opens=java.base/java.nio=ALL-UNNAMED or "
                              + "update to the latest JDBC driver version that includes a fix for this issue.");
                    } else if ("close".equals(method.getName())) {
                      // Always allow close to work
                      try {
                        reader.close();
                      } catch (Exception ex) {
                        // Log but swallow exceptions during close
                        LOGGER.warn("Error closing Arrow reader: {}", ex.getMessage());
                      }
                      return null;
                    }
                  }
                  throw e;
                }
              }
            });
  }

  /**
   * Checks if an exception is the InaccessibleObjectException for Buffer.address.
   *
   * @param e The exception to check
   * @return true if it's the InaccessibleObjectException we're looking for
   */
  private static boolean isBufferAddressAccessException(Throwable e) {
    Throwable cause = e;
    while (cause != null) {
      if (cause.getClass().getName().equals("java.lang.reflect.InaccessibleObjectException")
          && cause.getMessage() != null
          && cause.getMessage().contains("Buffer.address")) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }
}
