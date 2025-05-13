package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;
import java.util.WeakHashMap;

/**
 * A direct replacement for Arrow's MemoryUtil class that doesn't require the
 * --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
 *
 * <p>This class provides direct memory access operations for use with Arrow and other libraries
 * that need to work with native memory.
 */
public final class MemoryUtilAccess {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(MemoryUtilAccess.class);

  private static final WeakHashMap<ByteBuffer, Long> ADDRESS_CACHE = new WeakHashMap<>();
  private static final sun.misc.Unsafe UNSAFE;
  private static final long BUFFER_ADDRESS_OFFSET;
  private static final boolean CAN_ACCESS_DIRECT_BUFFER;
  private static final Throwable MEMORY_ACCESS_ERROR;

  static {
    sun.misc.Unsafe unsafe = null;
    long bufferAddressOffset = 0L;
    boolean canAccessDirectBuffer = false;
    Throwable memoryAccessError = null;

    try {
      unsafe = initializeUnsafe();
      if (unsafe != null) {
        try {
          // Get the offset of the address field in Buffer class
          Field addressField = Buffer.class.getDeclaredField("address");
          bufferAddressOffset = unsafe.objectFieldOffset(addressField);
          canAccessDirectBuffer = true;
          LOGGER.debug("Successfully initialized direct Buffer access");
        } catch (Throwable t) {
          memoryAccessError = t;
          LOGGER.debug(
              "Could not access Buffer.address using standard approach: {}", t.getMessage());
        }
      }
    } catch (Throwable t) {
      memoryAccessError = t;
      LOGGER.debug("Failed to initialize Unsafe: {}", t.getMessage());
    }

    UNSAFE = unsafe;
    BUFFER_ADDRESS_OFFSET = bufferAddressOffset;
    CAN_ACCESS_DIRECT_BUFFER = canAccessDirectBuffer;
    MEMORY_ACCESS_ERROR = memoryAccessError;
  }

  /**
   * Gets the memory address of a direct ByteBuffer.
   *
   * @param buffer The direct ByteBuffer
   * @return The memory address
   * @throws IllegalArgumentException If the buffer is not direct
   */
  public static long getDirectBufferAddress(ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "buffer");
    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("buffer is not direct");
    }

    // First check cache
    synchronized (ADDRESS_CACHE) {
      Long cachedAddress = ADDRESS_CACHE.get(buffer);
      if (cachedAddress != null) {
        return cachedAddress;
      }
    }

    // Try various methods to get the address
    long address = -1;

    // Method 1: Use UnsafeAccessUtil
    try {
      address = UnsafeAccessUtil.getBufferAddress(buffer);
      cacheAddress(buffer, address);
      return address;
    } catch (Exception e) {
      // Fall through to next method
    }

    // Method 2: Use UnsafeDirectBufferUtility
    try {
      if (UnsafeDirectBufferUtility.isInitialized()) {
        address = UnsafeDirectBufferUtility.getDirectBufferAddress(buffer);
        cacheAddress(buffer, address);
        return address;
      }
    } catch (Exception e) {
      // Fall through to next method
    }

    // Method 3: Use our unsafe method
    try {
      if (CAN_ACCESS_DIRECT_BUFFER && UNSAFE != null && BUFFER_ADDRESS_OFFSET > 0) {
        address = UNSAFE.getLong(buffer, BUFFER_ADDRESS_OFFSET);
        cacheAddress(buffer, address);
        return address;
      }
    } catch (Exception e) {
      // Fall through to next method
    }

    // Method 4: Use JDK 9+ method via DirectByteBuffer.address()
    try {
      Method addressMethod = buffer.getClass().getMethod("address");
      addressMethod.setAccessible(true);
      address = (long) addressMethod.invoke(buffer);
      cacheAddress(buffer, address);
      return address;
    } catch (Exception e) {
      // Fall through to next method
    }

    // Method 5: Use ByteBuffer utilities from sun.misc
    try {
      Class<?> cls = Class.forName("sun.misc.DirectBufferImpl");
      Method getAddress = cls.getMethod("getAddress", ByteBuffer.class);
      address = (long) getAddress.invoke(null, buffer);
      cacheAddress(buffer, address);
      return address;
    } catch (Exception e) {
      // Fall through to exception
    }

    throw new IllegalStateException(
        "Unable to access direct buffer address. Consider using --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.");
  }

  /**
   * Cache a buffer address for future lookups.
   *
   * @param buffer The buffer
   * @param address The address
   */
  private static void cacheAddress(ByteBuffer buffer, long address) {
    synchronized (ADDRESS_CACHE) {
      ADDRESS_CACHE.put(buffer, address);
    }
  }

  /**
   * Initialize the Unsafe instance.
   *
   * @return The Unsafe instance, or null if not available
   */
  private static sun.misc.Unsafe initializeUnsafe() {
    return AccessController.doPrivileged(
        (PrivilegedAction<sun.misc.Unsafe>)
            () -> {
              try {
                java.lang.reflect.Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                return (sun.misc.Unsafe) field.get(null);
              } catch (Exception e) {
                LOGGER.warn("Could not access sun.misc.Unsafe: {}", e.getMessage());
                return null;
              }
            });
  }

  /**
   * Check if direct Buffer access is available.
   *
   * @return true if direct Buffer access is available
   */
  public static boolean canAccessDirectBuffer() {
    return CAN_ACCESS_DIRECT_BUFFER;
  }

  /**
   * Get the byte at the specified index from a direct buffer.
   *
   * @param buffer The direct buffer
   * @param index The index
   * @return The byte value
   */
  public static byte getByte(ByteBuffer buffer, long index) {
    assert buffer.isDirect();
    long address = getDirectBufferAddress(buffer);
    long effectiveAddress = address + index;
    return UNSAFE.getByte(effectiveAddress);
  }

  /**
   * Get a short at the specified index from a direct buffer.
   *
   * @param buffer The direct buffer
   * @param index The index
   * @return The short value
   */
  public static short getShort(ByteBuffer buffer, long index) {
    assert buffer.isDirect();
    long address = getDirectBufferAddress(buffer);
    long effectiveAddress = address + index;

    if (buffer.order() == ByteOrder.BIG_ENDIAN) {
      short value = UNSAFE.getShort(effectiveAddress);
      return Short.reverseBytes(value);
    } else {
      return UNSAFE.getShort(effectiveAddress);
    }
  }

  /**
   * Get an int at the specified index from a direct buffer.
   *
   * @param buffer The direct buffer
   * @param index The index
   * @return The int value
   */
  public static int getInt(ByteBuffer buffer, long index) {
    assert buffer.isDirect();
    long address = getDirectBufferAddress(buffer);
    long effectiveAddress = address + index;

    if (buffer.order() == ByteOrder.BIG_ENDIAN) {
      int value = UNSAFE.getInt(effectiveAddress);
      return Integer.reverseBytes(value);
    } else {
      return UNSAFE.getInt(effectiveAddress);
    }
  }

  /**
   * Get a long at the specified index from a direct buffer.
   *
   * @param buffer The direct buffer
   * @param index The index
   * @return The long value
   */
  public static long getLong(ByteBuffer buffer, long index) {
    assert buffer.isDirect();
    long address = getDirectBufferAddress(buffer);
    long effectiveAddress = address + index;

    if (buffer.order() == ByteOrder.BIG_ENDIAN) {
      long value = UNSAFE.getLong(effectiveAddress);
      return Long.reverseBytes(value);
    } else {
      return UNSAFE.getLong(effectiveAddress);
    }
  }

  /**
   * Set the byte at the specified index in a direct buffer.
   *
   * @param buffer The direct buffer
   * @param index The index
   * @param value The byte value
   */
  public static void setByte(ByteBuffer buffer, long index, byte value) {
    assert buffer.isDirect();
    long address = getDirectBufferAddress(buffer);
    long effectiveAddress = address + index;
    UNSAFE.putByte(effectiveAddress, value);
  }

  /**
   * Set the short at the specified index in a direct buffer.
   *
   * @param buffer The direct buffer
   * @param index The index
   * @param value The short value
   */
  public static void setShort(ByteBuffer buffer, long index, short value) {
    assert buffer.isDirect();
    long address = getDirectBufferAddress(buffer);
    long effectiveAddress = address + index;

    if (buffer.order() == ByteOrder.BIG_ENDIAN) {
      UNSAFE.putShort(effectiveAddress, Short.reverseBytes(value));
    } else {
      UNSAFE.putShort(effectiveAddress, value);
    }
  }

  /**
   * Set the int at the specified index in a direct buffer.
   *
   * @param buffer The direct buffer
   * @param index The index
   * @param value The int value
   */
  public static void setInt(ByteBuffer buffer, long index, int value) {
    assert buffer.isDirect();
    long address = getDirectBufferAddress(buffer);
    long effectiveAddress = address + index;

    if (buffer.order() == ByteOrder.BIG_ENDIAN) {
      UNSAFE.putInt(effectiveAddress, Integer.reverseBytes(value));
    } else {
      UNSAFE.putInt(effectiveAddress, value);
    }
  }

  /**
   * Set the long at the specified index in a direct buffer.
   *
   * @param buffer The direct buffer
   * @param index The index
   * @param value The long value
   */
  public static void setLong(ByteBuffer buffer, long index, long value) {
    assert buffer.isDirect();
    long address = getDirectBufferAddress(buffer);
    long effectiveAddress = address + index;

    if (buffer.order() == ByteOrder.BIG_ENDIAN) {
      UNSAFE.putLong(effectiveAddress, Long.reverseBytes(value));
    } else {
      UNSAFE.putLong(effectiveAddress, value);
    }
  }
}
