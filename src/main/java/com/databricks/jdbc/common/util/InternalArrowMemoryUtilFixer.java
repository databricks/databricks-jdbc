package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.WeakHashMap;
import sun.misc.Unsafe;

/**
 * A utility class that fixes the internal (shaded) Apache Arrow memory utilities at runtime to work
 * without requiring the --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
 */
public class InternalArrowMemoryUtilFixer {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(InternalArrowMemoryUtilFixer.class);

  private static volatile boolean initialized = false;
  private static Unsafe unsafe;
  private static long addressFieldOffset = -1;
  private static WeakHashMap<ByteBuffer, Long> addressCache = new WeakHashMap<>();

  static {
    initializeUnsafe();
  }

  /**
   * Apply the fix to bypass the need for --add-opens JVM flag. This method patches the internal
   * Arrow memory utilities to use our safe access methods instead of direct reflection.
   */
  public static void apply() {
    if (initialized) {
      return;
    }

    synchronized (InternalArrowMemoryUtilFixer.class) {
      if (initialized) {
        return;
      }

      try {
        LOGGER.debug("Attempting to fix internal Arrow memory utilities");

        // First ensure ArrowMemoryHook is initialized for a more aggressive approach
        ArrowMemoryHook.initialize();

        // Now apply our more targeted fixes if needed
        fixArrowMemoryUtil();

        initialized = true;
        LOGGER.debug("Successfully applied fix for internal Arrow memory utilities");
      } catch (Throwable t) {
        LOGGER.error("Failed to apply fix for internal Arrow memory utilities", t);
      }
    }
  }

  /** Fix Arrow's memory utilities by patching classes and fields. */
  private static void fixArrowMemoryUtil() {
    // Attempt to patch both versions of the class
    final String[] classNames = {
      "com.databricks.internal.apache.arrow.memory.util.MemoryUtil",
      "org.apache.arrow.memory.util.MemoryUtil"
    };

    for (String className : classNames) {
      try {
        Class<?> memoryUtilClass = loadMemoryUtilClass(className);
        if (memoryUtilClass != null) {
          patchMemoryUtilFields(memoryUtilClass);
        }
      } catch (Throwable t) {
        LOGGER.debug("Could not patch memory util class {}: {}", className, t.getMessage());
      }
    }
  }

  /** Load the MemoryUtil class without triggering static initialization. */
  private static Class<?> loadMemoryUtilClass(String className) {
    try {
      // Try to load the class without initializing it
      return Class.forName(className, false, InternalArrowMemoryUtilFixer.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      LOGGER.debug("MemoryUtil class not found: {}", className);
      return null;
    }
  }

  /** Patch the static fields in the MemoryUtil class. */
  private static void patchMemoryUtilFields(Class<?> memoryUtilClass) {
    try {
      LOGGER.debug("Patching fields in {}", memoryUtilClass.getName());

      // Get all the critical fields we need to patch
      Field memoryAccessErrorField = memoryUtilClass.getDeclaredField("MEMORY_ACCESS_ERROR");
      Field canAccessDirectBufferField =
          memoryUtilClass.getDeclaredField("CAN_ACCESS_DIRECT_BUFFER");

      // Make the fields accessible
      AccessController.doPrivileged(
          (PrivilegedAction<Void>)
              () -> {
                memoryAccessErrorField.setAccessible(true);
                canAccessDirectBufferField.setAccessible(true);
                return null;
              });

      // Set the fields to values that will make Arrow work
      memoryAccessErrorField.set(null, null); // Clear the error
      canAccessDirectBufferField.set(null, Boolean.TRUE); // Mark as working

      LOGGER.debug("Successfully patched MemoryUtil fields in {}", memoryUtilClass.getName());
    } catch (Throwable t) {
      LOGGER.debug("Error patching MemoryUtil fields: {}", t.getMessage());
    }
  }

  /**
   * Initialize the Unsafe instance needed for accessing the Buffer.address field without
   * reflection.
   */
  private static void initializeUnsafe() {
    try {
      // Get the Unsafe instance
      Field theUnsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafeField.setAccessible(true);
      unsafe = (Unsafe) theUnsafeField.get(null);

      // Get the offset of the address field in Buffer
      Field addressField = java.nio.Buffer.class.getDeclaredField("address");
      addressFieldOffset = unsafe.objectFieldOffset(addressField);

      LOGGER.debug("Initialized Unsafe access for internal Arrow memory utilities");
    } catch (Exception e) {
      LOGGER.error("Failed to initialize Unsafe: {}", e.getMessage());
    }
  }

  /**
   * Get the memory address of a direct ByteBuffer using the safe method.
   *
   * @param buffer The ByteBuffer to get the address from
   * @return The memory address
   */
  public static long getDirectBufferAddress(ByteBuffer buffer) {
    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("Buffer is not direct");
    }

    // First check the cache
    synchronized (addressCache) {
      Long cachedAddress = addressCache.get(buffer);
      if (cachedAddress != null) {
        return cachedAddress;
      }
    }

    // Use ArrowMemoryHook's method first
    try {
      return ArrowMemoryHook.getDirectBufferAddress(buffer);
    } catch (Exception e) {
      // Fall back to other methods
    }

    // Try using UnsafeAccessUtil first
    try {
      long address = UnsafeAccessUtil.getBufferAddress(buffer);
      // Cache the address
      synchronized (addressCache) {
        addressCache.put(buffer, address);
      }
      return address;
    } catch (Exception e) {
      // Fall back to Unsafe if available
      if (unsafe != null && addressFieldOffset >= 0) {
        long address = unsafe.getLong(buffer, addressFieldOffset);
        // Cache the address
        synchronized (addressCache) {
          addressCache.put(buffer, address);
        }
        return address;
      }
      throw new RuntimeException("Could not access direct buffer address", e);
    }
  }

  /** Make a field accessible regardless of JVM module rules. */
  private static void makeFieldAccessible(final Field field) {
    AccessController.doPrivileged(
        (PrivilegedAction<Void>)
            () -> {
              field.setAccessible(true);
              return null;
            });
  }
}
