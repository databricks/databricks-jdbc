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

        // First check if our UnsafeAccessUtil is initialized
        if (!UnsafeAccessUtil.hasDirectAddressAccess()
            && !UnsafeDirectBufferUtility.isInitialized()) {
          LOGGER.debug(
              "Safe access utilities are not working, cannot fix internal Arrow memory utilities");
          return;
        }

        // Find and patch the internal MemoryUtil class
        Class<?> memoryUtilClass = null;
        try {
          memoryUtilClass =
              Class.forName("com.databricks.internal.apache.arrow.memory.util.MemoryUtil");
          LOGGER.debug("Found internal Arrow MemoryUtil class to patch");
        } catch (ClassNotFoundException e) {
          // The class hasn't been loaded yet, which is good - we'll attach a class loader to handle
          // it
          LOGGER.debug(
              "Internal Arrow MemoryUtil class not yet loaded, will try to patch dynamically");
        }

        // Register our class initialization hook if possible
        installAddressMethodHook();

        initialized = true;
        LOGGER.debug("Successfully applied fix for internal Arrow memory utilities");
      } catch (Throwable t) {
        LOGGER.error("Failed to apply fix for internal Arrow memory utilities", t);
      }
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

  /** Install a hook to intercept calls to get buffer addresses. */
  private static void installAddressMethodHook() {
    // Add a hook to the classloader system property
    String existingProp = System.getProperty("java.system.class.loader");

    try {
      // Try to load the class to see if it's already loaded
      Class.forName("com.databricks.internal.apache.arrow.memory.util.MemoryUtil");
      // If we get here, the class is already loaded, so we need to use bytecode tricks
      fixLoadedMemoryUtilClass();
    } catch (ClassNotFoundException e) {
      // This is expected if the class hasn't been loaded yet, which is good
      LOGGER.debug("Internal Arrow MemoryUtil not yet loaded - will fix at load time");
    } catch (Exception e) {
      LOGGER.error("Error while checking for MemoryUtil class: {}", e.getMessage());
    }
  }

  /**
   * Fix the already loaded MemoryUtil class if it exists by replacing the memory address methods.
   */
  private static void fixLoadedMemoryUtilClass() {
    try {
      ClassLoader cl = InternalArrowMemoryUtilFixer.class.getClassLoader();
      Class<?> memoryUtilClass =
          cl.loadClass("com.databricks.internal.apache.arrow.memory.util.MemoryUtil");

      // Get the field where the exception holder is stored
      Field exceptionField = memoryUtilClass.getDeclaredField("MEMORY_ACCESS_ERROR");
      makeFieldAccessible(exceptionField);

      // Clear the exception so it doesn't think there's an error
      exceptionField.set(null, null);

      // Set static fields indicating that we can access memories
      Field canAccessDirectBufferField =
          memoryUtilClass.getDeclaredField("CAN_ACCESS_DIRECT_BUFFER");
      makeFieldAccessible(canAccessDirectBufferField);
      canAccessDirectBufferField.setBoolean(null, true);

      LOGGER.debug("Successfully patched loaded MemoryUtil class");
    } catch (Exception e) {
      LOGGER.error("Failed to fix loaded MemoryUtil class: {}", e.getMessage());
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
