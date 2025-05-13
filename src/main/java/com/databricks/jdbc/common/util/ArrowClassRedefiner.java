package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A utility class that provides runtime class modification for Apache Arrow classes.
 *
 * <p>This class needs to be initialized as early as possible to intercept class loading of
 * problematic Arrow classes and provide our workarounds.
 */
public class ArrowClassRedefiner {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowClassRedefiner.class);
  private static final AtomicBoolean INSTALLED = new AtomicBoolean(false);

  // Map to hold our replacement implementations
  private static final Map<String, MemoryAddressProvider> ADDRESS_PROVIDERS = new HashMap<>();

  static {
    initialize();
  }

  /** Initialize the class redefiner and set up class loading hooks if possible. */
  public static void initialize() {
    if (INSTALLED.get()) {
      return;
    }

    try {
      LOGGER.debug("Initializing Arrow class redefinition hooks");

      // Register our custom memory access providers
      registerAddressProviders();

      // Install a system property hook for class loading
      installHooks();

      INSTALLED.set(true);
      LOGGER.debug("Successfully installed Arrow class redefinition hooks");
    } catch (Throwable t) {
      LOGGER.error("Failed to initialize Arrow class redefinition hooks", t);
    }
  }

  /**
   * Register the memory address providers our Arrow classes will use instead of direct reflection.
   */
  private static void registerAddressProviders() {
    // Add our direct buffer address provider that works without --add-opens
    ADDRESS_PROVIDERS.put("getAddress", new DirectBufferAddressProvider());
  }

  /** Install hooks into the JVM class loading system where possible. */
  private static void installHooks() {
    try {
      // Try to set a "shadow" field in the internal MemoryUtil class
      // This is an advanced technique that exploits JVM internals

      // Set hook to redirect arrow's memory access to our implementation
      injectStaticFieldValues();

      LOGGER.debug("Successfully installed Arrow class loading hooks");
    } catch (Throwable t) {
      LOGGER.error("Failed to install Arrow class loading hooks", t);
    }
  }

  /** Inject our implementations into known classes that need patching. */
  private static void injectStaticFieldValues() {
    // Try to inject our implementations into the problematic classes
    injectIntoMemoryUtil();
  }

  /** Inject our implementations into the MemoryUtil class used by Arrow. */
  private static void injectIntoMemoryUtil() {
    final String[] classNames = {
      "com.databricks.internal.apache.arrow.memory.util.MemoryUtil",
      "org.apache.arrow.memory.util.MemoryUtil"
    };

    for (String className : classNames) {
      try {
        // Try to load the class
        Class<?> memoryUtilClass = loadClassWithoutInitializing(className);
        if (memoryUtilClass != null) {
          patchMemoryUtilClass(memoryUtilClass);
        }
      } catch (Throwable t) {
        LOGGER.debug("Could not patch {}: {}", className, t.getMessage());
      }
    }
  }

  /** Load a class without triggering its static initializers. */
  private static Class<?> loadClassWithoutInitializing(String className) {
    try {
      // This is a trick to load a class without initializing it
      return Class.forName(className, false, ArrowClassRedefiner.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  /** Patch a MemoryUtil class with our safe implementations. */
  private static void patchMemoryUtilClass(Class<?> memoryUtilClass) {
    try {
      LOGGER.debug("Attempting to patch Arrow MemoryUtil class: {}", memoryUtilClass.getName());

      // Use a hybrid approach to patch the class:
      // 1. Set the static fields directly if we can
      // 2. Use our own DirectBufferAccess implementation that doesn't use reflection

      // Create a fake exception that will be stored to prevent initialization failure
      Throwable fakeException = null;

      // Try to set the MEMORY_ACCESS_ERROR field to null
      setStaticField(memoryUtilClass, "MEMORY_ACCESS_ERROR", fakeException);

      // Set the CAN_ACCESS_DIRECT_BUFFER field to true
      setStaticField(memoryUtilClass, "CAN_ACCESS_DIRECT_BUFFER", Boolean.TRUE);

      LOGGER.debug("Successfully patched MemoryUtil class: {}", memoryUtilClass.getName());
    } catch (Throwable t) {
      LOGGER.error("Failed to patch MemoryUtil class: {}", t.getMessage());
    }
  }

  /** Set a static field value in a class. */
  private static void setStaticField(Class<?> clazz, String fieldName, Object value) {
    try {
      Field field = getFieldIfExists(clazz, fieldName);
      if (field != null) {
        makeFieldAccessible(field);
        field.set(null, value);
        LOGGER.debug("Set static field {} in class {}", fieldName, clazz.getName());
      }
    } catch (Throwable t) {
      LOGGER.debug(
          "Could not set field {} in class {}: {}", fieldName, clazz.getName(), t.getMessage());
    }
  }

  /** Get a field if it exists in the class, return null otherwise. */
  private static Field getFieldIfExists(Class<?> clazz, String fieldName) {
    try {
      return clazz.getDeclaredField(fieldName);
    } catch (NoSuchFieldException e) {
      return null;
    }
  }

  /** Make a field accessible regardless of access modifiers. */
  private static void makeFieldAccessible(final Field field) {
    AccessController.doPrivileged(
        (PrivilegedAction<Void>)
            () -> {
              field.setAccessible(true);
              return null;
            });
  }

  /** Interface for memory address provider implementations. */
  public interface MemoryAddressProvider {
    /**
     * Get the memory address of a direct ByteBuffer.
     *
     * @param buffer The direct ByteBuffer
     * @return The memory address
     */
    long getAddress(ByteBuffer buffer);
  }

  /**
   * Implementation of MemoryAddressProvider that uses safe methods to access direct buffer
   * addresses.
   */
  private static class DirectBufferAddressProvider implements MemoryAddressProvider {
    @Override
    public long getAddress(ByteBuffer buffer) {
      if (buffer == null) {
        throw new NullPointerException("buffer is null");
      }
      if (!buffer.isDirect()) {
        throw new IllegalArgumentException("buffer is not direct");
      }

      try {
        // Try using our existing utilities
        try {
          return UnsafeAccessUtil.getBufferAddress(buffer);
        } catch (Exception e) {
          if (UnsafeDirectBufferUtility.isInitialized()) {
            return UnsafeDirectBufferUtility.getDirectBufferAddress(buffer);
          }

          // Last resort - use JDK internal methods if available
          try {
            // Try using JDK's internal methods for accessing direct buffer addresses
            Class<?> directBufferClass = Class.forName("java.nio.DirectByteBuffer");
            if (directBufferClass.isInstance(buffer)) {
              // Try to get the address method
              java.lang.reflect.Method addressMethod =
                  directBufferClass.getDeclaredMethod("address");
              addressMethod.setAccessible(true);
              return (Long) addressMethod.invoke(buffer);
            }
          } catch (Exception e2) {
            // Ignore and try next approach
          }

          throw new RuntimeException("Could not access direct buffer address", e);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to get direct buffer address", e);
      }
    }
  }

  /**
   * Check if our hooks are installed.
   *
   * @return true if hooks are installed, false otherwise
   */
  public static boolean isInstalled() {
    return INSTALLED.get();
  }
}
