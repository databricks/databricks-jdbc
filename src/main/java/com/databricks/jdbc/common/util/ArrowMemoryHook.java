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
 * A utility class that provides direct hooking into Arrow's memory system to bypass the need for
 * the --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
 *
 * <p>This class must be initialized as early as possible, before any Arrow classes are loaded.
 */
public class ArrowMemoryHook {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowMemoryHook.class);
  private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
  private static final Map<String, Class<?>> MOCK_CLASSES = new HashMap<>();

  static {
    initialize();
  }

  /** Initialize the memory hook. */
  public static void initialize() {
    if (INITIALIZED.get()) {
      return;
    }

    synchronized (ArrowMemoryHook.class) {
      if (INITIALIZED.get()) {
        return;
      }

      try {
        LOGGER.debug("Initializing Arrow memory hook");

        // Initialize early
        MemoryUtilAccess.canAccessDirectBuffer();

        // Apply our patch to the internal Arrow memory utilities
        applyMemoryHooks();

        INITIALIZED.set(true);
        LOGGER.debug("Arrow memory hook initialization complete");
      } catch (Throwable t) {
        LOGGER.error("Failed to initialize Arrow memory hook", t);
      }
    }
  }

  /** Apply hooks to Arrow's memory utilities. */
  private static void applyMemoryHooks() {
    try {
      // Attempt to hook into the BaseAllocator initialization
      hookBaseAllocator();

      // Hook the internal MemoryUtil class
      hookMemoryUtilClass();

      LOGGER.debug("Successfully applied Arrow memory hooks");
    } catch (Throwable t) {
      LOGGER.error("Failed to apply Arrow memory hooks", t);
    }
  }

  /** Hook the BaseAllocator class to use our memory utilities. */
  private static void hookBaseAllocator() {
    try {
      // First check if the class is already loaded
      try {
        Class<?> baseAllocator =
            Class.forName(
                "com.databricks.internal.apache.arrow.memory.BaseAllocator",
                false,
                ArrowMemoryHook.class.getClassLoader());
        if (baseAllocator != null) {
          patchBaseAllocator(baseAllocator);
        }
      } catch (ClassNotFoundException e) {
        // Expected if the class is not loaded yet
      }

      // Also hook the unshaded version just in case
      try {
        Class<?> baseAllocator =
            Class.forName(
                "org.apache.arrow.memory.BaseAllocator",
                false,
                ArrowMemoryHook.class.getClassLoader());
        if (baseAllocator != null) {
          patchBaseAllocator(baseAllocator);
        }
      } catch (ClassNotFoundException e) {
        // Expected if the class is not loaded yet
      }
    } catch (Throwable t) {
      LOGGER.error("Failed to hook BaseAllocator class", t);
    }
  }

  /** Patch the BaseAllocator class. */
  private static void patchBaseAllocator(Class<?> baseAllocator) {
    try {
      // Try to patch static initializers or critical fields
      LOGGER.debug("Patching BaseAllocator class: {}", baseAllocator.getName());

      // For now, just logging as we'll tackle this if needed
      LOGGER.debug("BaseAllocator patching not yet implemented");
    } catch (Throwable t) {
      LOGGER.error("Failed to patch BaseAllocator class", t);
    }
  }

  /** Hook the internal MemoryUtil class to bypass its problematic initialization. */
  private static void hookMemoryUtilClass() {
    final String[] memoryUtilClassNames = {
      "com.databricks.internal.apache.arrow.memory.util.MemoryUtil",
      "org.apache.arrow.memory.util.MemoryUtil"
    };

    for (String className : memoryUtilClassNames) {
      try {
        // Check if the class is already loaded
        try {
          Class<?> memoryUtilClass =
              Class.forName(className, false, ArrowMemoryHook.class.getClassLoader());
          if (memoryUtilClass != null) {
            patchMemoryUtilClass(memoryUtilClass);
          }
        } catch (ClassNotFoundException e) {
          // Expected if the class is not loaded yet
          LOGGER.debug("MemoryUtil class not loaded yet: {}", className);
        }
      } catch (Throwable t) {
        LOGGER.error("Failed to hook MemoryUtil class: {}", className, t);
      }
    }
  }

  /** Patch the MemoryUtil class to use our safe memory access methods. */
  private static void patchMemoryUtilClass(Class<?> memoryUtilClass) {
    try {
      LOGGER.debug("Patching MemoryUtil class: {}", memoryUtilClass.getName());

      // Set critical fields
      // Since we can't modify the class after it's loaded, our best bet
      // is to reset the exception field so initialization can continue

      Field memoryAccessErrorField = null;
      Field canAccessDirectBufferField = null;

      try {
        memoryAccessErrorField = memoryUtilClass.getDeclaredField("MEMORY_ACCESS_ERROR");
        canAccessDirectBufferField = memoryUtilClass.getDeclaredField("CAN_ACCESS_DIRECT_BUFFER");
      } catch (NoSuchFieldException e) {
        LOGGER.debug("Could not find expected fields in MemoryUtil class");
        return;
      }

      // Make the fields accessible
      final Field finalMemoryAccessErrorField = memoryAccessErrorField;
      final Field finalCanAccessDirectBufferField = canAccessDirectBufferField;

      AccessController.doPrivileged(
          (PrivilegedAction<Void>)
              () -> {
                finalMemoryAccessErrorField.setAccessible(true);
                finalCanAccessDirectBufferField.setAccessible(true);
                return null;
              });

      // Set the fields
      memoryAccessErrorField.set(null, null); // Clear the error
      canAccessDirectBufferField.set(null, Boolean.TRUE); // Mark as working

      LOGGER.debug("Successfully patched MemoryUtil class: {}", memoryUtilClass.getName());
    } catch (Throwable t) {
      LOGGER.error("Failed to patch MemoryUtil class: {}", t.getMessage());
    }
  }

  /** A replacement method for MemoryUtil.getDirectBufferAddress. */
  public static long getDirectBufferAddress(ByteBuffer buffer) {
    return MemoryUtilAccess.getDirectBufferAddress(buffer);
  }

  /**
   * Check if our hooks are initialized.
   *
   * @return true if initialized, false otherwise
   */
  public static boolean isInitialized() {
    return INITIALIZED.get();
  }
}
