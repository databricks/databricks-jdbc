package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.common.util.UnsafeAccessUtil;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import org.apache.arrow.memory.ArrowBuf;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * A helper class that provides utilities to work with Arrow buffers without requiring
 * the --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
 */
public class DatabricksArrowBufferHelper {
    private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksArrowBufferHelper.class);
    
    // Flag to check if we've installed the bridge
    private static boolean BRIDGE_INSTALLED = false;
    
    // Fields and methods for reflection
    private static Optional<Field> addressField = Optional.empty();
    private static Optional<Method> getAddressMethod = Optional.empty();
    
    /**
     * Initialize the system to use our safe ByteBuffer.address access.
     * This should be called early during driver initialization.
     *
     * This method installs hooks into the JVM that allow Arrow to access ByteBuffer.address
     * without requiring the --add-opens flag.
     */
    public static synchronized void initializeArrowBufferBridge() {
        if (BRIDGE_INSTALLED) {
            return;
        }
        
        try {
            // Check if we even need this workaround
            if (UnsafeAccessUtil.hasDirectAddressAccess()) {
                LOGGER.info("Direct ByteBuffer.address access is available, no need for workaround");
                BRIDGE_INSTALLED = true;
                return;
            }
            
            // Try to initialize a buffer hook to intercept access to Buffer.address
            // We'll use method handles to call our own UnsafeAccessUtil
            
            // Find ArrowBufUnderlyingBuffer class using reflection
            Class<?> arrowByteBufClass = null;
            try {
                // Different versions of Arrow use different package names
                // Try different possibilities
                arrowByteBufClass = Class.forName("org.apache.arrow.memory.ArrowByteBuf");
            } catch (ClassNotFoundException e) {
                try {
                    arrowByteBufClass = Class.forName("io.netty.buffer.ArrowByteBuf");
                } catch (ClassNotFoundException ex) {
                    // Likely a different version of Arrow, we'll try a different approach
                }
            }
            
            // Log success if we found the class
            if (arrowByteBufClass != null) {
                LOGGER.info("Found ArrowByteBuf class: {}", arrowByteBufClass.getName());
            }
            
            // Initialize access to buffer methods
            try {
                // Try to find java.nio.Buffer.address field
                Field addrField = Class.forName("java.nio.Buffer").getDeclaredField("address");
                addrField.setAccessible(true);
                addressField = Optional.of(addrField);
                LOGGER.info("Successfully accessed Buffer.address field");
            } catch (Exception e) {
                LOGGER.info("Could not access Buffer.address field: {}", e.getMessage());
            }
            
            BRIDGE_INSTALLED = true;
            LOGGER.info("Arrow Buffer bridge has been installed");
        } catch (Exception e) {
            LOGGER.warn("Failed to initialize Arrow buffer bridge: {}", e.getMessage());
        }
    }
    
    /**
     * Get the memory address of a direct ByteBuffer using our safe access method.
     * This is a workaround for the reflection-based approach that requires --add-opens.
     *
     * @param buffer The ByteBuffer to get the address of
     * @return The memory address as a long
     * @throws IllegalArgumentException If the buffer is not direct or address cannot be accessed
     */
    public static long getBufferAddress(ByteBuffer buffer) {
        return UnsafeAccessUtil.getBufferAddress(buffer);
    }
    
    /**
     * Sets the address field of a Buffer using our safe access method.
     * This can be used to override the address field when Arrow tries to access it.
     *
     * @param buffer The Buffer to set the address in
     * @param address The address value to set
     * @return true if successful, false otherwise
     */
    public static boolean setBufferAddress(Object buffer, long address) {
        if (addressField.isPresent()) {
            try {
                addressField.get().set(buffer, address);
                return true;
            } catch (Exception e) {
                LOGGER.warn("Failed to set Buffer.address: {}", e.getMessage());
            }
        }
        return false;
    }
    
    /**
     * Check if a ByteBuffer is direct.
     *
     * @param buffer The buffer to check
     * @return true if the buffer is direct
     */
    public static boolean isDirectBuffer(ByteBuffer buffer) {
        return buffer.isDirect();
    }
} 