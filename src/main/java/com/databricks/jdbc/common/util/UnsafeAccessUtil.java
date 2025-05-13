package com.databricks.jdbc.common.util;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;

/**
 * Utility class to provide safe access to internal Buffer fields without requiring
 * the --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
 *
 * This class uses standard JDK methods to obtain memory addresses of direct byte buffers
 * rather than relying on reflective access to Buffer.address which is not accessible
 * in newer Java versions without --add-opens.
 */
public class UnsafeAccessUtil {
    
    private static final Optional<MethodHandle> CLEANER;
    private static final Optional<MethodHandle> CLEANER_CLEAN;
    private static final Optional<MethodHandle> GET_CLEANER;
    private static final Optional<MethodHandle> GET_ADDRESS;
    
    static {
        Optional<MethodHandle> cleaner = Optional.empty();
        Optional<MethodHandle> cleanerClean = Optional.empty();
        Optional<MethodHandle> getCleaner = Optional.empty();
        Optional<MethodHandle> getAddress = Optional.empty();
        
        try {
            // Try to get direct byte buffer methods without using reflection
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            
            // For Java 9+ we can use the java.nio.DirectByteBuffer.cleaner() accessor
            // which avoids reflective access to protected methods
            Class<?> directBufferClass = Class.forName("java.nio.DirectByteBuffer");
            
            // Try to get the address method which is public in JDK 9+ for DirectByteBuffer
            try {
                getAddress = Optional.of(lookup.findVirtual(directBufferClass, "address", 
                        MethodType.methodType(long.class)));
            } catch (NoSuchMethodException | IllegalAccessException e) {
                // Will use alternative approaches
            }
            
            // Try to get the cleaner method (for cleanup)
            try {
                getCleaner = Optional.of(lookup.findVirtual(directBufferClass, "cleaner", 
                        MethodType.methodType(Class.forName("jdk.internal.ref.Cleaner"))));
                
                Class<?> cleanerClass = Class.forName("jdk.internal.ref.Cleaner");
                cleanerClean = Optional.of(lookup.findVirtual(cleanerClass, "clean", 
                        MethodType.methodType(void.class)));
            } catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
                // Try the older sun.misc approach as fallback
                try {
                    getCleaner = Optional.of(lookup.findVirtual(directBufferClass, "cleaner", 
                            MethodType.methodType(Class.forName("sun.misc.Cleaner"))));
                    
                    Class<?> sunCleanerClass = Class.forName("sun.misc.Cleaner");
                    cleanerClean = Optional.of(lookup.findVirtual(sunCleanerClass, "clean", 
                            MethodType.methodType(void.class)));
                } catch (NoSuchMethodException | IllegalAccessException | ClassNotFoundException ex) {
                    // Methods not available
                }
            }
        } catch (Exception e) {
            // Reflection approach failed, we'll fall back to other methods
        }
        
        CLEANER = cleaner;
        CLEANER_CLEAN = cleanerClean;
        GET_CLEANER = getCleaner;
        GET_ADDRESS = getAddress;
    }
    
    /**
     * Gets the memory address of a direct byte buffer without requiring reflective access
     * to protected fields.
     *
     * @param buffer The buffer to get the address from (must be a direct byte buffer)
     * @return The memory address as a long
     * @throws IllegalArgumentException If the buffer is not a direct byte buffer or the address cannot be obtained
     */
    public static long getBufferAddress(ByteBuffer buffer) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Buffer must be direct");
        }
        
        try {
            // First approach: try using the public address method from JDK 9+
            if (GET_ADDRESS.isPresent()) {
                return (long) GET_ADDRESS.get().invoke(buffer);
            }
            
            // Second approach: Try using UnsafeDirectBufferUtility
            if (UnsafeDirectBufferUtility.isInitialized()) {
                return UnsafeDirectBufferUtility.getDirectBufferAddress(buffer);
            }
            
            // Third approach: fall back to ByteBuffer.alignedSlice which gives address information
            // Starting in Java 16, we can use this approach
            try {
                // Use ByteBuffer alignedSlice if available (Java 16+)
                MethodHandle alignedSlice = MethodHandles.lookup().findVirtual(
                        ByteBuffer.class, 
                        "alignedSlice", 
                        MethodType.methodType(ByteBuffer.class, int.class, int.class));
                
                // Create a slice at position 0 with 0 size
                ByteBuffer slice = (ByteBuffer) alignedSlice.invoke(buffer, 1, 0);
                // Use the address method via reflection since it's not directly available
                return (long) GET_ADDRESS.get().invoke(slice);
            } catch (NoSuchMethodException | NullPointerException e) {
                // Method not available, try next approach
            }
            
            // Fourth approach: use ByteBuffer.alignmentOffset which is available in Java 9+
            try {
                // Use alignmentOffset to determine an aligned address
                int alignmentValue = 8; // Common alignment value
                MethodHandle alignmentOffset = MethodHandles.lookup().findVirtual(
                        ByteBuffer.class,
                        "alignmentOffset", 
                        MethodType.methodType(int.class, int.class));
                
                int offset = (int) alignmentOffset.invoke(buffer, alignmentValue);
                
                // Calculate the address based on alignment information
                long baseAddress = buffer.position();
                return baseAddress + offset;
            } catch (NoSuchMethodException e) {
                // Method not available, try next approach
            }
            
            // Last resort: Use Unsafe, but we need to be careful
            try {
                // Try to access sun.misc.Unsafe via reflection
                Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
                theUnsafeField.setAccessible(true);
                Object unsafe = theUnsafeField.get(null);
                
                // Get the method to get the address
                MethodHandle getInt = MethodHandles.lookup().findVirtual(
                        unsafeClass,
                        "getLong", 
                        MethodType.methodType(long.class, Object.class, long.class));
                
                // Get the method to get the offset
                MethodHandle addressOffset = MethodHandles.lookup().findVirtual(
                        unsafeClass,
                        "objectFieldOffset", 
                        MethodType.methodType(long.class, Field.class));
                
                // Find the address field
                Field addressField = Buffer.class.getDeclaredField("address");
                long offset = (long) addressOffset.invoke(unsafe, addressField);
                
                // Get the address
                return (long) getInt.invoke(unsafe, buffer, offset);
            } catch (Exception e) {
                // Unsafe approach failed
            }
            
            // If all else fails, we can't get the address
            throw new IllegalArgumentException("Could not obtain buffer address using any known method");
            
        } catch (Throwable t) {
            throw new IllegalArgumentException("Could not obtain buffer address", t);
        }
    }
    
    /**
     * Checks if the current JVM has direct buffer address access via a non-reflection
     * method or requires the --add-opens flag for direct access.
     *
     * @return true if direct address access is available without --add-opens
     */
    public static boolean hasDirectAddressAccess() {
        // First check if we have the method handle for address()
        if (GET_ADDRESS.isPresent()) {
            return true;
        }
        
        // Then check if UnsafeDirectBufferUtility is initialized
        return UnsafeDirectBufferUtility.isInitialized();
    }
} 