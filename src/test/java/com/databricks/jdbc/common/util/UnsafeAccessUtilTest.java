package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class UnsafeAccessUtilTest {

  @Test
  void testGetBufferAddressWithDirectBuffer() {
    // Create a direct byte buffer
    ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024);
    directBuffer.putInt(42);
    directBuffer.flip();

    // Test that we can get an address (should be non-zero)
    long address = UnsafeAccessUtil.getBufferAddress(directBuffer);

    // The address should be a non-zero value for a valid direct buffer
    assertNotEquals(0L, address);
    System.out.println("Direct buffer address: " + address);
  }

  @Test
  void testGetBufferAddressWithNonDirectBuffer() {
    // Create a non-direct buffer
    ByteBuffer nonDirectBuffer = ByteBuffer.allocate(1024);

    // Attempting to get the address should throw an exception
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          UnsafeAccessUtil.getBufferAddress(nonDirectBuffer);
        });
  }

  @Test
  void testHasDirectAddressAccess() {
    // This method should return a boolean value
    boolean hasAccess = UnsafeAccessUtil.hasDirectAddressAccess();

    // The result will depend on the JVM version, but we can at least verify
    // that the method runs without exception
    System.out.println("Has direct address access: " + hasAccess);
  }
}
