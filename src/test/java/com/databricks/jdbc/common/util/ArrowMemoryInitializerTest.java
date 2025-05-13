package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

class ArrowMemoryInitializerTest {

  @Test
  void testInitializeAndDirectBufferAccess() {
    // Initialize our utilities
    ArrowMemoryInitializer.initialize();

    // Verify it's marked as initialized
    assertTrue(ArrowMemoryInitializer.isInitialized());

    // Create a direct ByteBuffer
    ByteBuffer buffer = ByteBuffer.allocateDirect(8);
    buffer.putLong(0, 0x1234567890ABCDEFL);

    try {
      // Try to get the address - this should work without the JVM flag
      long address = UnsafeAccessUtil.getBufferAddress(buffer);
      assertTrue(address > 0, "Buffer address should be a positive value");
    } catch (Exception e) {
      fail("Should be able to access direct buffer address: " + e.getMessage());
    }
  }

  @Test
  void testArrowAllocatorCreation() {
    // Initialize our utilities
    ArrowMemoryInitializer.initialize();

    try (BufferAllocator allocator = ArrowAllocatorFactory.createAllocator(Integer.MAX_VALUE)) {
      // Just verify we can create an allocator without exceptions
      assertNotNull(allocator);
      assertTrue(allocator instanceof RootAllocator);
    }
  }
}
