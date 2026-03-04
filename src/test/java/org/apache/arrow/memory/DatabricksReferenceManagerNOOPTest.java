package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

/** Test for DatabricksReferenceManagerNOOP. */
@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
@EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
public class DatabricksReferenceManagerNOOPTest {
  /** Test that getRefCount always returns 1. */
  @Test
  public void testGetRefCount() {
    assertEquals(1, DatabricksReferenceManagerNOOP.INSTANCE.getRefCount());
  }

  /** Test that release operations always return false. */
  @Test
  public void testRelease() {
    DatabricksReferenceManagerNOOP noop = DatabricksReferenceManagerNOOP.INSTANCE;

    assertFalse(noop.release(), "release() should always return false");
    assertFalse(noop.release(1), "release(int) should always return false");
    assertFalse(noop.release(100), "release(int) should always return false");

    // Ref count should remain 1 after release operations.
    assertEquals(1, noop.getRefCount(), "Ref count should remain 1");
  }

  /** Test that retain operations don't throw and don't change ref count. */
  @Test
  public void testRetain() {
    DatabricksReferenceManagerNOOP noop = DatabricksReferenceManagerNOOP.INSTANCE;

    // These should not throw.
    noop.retain();
    noop.retain(1);
    noop.retain(100);

    // Ref count should remain 1 after retain operations.
    assertEquals(1, noop.getRefCount(), "Ref count should remain 1");
  }

  /** Test that getSize and getAccountedSize return 0. */
  @Test
  public void testSizeReturnsZero() {
    DatabricksReferenceManagerNOOP noop = DatabricksReferenceManagerNOOP.INSTANCE;

    assertEquals(0L, noop.getSize(), "getSize() should return 0");
    assertEquals(0L, noop.getAccountedSize(), "getAccountedSize() should return 0");
  }

  /** Test that getAllocator returns a DatabricksBufferAllocator. */
  @Test
  public void testGetAllocator() {
    DatabricksReferenceManagerNOOP noop = DatabricksReferenceManagerNOOP.INSTANCE;

    BufferAllocator allocator = noop.getAllocator();
    assertNotNull(allocator, "getAllocator() should not return null");
    assertTrue(
        allocator instanceof DatabricksBufferAllocator,
        "getAllocator() should return a DatabricksBufferAllocator");
  }

  /** Test that retain(ArrowBuf, BufferAllocator) returns the same buffer. */
  @Test
  public void testRetainBuffer() {
    DatabricksReferenceManagerNOOP noop = DatabricksReferenceManagerNOOP.INSTANCE;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    ArrowBuf buffer = allocator.buffer(1024);

    ArrowBuf retained = noop.retain(buffer, allocator);
    assertSame(buffer, retained, "retain() should return the same buffer");
  }

  /** Test that deriveBuffer returns the same buffer. */
  @Test
  public void testDeriveBuffer() {
    DatabricksReferenceManagerNOOP noop = DatabricksReferenceManagerNOOP.INSTANCE;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    ArrowBuf buffer = allocator.buffer(1024);

    ArrowBuf derived = noop.deriveBuffer(buffer, 0, 512);
    assertSame(buffer, derived, "deriveBuffer() should return the same buffer");

    // Test with different index and length - should still return the same buffer.
    derived = noop.deriveBuffer(buffer, 256, 256);
    assertSame(buffer, derived, "deriveBuffer() should return the same buffer regardless of index");
  }

  /** Test that transferOwnership returns a valid result. */
  @Test
  public void testTransferOwnership() {
    DatabricksReferenceManagerNOOP noop = DatabricksReferenceManagerNOOP.INSTANCE;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    ArrowBuf buffer = allocator.buffer(1024);

    OwnershipTransferResult result =
        noop.transferOwnership(buffer, new DatabricksBufferAllocator());

    assertNotNull(result, "transferOwnership() should not return null");
    assertTrue(result.getAllocationFit(), "getAllocationFit() should return true");
    assertSame(
        buffer,
        result.getTransferredBuffer(),
        "getTransferredBuffer() should return the original buffer");
  }

  /** Test all operations remain consistent after multiple calls. */
  @Test
  public void testConsistencyAcrossMultipleCalls() {
    DatabricksReferenceManagerNOOP noop = DatabricksReferenceManagerNOOP.INSTANCE;

    for (int i = 0; i < 100; i++) {
      switch (i % 4) {
        case 0:
          noop.retain();
          break;
        case 1:
          noop.release();
          break;
        case 2:
          noop.retain(i);
          break;
        case 3:
        default:
          noop.release(i);
          break;
      }

      assertEquals(1, noop.getRefCount(), "Ref count should always be 1");
      assertEquals(0L, noop.getSize(), "Size should always be 0");
      assertEquals(0L, noop.getAccountedSize(), "Accounted size should always be 0");
    }
  }
}
