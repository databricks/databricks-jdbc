package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

/** Test reference manager */
@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
@EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
public class DatabricksReferenceManagerTest {
  private static int REF_COUNT = 1;

  /** Test accounting of Reference manager */
  @Test
  public void testAccounting() {
    final long size = 2048;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    DatabricksReferenceManager referenceManager = new DatabricksReferenceManager(allocator, size);

    for (int i = 0; i < 100; i++) {
      switch (i % 4) {
        case 0:
          referenceManager.retain();
          break;
        case 1:
          referenceManager.release();
          break;
        case 2:
          referenceManager.retain(i);
          break;
        case 3:
        default:
          referenceManager.release(i);
          break;
      }
      assertEquals(REF_COUNT, referenceManager.getRefCount(), "Ref count should be constant");
      assertEquals(size, referenceManager.getSize(), "Size should be constant");
      assertEquals(size, referenceManager.getAccountedSize(), "Size should be constant");
      assertEquals(allocator, referenceManager.getAllocator(), "Allocator should be the same");
    }
  }

  /** Test derive buffer fails on invalid arguments */
  @Test
  public void testDeriveBufferFailsOnPreconditions() {
    final int bufferSize = 1024;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    DatabricksReferenceManager referenceManager =
        new DatabricksReferenceManager(allocator, bufferSize);
    DatabricksArrowBuf buffer = (DatabricksArrowBuf) allocator.buffer(bufferSize);

    assertThrows(
        IllegalArgumentException.class,
        () -> referenceManager.deriveBuffer(buffer, 0, Integer.MAX_VALUE + 1L),
        "Should fail on invalid length");
    assertThrows(
        IllegalArgumentException.class,
        () -> referenceManager.deriveBuffer(buffer, bufferSize - 1, 2),
        "Should fail on invalid length");
  }

  /** Test derive buffer. */
  @Test
  public void testDeriveBuffer() {
    final int bufferSize = 4096;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    DatabricksReferenceManager referenceManager =
        new DatabricksReferenceManager(allocator, bufferSize);
    DatabricksArrowBuf buffer = (DatabricksArrowBuf) allocator.buffer(bufferSize);

    // Write zeroes into the original buffer.
    for (int i = 0; i < bufferSize; i++) {
      buffer.writeByte((byte) 0);
    }

    // Test whole slice.
    DatabricksArrowBuf wholeSlice = (DatabricksArrowBuf) referenceManager.retain(buffer, allocator);
    testWriteAffectsOriginalAndDerivedBuffer(buffer, wholeSlice, 0);

    // Test transfer ownership.
    OwnershipTransferResult ownershipTransferResult =
        referenceManager.transferOwnership(buffer, new DatabricksBufferAllocator());
    assertTrue(ownershipTransferResult.getAllocationFit(), "Should fit");
    DatabricksArrowBuf transferredBuffer =
        (DatabricksArrowBuf) ownershipTransferResult.getTransferredBuffer();
    testWriteAffectsOriginalAndDerivedBuffer(transferredBuffer, buffer, 0);

    // Write data to part of a slice and check that the original buffer is affected as well.
    for (int sliceSize = 1; sliceSize < bufferSize; sliceSize++) {
      DatabricksArrowBuf slice =
          (DatabricksArrowBuf) referenceManager.deriveBuffer(buffer, 0, sliceSize);
      testWriteAffectsOriginalAndDerivedBuffer(buffer, slice, 0);

      int index = bufferSize - sliceSize;
      slice = (DatabricksArrowBuf) buffer.slice(index, sliceSize);
      testWriteAffectsOriginalAndDerivedBuffer(buffer, slice, index);
    }

    // Test some random offsets and length.
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      int startOffset = random.nextInt(bufferSize);
      int size = random.nextInt(bufferSize - startOffset);
      DatabricksArrowBuf slice =
          (DatabricksArrowBuf) referenceManager.deriveBuffer(buffer, startOffset, size);
      testWriteAffectsOriginalAndDerivedBuffer(buffer, slice, startOffset);
    }
  }

  private void testWriteAffectsOriginalAndDerivedBuffer(
      DatabricksArrowBuf buffer, DatabricksArrowBuf slice, int sliceStartIndex) {
    final int bufferSize = (int) buffer.capacity();

    // Write zeroes into the original buffer.
    buffer.clear();
    for (int i = 0; i < bufferSize; i++) {
      buffer.writeByte((byte) 0);
    }

    // Write data to the slice and check that the original buffer is affected as well.
    for (int i = 0; i < slice.capacity(); i++) {
      slice.setByte(i, getByteValue(i));
    }
    for (int i = 0; i < slice.capacity(); i++) {
      assertEquals(
          getByteValue(i),
          buffer.getByte(sliceStartIndex + i),
          "Readable bytes should be correct at index " + i);
    }

    // Write data to the original buffer and check that the slice is affected.
    for (int i = sliceStartIndex; i < sliceStartIndex + slice.capacity(); i++) {
      buffer.setByte(i, getByteValue(i));
    }
    for (int i = 0; i < slice.capacity(); i++) {
      assertEquals(
          getByteValue(sliceStartIndex + i),
          slice.getByte(i),
          "Readable bytes should be correct at index " + i);
    }
  }

  private byte getByteValue(int i) {
    return (byte) (i % 256);
  }
}
