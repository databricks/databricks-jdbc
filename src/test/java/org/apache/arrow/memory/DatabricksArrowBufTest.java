package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test all the public API of {@code DatabricksArrowBuf}. */
@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
@EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
public class DatabricksArrowBufTest {

  private static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

  private static final Logger logger = LoggerFactory.getLogger(DatabricksArrowBufTest.class);

  /** Test the constructor fails on invalid capacity arguments. */
  @Test
  public void testConstructorFailsOnInvalidCapacity() {
    final int bufferSize = 32;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    DatabricksReferenceManager refManager = new DatabricksReferenceManager(allocator, bufferSize);

    // Should fail when capacity is exceeded.
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          try (DatabricksArrowBuf arrowBuf =
              new DatabricksArrowBuf(refManager, null, Integer.MAX_VALUE + 1L)) {
            logger.info("Should not reach here. {}", arrowBuf);
          }
        },
        "Constructor should fail when capacity is greater than Integer.MAX_VALUE");
  }

  /**
   * Test ref count is always positive as long as there is a reference to the DatabricksArrowBuffer.
   */
  @Test
  public void testRefCountIsAlwaysPositive() {
    final int bufferSize = 32;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    DatabricksArrowBuf buffer = (DatabricksArrowBuf) allocator.buffer(bufferSize);
    assertTrue(buffer.refCnt() > 0, "Refcount should be positive");

    // Even after allocator is closed, if there is a reference to the buffer it should be
    // positive.
    allocator.close();
    assertTrue(buffer.refCnt() > 0, "Refcount should be positive even after allocator is closed");
  }

  /** Test checkBytes behaviour. */
  @Test
  public void testCheckBytes() {
    final int bufferSize = 32;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int i = 0; i < bufferSize; i++) {
        buffer.checkBytes(0, bufferSize);
      }

      // Negative should throw an exception.
      assertThrows(
          IndexOutOfBoundsException.class,
          () -> buffer.checkBytes(-1, bufferSize),
          "Negative start index should fail.");

      // Past end should throw an exception.
      assertThrows(
          IndexOutOfBoundsException.class,
          () -> buffer.checkBytes(0, bufferSize + 1),
          "Out of bounds end index should fail.");
    }
  }

  /** Test setting buffer capacity. */
  @Test
  public void testSetCapacity() {
    final int bufferSize = 32;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {

      assertThrows(
          UnsupportedOperationException.class,
          () -> buffer.capacity(bufferSize + 1),
          "Increasing buffer capacity should fail.");

      // Reducing buffer size capacity should be supported.
      for (int capacity = bufferSize - 1; capacity >= 0; capacity--) {
        buffer.capacity(capacity);
      }
    }
  }

  /** Test byte order is as expected. */
  @Test
  public void testByteOrder() {
    final int bufferSize = 32;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      assertEquals(BYTE_ORDER, buffer.order(), "ByteOrder should be " + BYTE_ORDER);
    }
  }

  /** Test readable bytes behaviour is correct. */
  @Test
  public void testReadableBytes() {
    final int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int i = 0; i < bufferSize; i++) {
        buffer.writeByte((byte) i);
        assertEquals(i + 1, buffer.readableBytes(), "Readable bytes should be correct.");
      }

      for (int i = 0; i < bufferSize; i++) {
        buffer.readByte();
        assertEquals(
            bufferSize - 1 - i, buffer.readableBytes(), "Readable bytes should be correct.");
      }
    }
  }

  /** Test writable bytes behaviour is correct. */
  @Test
  public void testWritableBytes() {
    final int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      assertEquals(bufferSize, buffer.writableBytes(), "Writable bytes should be correct.");
      for (int i = 0; i < bufferSize; i++) {
        buffer.writeByte((byte) i);
        assertEquals(
            bufferSize - 1 - i, buffer.writableBytes(), "Writable bytes should be correct.");
      }
    }
  }

  /** Test slice works as expected. */
  @Test
  public void testSlice() {
    final int bufferSize = 1024;
    DatabricksArrowBuf buffer = newBuffer(bufferSize);

    // Write zeroes into the original buffer.
    for (int i = 0; i < bufferSize; i++) {
      buffer.writeByte((byte) 0);
    }

    // Test whole slice.
    DatabricksArrowBuf wholeSlice = (DatabricksArrowBuf) buffer.slice();
    testWriteAffectsOriginalBufferAndSlice(buffer, wholeSlice, 0);

    // Write data to part of a slice and check that the original buffer is affected as well.
    for (int sliceSize = 1; sliceSize < bufferSize; sliceSize++) {
      DatabricksArrowBuf slice = (DatabricksArrowBuf) buffer.slice(0, sliceSize);
      testWriteAffectsOriginalBufferAndSlice(buffer, slice, 0);

      int index = bufferSize - sliceSize;
      slice = (DatabricksArrowBuf) buffer.slice(index, sliceSize);
      testWriteAffectsOriginalBufferAndSlice(buffer, slice, index);
    }

    // Test some random offsets and length.
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      int startOffset = random.nextInt(bufferSize);
      int size = random.nextInt(bufferSize - startOffset);
      DatabricksArrowBuf slice = (DatabricksArrowBuf) buffer.slice(startOffset, size);
      testWriteAffectsOriginalBufferAndSlice(buffer, slice, startOffset);
    }
  }

  private void testWriteAffectsOriginalBufferAndSlice(
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

  /** Should fail on incorrect indices. */
  @Test
  public void testSliceFailsOnIncorrectIndices() {
    final int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      // Write zeroes into the original buffer.
      for (int i = 0; i < bufferSize; i++) {
        buffer.writeByte((byte) 0);
      }

      assertThrows(
          IndexOutOfBoundsException.class,
          () -> buffer.slice(-1, bufferSize),
          "Should fail on negative index.");
      assertThrows(
          IndexOutOfBoundsException.class,
          () -> buffer.slice(0, bufferSize + 1),
          "Should fail on out of bounds length.");
      assertThrows(
          IndexOutOfBoundsException.class,
          () -> buffer.slice(1, bufferSize),
          "Should fail on out of bounds length");
    }
  }

  /** Test nio buffer behaviour. */
  @Test
  public void testNioBuffer() {
    final int bufferSize = 1024;
    ByteBuffer nioBuffer;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int i = 0; i < bufferSize; i++) {
        buffer.writeByte(getByteValue(i));
      }

      nioBuffer = buffer.nioBuffer();
    }
    assertEquals(
        bufferSize,
        nioBuffer.remaining(),
        "NioBuffer should have " + bufferSize + " " + "bytes to read.");

    for (int i = nioBuffer.position(); i < nioBuffer.limit(); i++) {
      assertEquals(
          getByteValue(i), nioBuffer.get(i), "Readable bytes should be correct at index " + i);
    }
  }

  /** Test nio buffer slices. */
  @Test
  public void testNioBufferSlices() {
    final int bufferSize = 1024;
    DatabricksArrowBuf buffer = newBuffer(bufferSize);

    // Write data to part of a slice and check that the original buffer is affected as well.
    for (int i = 0; i < bufferSize; i++) {
      int size = bufferSize - i;
      testWriteAffectsOriginalBufferAndNioBuffer(buffer, i, size);

      int index = bufferSize - i;
      testWriteAffectsOriginalBufferAndNioBuffer(buffer, index, i);
    }

    // Test some random offsets and length.
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      int startIndex = random.nextInt(bufferSize);
      int size = random.nextInt(bufferSize - startIndex);
      testWriteAffectsOriginalBufferAndNioBuffer(buffer, startIndex, size);
    }
  }

  private void testWriteAffectsOriginalBufferAndNioBuffer(
      DatabricksArrowBuf buffer, int startIndex, int length) {
    final int bufferSize = (int) buffer.capacity();

    // Write zeroes into the original buffer.
    buffer.clear();
    for (int i = 0; i < bufferSize; i++) {
      buffer.writeByte(getByteValue(i));
    }

    ByteBuffer nioBuffer = buffer.nioBuffer(startIndex, length);
    assertEquals(
        length, nioBuffer.remaining(), "NioBuffer should have " + length + " bytes to " + "read.");
    for (int i = nioBuffer.position(); i < nioBuffer.limit(); i++) {
      assertEquals(
          getByteValue(i), nioBuffer.get(i), "Readable bytes should be correct at index " + i);
    }
  }

  /** Test memory address returned is correct. */
  @Test
  public void testMemoryAddress() {
    final int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      assertEquals(0, buffer.memoryAddress(), "Memory address should be correct.");
    }
  }

  /** Test toString works. */
  @Test
  public void testToString() {
    final int bufferSize = 1024;
    DatabricksArrowBuf buffer;
    try (DatabricksBufferAllocator allocator = new DatabricksBufferAllocator()) {
      buffer = (DatabricksArrowBuf) allocator.buffer(bufferSize);
    }
    //noinspection Convert2MethodRef
    assertDoesNotThrow(() -> buffer.toString());
  }

  /** Test reference equality. */
  @Test
  public void testEquals() {
    final int bufferSize = 1024;
    DatabricksArrowBuf buffer = newBuffer(bufferSize);
    //noinspection EqualsWithItself
    assertEquals(buffer, buffer, "Same object should be equal");

    DatabricksArrowBuf slice = (DatabricksArrowBuf) buffer.slice(0, bufferSize);
    assertNotEquals(buffer, slice, "Different object should not be equal");
  }

  /** Test hash code. */
  @Test
  public void testHashCode() {
    final int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      assertEquals(buffer.hashCode(), buffer.hashCode(), "Same object should have same hashcode.");

      DatabricksArrowBuf slice = (DatabricksArrowBuf) buffer.slice(0, bufferSize);
      assertNotEquals(
          buffer.hashCode(), slice.hashCode(), "Different object should have different hashcode. ");
    }
  }

  /** Test get and set on long. */
  @Test
  public void testGetAndSetLong() {
    final int bufferSize = 1024;
    ByteBuffer byteBuffer = newByteBuffer(bufferSize);
    try (DatabricksArrowBuf buffer = newBuffer(byteBuffer)) {
      for (int i = 0; i < bufferSize - Long.BYTES; i++) {
        @SuppressWarnings("UnnecessaryLocalVariable")
        long value = i;
        byteBuffer.putLong(i, value);
        assertEquals(value, buffer.getLong(i), "Long values should be same.");
      }

      for (int i = 0; i < bufferSize - Long.BYTES; i++) {
        long value = i + bufferSize;
        buffer.setLong(i, value);
        assertEquals(value, byteBuffer.getLong(i), "Long values should be same.");
      }
    }
  }

  /** Test get and set on float. */
  @Test
  public void testGetAndSetFloat() {
    final int bufferSize = 1024;
    ByteBuffer byteBuffer = newByteBuffer(bufferSize);
    try (DatabricksArrowBuf buffer = newBuffer(byteBuffer)) {
      for (int i = 0; i < bufferSize - Float.BYTES; i++) {
        float value = (float) (i * Math.PI);
        byteBuffer.putFloat(i, value);
        assertEquals(value, buffer.getFloat(i), "Float values should be same.");
      }

      for (int i = 0; i < bufferSize - Float.BYTES; i++) {
        float value = (float) ((i + bufferSize) * Math.PI);
        buffer.setFloat(i, value);
        assertEquals(value, byteBuffer.getFloat(i), "Float values should be same.");
      }
    }
  }

  /** Test get and set on double */
  @Test
  public void testGetAndSetDouble() {
    final int bufferSize = 1024;
    ByteBuffer byteBuffer = newByteBuffer(bufferSize);
    try (DatabricksArrowBuf buffer = newBuffer(byteBuffer)) {
      for (int i = 0; i < bufferSize - Double.BYTES; i++) {
        double value = i * Math.PI;
        byteBuffer.putDouble(i, value);
        assertEquals(value, buffer.getDouble(i), "Double values should be same.");
      }

      for (int i = 0; i < bufferSize - Double.BYTES; i++) {
        double value = (i + bufferSize) * Math.PI;
        buffer.setDouble(i, value);
        assertEquals(value, byteBuffer.getDouble(i), "Double values should be same.");
      }
    }
  }

  /** Test get and set on char */
  @Test
  public void testGetAndSetChar() {
    final int bufferSize = 1024;
    ByteBuffer byteBuffer = newByteBuffer(bufferSize);
    try (DatabricksArrowBuf buffer = newBuffer(byteBuffer)) {
      for (int i = 0; i < bufferSize - Character.BYTES; i++) {
        char value = (char) getByteValue(i);
        byteBuffer.putChar(i, value);
        assertEquals(value, buffer.getChar(i), "Character values should be same.");
      }

      for (int i = 0; i < bufferSize - Character.BYTES; i++) {
        char value = (char) getByteValue(i + bufferSize);
        buffer.setChar(i, value);
        assertEquals(value, byteBuffer.getChar(i), "Character values should be same.");
      }
    }
  }

  /** Test get and set on int */
  @Test
  public void testGetAndSetInt() {
    final int bufferSize = 1024;
    ByteBuffer byteBuffer = newByteBuffer(bufferSize);
    try (DatabricksArrowBuf buffer = newBuffer(byteBuffer)) {
      for (int i = 0; i < bufferSize - Integer.BYTES; i++) {
        @SuppressWarnings("UnnecessaryLocalVariable")
        int value = i;
        byteBuffer.putInt(i, value);
        assertEquals(value, buffer.getInt(i), "Integer values should be same.");
      }

      for (int i = 0; i < bufferSize - Integer.BYTES; i++) {
        int value = i + bufferSize;
        buffer.setInt(i, value);
        assertEquals(value, byteBuffer.getInt(i), "Integer values should be same.");
      }
    }
  }

  /** Test get and set on short */
  @Test
  public void testGetAndSetShort() {
    final int bufferSize = 1024;
    ByteBuffer byteBuffer = newByteBuffer(bufferSize);
    try (DatabricksArrowBuf buffer = newBuffer(byteBuffer)) {
      for (int i = 0; i < bufferSize - Short.BYTES; i++) {
        short value = (short) i;
        byteBuffer.putShort(i, value);
        assertEquals(value, buffer.getShort(i), "Short values should be same.");
      }

      for (int i = 0; i < bufferSize - Short.BYTES; i++) {
        short value = (short) (i + bufferSize);
        buffer.setShort(i, value);
        assertEquals(value, byteBuffer.getShort(i), "Short values should be same.");
      }
    }
  }

  /** Test get and set on byte */
  @Test
  public void testGetAndSetByte() {
    final int bufferSize = 1024;
    ByteBuffer byteBuffer = newByteBuffer(bufferSize);
    try (DatabricksArrowBuf buffer = newBuffer(byteBuffer)) {
      for (int i = 0; i < bufferSize - Byte.BYTES; i++) {
        byte value = getByteValue(i);
        byteBuffer.put(i, value);
        assertEquals(value, buffer.getByte(i), "Byte values should be same.");
      }

      for (int i = 0; i < bufferSize - Byte.BYTES; i++) {
        byte value = getByteValue(i + bufferSize);
        buffer.setByte(i, value);
        assertEquals(value, byteBuffer.get(i), "Byte values should be same.");
      }
    }
  }

  /** Test read byte and write byte. */
  @Test
  public void testReadByteAndWriteByte() {
    final int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      // Write bytes.
      for (int i = 0; i < bufferSize - Byte.BYTES; i++) {
        byte value = getByteValue(i);
        if (i % 2 == 0) {
          buffer.writeByte(value);
        } else {
          buffer.writeByte(i);
        }
      }

      // Read back the same bytes.
      for (int i = 0; i < bufferSize - Byte.BYTES; i++) {
        byte value = getByteValue(i);
        assertEquals(value, buffer.readByte(), "Byte values should be same.");
      }
    }
  }

  /** Test readBytes and writeBytes. */
  @Test
  public void testWriteBytesAndReadBytes() {
    final int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int size = 1; size < bufferSize; size++) {
        logger.info("Testing bytes of length {}", size);
        // Fill the write buffer.
        byte[] writeBytes = new byte[size];
        for (int i = 0; i < size; i++) {
          writeBytes[i] = getByteValue(i);
        }

        // Write data.
        buffer.clear();
        for (int i = 0; i + writeBytes.length < bufferSize; i += writeBytes.length) {
          buffer.writeBytes(writeBytes);
        }

        // Read the same data and validate.
        for (int i = 0; i + writeBytes.length < bufferSize; i += writeBytes.length) {
          byte[] readBytes = new byte[writeBytes.length];
          buffer.readBytes(readBytes);
          assertArrayEquals(writeBytes, readBytes, "Byte values should be same.");
        }
      }
    }
  }

  /** Test write methods - writeShort, writeInt, writeLong, writeFloat, writeDouble. */
  @Test
  public void testWriteOfNumbers() {
    final int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      // Write random numbers.
      Random random = new Random();
      List<Object> values = new ArrayList<>();
      for (int i = 0; i < bufferSize; /* incremented in loop */ ) {
        int bytesAvailable = bufferSize - i;
        if (bytesAvailable >= Double.BYTES) {
          int rand = random.nextInt(5);
          switch (rand) {
            case 0:
              short shortValue = (short) random.nextInt(Short.MAX_VALUE);
              buffer.writeShort(shortValue);
              values.add(shortValue);
              i += Short.BYTES;
              break;
            case 1:
              int intValue = random.nextInt();
              buffer.writeInt(intValue);
              values.add(intValue);
              i += Integer.BYTES;
              break;
            case 2:
              long longValue = random.nextLong();
              buffer.writeLong(longValue);
              values.add(longValue);
              i += Long.BYTES;
              break;
            case 3:
              float floatValue = random.nextFloat();
              buffer.writeFloat(floatValue);
              values.add(floatValue);
              i += Float.BYTES;
              break;
            case 4:
              double doubleValue = random.nextDouble();
              buffer.writeDouble(doubleValue);
              values.add(doubleValue);
              i += Double.BYTES;
              break;
            default:
              throw new IllegalArgumentException("Invalid random number " + rand);
          }
        } else {
          for (int j = 0; j < bytesAvailable; j++) {
            byte value = getByteValue(j);
            buffer.writeByte(value);
            values.add(value);
          }
          i += bytesAvailable;
        }
      }

      // Read and validate the numbers.
      int index = 0;
      for (Object value : values) {
        if (value instanceof Byte) {
          assertEquals(
              (Byte) value, buffer.getByte(index), "Byte values should be same at index " + index);
          index += Byte.BYTES;
        } else if (value instanceof Short) {
          assertEquals(
              (short) value,
              buffer.getShort(index),
              "Short values should be same at index " + index);
          index += Short.BYTES;
        } else if (value instanceof Integer) {
          assertEquals(
              (int) value, buffer.getInt(index), "Integer values should be same at index " + index);
          index += Integer.BYTES;
        } else if (value instanceof Long) {
          assertEquals(
              (long) value, buffer.getLong(index), "Long values should be same at index " + index);
          index += Long.BYTES;
        } else if (value instanceof Float) {
          assertEquals(
              (float) value,
              buffer.getFloat(index),
              "Float values should be same at index " + index);
          index += Float.BYTES;
        } else if (value instanceof Double) {
          assertEquals(
              (double) value,
              buffer.getDouble(index),
              "Double values should be same at index " + index);
          index += Double.BYTES;
        } else {
          throw new IllegalArgumentException("Invalid value " + value + " at index " + index);
        }
      }
    }
  }

  /** Test get and set on native byte arrays. */
  @Test
  public void testGetAndSetBytesOnNativeByteArrays() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int size = 1; size < bufferSize; size++) {
        logger.info("Testing bytes of length {}", size);
        // Fill the write buffer.
        byte[] writeBytes = new byte[size];
        for (int i = 0; i < size; i++) {
          writeBytes[i] = getByteValue(i);
        }

        // Write data.
        buffer.clear();
        for (int i = 0; i + writeBytes.length < bufferSize; i += writeBytes.length) {
          buffer.setBytes(i, writeBytes);
        }

        // Read the same data and validate.
        for (int i = 0; i + writeBytes.length < bufferSize; i += writeBytes.length) {
          byte[] readBytes = new byte[writeBytes.length];
          buffer.getBytes(i, readBytes);
          assertArrayEquals(writeBytes, readBytes, "Byte values should be same.");
        }
      }
    }
  }

  /** Test get and set on native byte arrays with index. */
  @Test
  public void testGetAndSetBytesOnNativeByteArraysWithIndex() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int size = 1; size < bufferSize; size++) {
        logger.info("Testing bytes of length {}", size);
        // Fill the write buffer.
        byte[] writeBytes = new byte[size];
        for (int i = 0; i < size; i++) {
          writeBytes[i] = getByteValue(i);
        }

        // Write data.
        buffer.clear();
        for (int i = 0; i + writeBytes.length < bufferSize; i += writeBytes.length) {
          // Write total=size data in stages of length 1, 2, 3, ...
          int windex = 0;
          int wlen = 1;
          while (windex < writeBytes.length) {
            int len = Math.min(writeBytes.length - windex, wlen - windex);
            buffer.setBytes(i + windex, writeBytes, windex, len);
            windex += len;
            wlen++;
          }
        }

        // Read the same data and validate.
        byte[] readBytes = new byte[writeBytes.length];
        for (int i = 0; i + readBytes.length < bufferSize; i += readBytes.length) {
          // Read total=size data in stages of length 1, 2, 3, ...
          int rindex = 0;
          int rlen = 1;
          while (rindex < readBytes.length) {
            int len = Math.min(readBytes.length - rindex, rlen - rindex);
            buffer.getBytes(i + rindex, readBytes, rindex, len);
            rindex += len;
            rlen++;
          }
          assertArrayEquals(writeBytes, readBytes, "Byte values should be same.");
        }
      }
    }
  }

  /** Test get and set on byte buffers. */
  @Test
  public void testGetAndSetBytesOnByteBuffers() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int size = 1; size < bufferSize; size++) {
        logger.info("Testing byte buffer of length {}", size);

        // Fill the write buffer.
        ByteBuffer writeByteBuffer = newByteBuffer(size);
        for (int i = 0; i < size; i++) {
          writeByteBuffer.put(getByteValue(i));
        }

        // Write data.
        buffer.clear();
        writeByteBuffer.flip();
        for (int i = 0;
            i + writeByteBuffer.capacity() < bufferSize;
            i += writeByteBuffer.capacity()) {
          writeByteBuffer.rewind();
          buffer.setBytes(i, writeByteBuffer);
        }

        // Read the same data and validate.
        for (int i = 0;
            i + writeByteBuffer.capacity() < bufferSize;
            i += writeByteBuffer.capacity()) {
          ByteBuffer readByteBuffer = newByteBuffer(writeByteBuffer.capacity());
          buffer.getBytes(i, readByteBuffer);
          assertArrayEquals(
              writeByteBuffer.array(), readByteBuffer.array(), "Byte values should be same.");
        }
      }
    }
  }

  /** Test get and set on byte buffers with index. */
  @Test
  public void testGetAndSetBytesOnByteBuffersWithIndex() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int size = 1; size < bufferSize; size++) {
        logger.info("Testing bytes of length {}", size);

        // Fill the write buffer.
        ByteBuffer writeByteBuffer = newByteBuffer(size);
        for (int i = 0; i < size; i++) {
          writeByteBuffer.put(getByteValue(i));
        }

        // Write data.
        buffer.clear();
        writeByteBuffer.flip();
        for (int i = 0;
            i + writeByteBuffer.capacity() < bufferSize;
            i += writeByteBuffer.capacity()) {
          // Write total=size data in stages of length 1, 2, 3, ...
          int windex = 0;
          int wlen = 1;
          while (windex < writeByteBuffer.capacity()) {
            int len = Math.min(writeByteBuffer.capacity() - windex, wlen - windex);
            buffer.setBytes(i + windex, writeByteBuffer, windex, len);
            windex += len;
            wlen++;
          }
        }

        // Read the same data and validate.
        ByteBuffer readByteBuffer = newByteBuffer(writeByteBuffer.capacity());
        for (int i = 0;
            i + readByteBuffer.capacity() < bufferSize;
            i += readByteBuffer.capacity()) {
          buffer.getBytes(i, readByteBuffer);
          assertArrayEquals(
              writeByteBuffer.array(), readByteBuffer.array(), "Byte values should be same.");
        }
      }
    }
  }

  /** Test get and set bytes on Arrow buffer. */
  @Test
  public void testGetAndSetBytesOnArrowBuf() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int size = 1; size < bufferSize; size++) {
        logger.info("Testing buffers of length {}", size);

        // Set data in write buffer.
        DatabricksArrowBuf writeBuffer = newBuffer(size);
        for (int i = 0; i < size; i++) {
          writeBuffer.writeByte(getByteValue(i));
        }

        // Copy data to buffer.
        buffer.clear();
        for (int i = 0;
            i + writeBuffer.capacity() < bufferSize;
            i += (int) writeBuffer.capacity()) {
          writeBuffer.readerIndex(0);
          buffer.setBytes(i, writeBuffer);
        }

        // Read the same data and validate.
        DatabricksArrowBuf readBuffer = newBuffer(size);
        for (int i = 0; i + readBuffer.capacity() < bufferSize; i += (int) readBuffer.capacity()) {
          readBuffer.clear();
          buffer.getBytes(i, readBuffer, 0, (int) readBuffer.capacity());

          byte[] readBytes = new byte[(int) readBuffer.capacity()];
          readBuffer.getBytes(0, readBytes);

          byte[] writeBytes = new byte[readBytes.length];
          writeBuffer.getBytes(0, writeBytes);

          assertArrayEquals(writeBytes, readBytes, "Byte values should be same for size " + size);
        }
      }
    }
  }

  @Test
  public void testGetAndSetBytesOnArrowBufWithIndex() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int size = 1; size < bufferSize; size++) {
        logger.info("Testing buffers of length {}", size);

        // Set data in write buffer.
        DatabricksArrowBuf writeBuffer = newBuffer(size);
        for (int i = 0; i < size; i++) {
          writeBuffer.writeByte(getByteValue(i));
        }

        // Copy data to buffer.
        buffer.clear();
        for (int i = 0;
            i + writeBuffer.capacity() < bufferSize;
            i += (int) writeBuffer.capacity()) {
          writeBuffer.readerIndex(0);
          // Write total=size data in stages of length 1, 2, 3, ...
          int windex = 0;
          int wlen = 1;
          while (windex < writeBuffer.capacity()) {
            int len = Math.min((int) writeBuffer.capacity() - windex, wlen - windex);
            buffer.setBytes(i + windex, writeBuffer, windex, len);
            windex += len;
            wlen++;
          }
        }

        // Read the same data and validate.
        DatabricksArrowBuf readBuffer = newBuffer(size);
        for (int i = 0; i + readBuffer.capacity() < bufferSize; i += (int) readBuffer.capacity()) {
          readBuffer.clear();
          buffer.getBytes(i, readBuffer, 0, (int) readBuffer.capacity());

          byte[] readBytes = new byte[(int) readBuffer.capacity()];
          readBuffer.getBytes(0, readBytes);

          byte[] writeBytes = new byte[readBytes.length];
          writeBuffer.getBytes(0, writeBytes);

          assertArrayEquals(writeBytes, readBytes, "Byte values should be same for size " + size);
        }
      }
    }
  }

  /** Test get and set bytes on streams. */
  @Test
  public void testGetAndSetBytesOnInputAndOutputStream() throws IOException {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int size = 1; size < bufferSize; size++) {
        logger.info("Testing streams of length {}", size);
        // Fill the write buffer.
        byte[] writeBytes = new byte[size];
        for (int i = 0; i < size; i++) {
          writeBytes[i] = getByteValue(i);
        }

        // Write data.
        buffer.clear();
        for (int i = 0; i + writeBytes.length < bufferSize; i += writeBytes.length) {
          buffer.setBytes(i, new ByteArrayInputStream(writeBytes), writeBytes.length);
        }

        // Read the same data and validate.
        for (int i = 0; i + writeBytes.length < bufferSize; i += writeBytes.length) {
          ByteArrayOutputStream readBytes = new ByteArrayOutputStream(writeBytes.length);
          buffer.getBytes(i, readBytes, writeBytes.length);
          assertArrayEquals(writeBytes, readBytes.toByteArray(), "Byte values should be same.");
        }
      }
    }
  }

  /** Test possible memory consumed. */
  @Test
  public void testPossibleMemoryConsumed() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      assertEquals(
          buffer.capacity(),
          buffer.getPossibleMemoryConsumed(),
          "Memory consumed should be same for size " + buffer.capacity());
    }
  }

  /** Test actual memory consumed. */
  @Test
  public void testActualMemoryConsumed() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      assertEquals(
          buffer.capacity(),
          buffer.getActualMemoryConsumed(),
          "Memory consumed should be same for size " + buffer.capacity());
    }
  }

  /** Test hex string does not throw exception. */
  @Test
  public void testToHexString() {
    int bufferSize = 1024;
    DatabricksArrowBuf buffer = newBuffer(bufferSize);

    assertDoesNotThrow(
        () -> buffer.toHexString(0, bufferSize), "To hex string should not throw exception.");
    for (int i = 0; i < buffer.capacity(); i++) {
      buffer.writeByte(getByteValue(i));
      assertDoesNotThrow(
          () -> buffer.toHexString(0, bufferSize), "To hex string should not throw exception.");
    }

    buffer.clear();
    assertDoesNotThrow(
        () -> buffer.toHexString(0, bufferSize), "To hex string should not throw exception.");

    buffer.close();
    assertDoesNotThrow(
        () -> buffer.toHexString(0, bufferSize), "To hex string should not throw exception.");
  }

  /** Test print. */
  @Test
  public void testPrint() {
    int bufferSize = 1024;
    DatabricksArrowBuf buffer = newBuffer(bufferSize);

    testPrint(buffer);
    for (int i = 0; i < buffer.capacity(); i++) {
      buffer.writeByte(getByteValue(i));
      testPrint(buffer);
    }
  }

  private void testPrint(DatabricksArrowBuf buffer) {
    for (int indent = 0; indent <= 8; indent++) {
      StringBuilder sb = new StringBuilder();
      buffer.print(sb, indent);
      assertTrue(sb.length() > 0, "Print failed");
    }
  }

  /** Test reader and writer index. */
  @Test
  public void testReaderAndWriterIndex() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int i = 0; i < buffer.capacity(); i++) {
        buffer.writeByte(getByteValue(i));
        assertEquals(i + 1, buffer.writerIndex(), "writerIndex should be same");
      }

      for (int i = 0; i < buffer.capacity(); i++) {
        buffer.readByte();
        assertEquals(i + 1, buffer.readerIndex(), "readerIndex should be same");
      }
    }
  }

  /** Test empty buffer (size 0) behavior. */
  @Test
  public void testEmptyBuffer() {
    try (DatabricksArrowBuf buffer = newBuffer(0)) {
      // Basic properties
      assertEquals(0, buffer.capacity(), "Empty buffer should have 0 capacity");
      assertEquals(0, buffer.readableBytes(), "Empty buffer should have 0 readable bytes");
      assertEquals(0, buffer.writableBytes(), "Empty buffer should have 0 writable bytes");

      // checkBytes with 0 length should succeed
      assertDoesNotThrow(() -> buffer.checkBytes(0, 0), "checkBytes(0, 0) should not throw");

      // nioBuffer should return empty buffer
      ByteBuffer nioBuffer = buffer.nioBuffer();
      assertEquals(0, nioBuffer.remaining(), "NIO buffer should be empty");

      // slice() should work
      ArrowBuf slice = buffer.slice();
      assertEquals(0, slice.capacity(), "Slice of empty buffer should be empty");

      // slice(0, 0) should work
      ArrowBuf slice2 = buffer.slice(0, 0);
      assertEquals(0, slice2.capacity(), "slice(0, 0) should return empty buffer");
    }
  }

  /** Test set zero. */
  @Test
  public void testSetZero() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int i = 0; i < buffer.capacity(); i++) {
        buffer.writeByte(getByteValue(i));
      }

      buffer.clear();
      int index = 0;
      int size = 1;
      while (index < buffer.capacity()) {
        int len = Math.min((int) buffer.capacity() - index, size);
        buffer.setZero(index, len);
        index += len;
        size += 1;
      }

      for (int i = 0; i < buffer.capacity(); i++) {
        assertEquals(0, buffer.getByte(i), "Byte values should be same at index " + index);
      }
    }
  }

  /** Test set zero. */
  @Test
  public void testSetOne() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int i = 0; i < buffer.capacity(); i++) {
        buffer.writeByte(getByteValue(i));
      }

      buffer.clear();
      int index = 0;
      int size = 1;
      while (index < buffer.capacity()) {
        int len = Math.min((int) buffer.capacity() - index, size);
        //noinspection deprecation
        buffer.setOne(index, len);
        index += len;
        size += 1;
      }

      for (int i = 0; i < buffer.capacity(); i++) {
        assertEquals(
            (byte) 0xff, buffer.getByte(i), "Byte values should be same at index " + index);
      }
    }
  }

  /** Test realloc. */
  @Test
  public void testRealloc() {
    int bufferSize = 1024;
    DatabricksArrowBuf buffer = newBuffer(bufferSize);

    for (int size = 0; size < buffer.capacity(); size++) {
      ArrowBuf realloced = buffer.reallocIfNeeded(size);
      assertEquals(buffer, realloced, "Should be the same");
    }

    assertThrows(
        UnsupportedOperationException.class,
        () -> buffer.reallocIfNeeded(buffer.capacity() + 1),
        "Realloc above capacity should fail.");
  }

  /** Test clear. */
  @Test
  public void testClear() {
    int bufferSize = 1024;
    try (DatabricksArrowBuf buffer = newBuffer(bufferSize)) {
      for (int i = 0; i < buffer.capacity(); i++) {
        buffer.writeByte(getByteValue(i));
      }
      for (int i = 0; i < buffer.capacity(); i++) {
        buffer.readByte();
      }

      assertEquals(buffer.capacity(), buffer.writerIndex(), "Write index should match");
      assertEquals(buffer.capacity(), buffer.readerIndex(), "Read index should match");

      buffer.clear();
      assertEquals(0, buffer.writerIndex(), "Write index should be zero");
      assertEquals(0, buffer.readerIndex(), "Write index should be zero");
    }
  }

  @SuppressWarnings("SameParameterValue")
  private ByteBuffer newByteBuffer(int size) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(size);
    byteBuffer.order(BYTE_ORDER);
    return byteBuffer;
  }

  private DatabricksArrowBuf newBuffer(ByteBuffer byteBuffer) {
    final int bufferSize = byteBuffer.capacity();
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    DatabricksReferenceManager refManager = new DatabricksReferenceManager(allocator, bufferSize);
    return new DatabricksArrowBuf(refManager, null, byteBuffer, 0, bufferSize);
  }

  @SuppressWarnings("resource")
  private DatabricksArrowBuf newBuffer(int size) {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    return (DatabricksArrowBuf) allocator.buffer(size);
  }

  private byte getByteValue(int index) {
    return (byte) (index % 256);
  }
}
