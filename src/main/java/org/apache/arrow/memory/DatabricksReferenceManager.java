package org.apache.arrow.memory;

import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Databricks reference manager which acts as a no-op and does not reference count. All data is
 * allocated on the heap and taken care of by the JVM garbage collector.
 */
class DatabricksReferenceManager implements ReferenceManager {
  private static final Logger logger = LoggerFactory.getLogger(DatabricksReferenceManager.class);

  /** Allocator of this reference manager. */
  private final DatabricksBufferAllocator allocator;

  /** Size of this reference. */
  private final long size;

  /** The memory is heap allocated and taken care of by the JVM. Assuming value of one is safe. */
  private static final int REF_COUNT = 1;

  public DatabricksReferenceManager(DatabricksBufferAllocator allocator, long size) {
    this.allocator = allocator;
    this.size = size;
  }

  @Override
  public int getRefCount() {
    return REF_COUNT;
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    return getRefCount() == 0;
  }

  @Override
  public void retain() {
    retain(1);
  }

  @Override
  public void retain(int increment) {
    // Do nothing.
  }

  @Override
  public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
    DatabricksArrowBuf buf = checkBufferType(srcBuffer);
    return deriveBuffer(buf);
  }

  private ArrowBuf deriveBuffer(DatabricksArrowBuf srcBuffer) {
    return deriveBuffer(srcBuffer, 0, srcBuffer.capacity());
  }

  @Override
  public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, long index, long length) {
    Preconditions.checkArgument(
        length <= Integer.MAX_VALUE,
        "Length %s should be less than or equal to %s",
        length,
        Integer.MAX_VALUE);

    Preconditions.checkArgument(
        index + length <= sourceBuffer.capacity(),
        "Index="
            + index
            + " and length="
            + length
            + " exceeds source buffer capacity="
            + sourceBuffer.capacity());

    // Create a new DatabricksArrowBuf sharing the same byte buffer.
    DatabricksArrowBuf buf = checkBufferType(sourceBuffer);

    logger.debug("Deriving buffer at index {} and length {} from buffer {}", index, length, buf);

    return new DatabricksArrowBuf(
        this, null, buf.getByteBuffer(), buf.getOffset() + (int) index, length);
  }

  @Override
  public OwnershipTransferResult transferOwnership(
      ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
    DatabricksArrowBuf buf = checkBufferType(sourceBuffer);
    checkAllocatorType(targetAllocator);

    final ArrowBuf newBuf = deriveBuffer(buf);
    return new OwnershipTransferResult() {
      @Override
      public boolean getAllocationFit() {
        return true;
      }

      @Override
      public ArrowBuf getTransferredBuffer() {
        return newBuf;
      }
    };
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public long getAccountedSize() {
    return size;
  }

  private DatabricksArrowBuf checkBufferType(ArrowBuf buffer) {
    if (!(buffer instanceof DatabricksArrowBuf)) {
      throw new IllegalArgumentException("Buffer should be an instance of DatabricksArrowBuf");
    }
    return (DatabricksArrowBuf) buffer;
  }

  private DatabricksBufferAllocator checkAllocatorType(BufferAllocator bufferAllocator) {
    if (!(bufferAllocator instanceof DatabricksBufferAllocator)) {
      throw new IllegalArgumentException(
          "Allocator should be an instance of DatabricksBufferAllocator");
    }
    return (DatabricksBufferAllocator) bufferAllocator;
  }
}
