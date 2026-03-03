package org.apache.arrow.memory;

/**
 * A Databricks specific no-op ReferenceManager that returns a <code>DatabricksBufferAllocator
 * </code>.
 */
class DatabricksReferenceManagerNOOP implements ReferenceManager {
  public static DatabricksReferenceManagerNOOP INSTANCE = new DatabricksReferenceManagerNOOP();

  private DatabricksReferenceManagerNOOP() {}

  @Override
  public int getRefCount() {
    return 1;
  }

  @Override
  public boolean release() {
    return false;
  }

  @Override
  public boolean release(int decrement) {
    return false;
  }

  @Override
  public void retain() {}

  @Override
  public void retain(int increment) {}

  @Override
  public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
    return srcBuffer;
  }

  @Override
  public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, long index, long length) {
    return sourceBuffer;
  }

  @Override
  public OwnershipTransferResult transferOwnership(
      ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
    return new OwnershipTransferNOOP(sourceBuffer);
  }

  @Override
  public BufferAllocator getAllocator() {
    return new DatabricksBufferAllocator();
  }

  @Override
  public long getSize() {
    return 0L;
  }

  @Override
  public long getAccountedSize() {
    return 0L;
  }
}
