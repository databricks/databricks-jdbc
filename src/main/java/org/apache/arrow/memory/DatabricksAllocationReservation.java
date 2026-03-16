package org.apache.arrow.memory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** An AllocationReservation implementation for cumulative allocation requests. */
class DatabricksAllocationReservation implements AllocationReservation {

  private final DatabricksBufferAllocator allocator;
  private final AtomicLong reservedSize = new AtomicLong(0);
  private final AtomicBoolean used = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public DatabricksAllocationReservation(DatabricksBufferAllocator allocator) {
    this.allocator = allocator;
  }

  @SuppressWarnings("removal")
  @Override
  @Deprecated
  public boolean add(int nBytes) {
    return add((long) nBytes);
  }

  @Override
  public boolean add(long nBytes) {
    assertNotUsed();
    if (nBytes < 0) {
      return false;
    }
    reservedSize.addAndGet(nBytes);
    return true;
  }

  @SuppressWarnings("removal")
  @Override
  @Deprecated
  public boolean reserve(int nBytes) {
    return reserve((long) nBytes);
  }

  @Override
  public boolean reserve(long nBytes) {
    assertNotUsed();
    if (nBytes < 0) {
      return false;
    }
    // Check if reservation would exceed limits
    long currentReservation = reservedSize.get();
    long newReservation = currentReservation + nBytes;
    if (newReservation > allocator.getHeadroom() + currentReservation) {
      return false;
    }
    reservedSize.addAndGet(nBytes);
    return true;
  }

  @Override
  public ArrowBuf allocateBuffer() {
    assertNotUsed();
    if (!used.compareAndSet(false, true)) {
      throw new IllegalStateException("Reservation already used");
    }
    long size = reservedSize.get();
    if (size == 0) {
      return allocator.getEmpty();
    }
    return allocator.buffer(size);
  }

  @Override
  public int getSize() {
    return (int) Math.min(reservedSize.get(), Integer.MAX_VALUE);
  }

  @Override
  public long getSizeLong() {
    return reservedSize.get();
  }

  @Override
  public boolean isUsed() {
    return used.get();
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void close() {
    closed.set(true);
  }

  private void assertNotUsed() {
    if (used.get()) {
      throw new IllegalStateException("Reservation already used");
    }
    if (closed.get()) {
      throw new IllegalStateException("Reservation is closed");
    }
  }
}
