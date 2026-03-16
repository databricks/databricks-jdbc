package org.apache.arrow.memory;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BufferAllocator implementation that uses DatabricksArrowBuf for memory allocation. This
 * allocator uses heap-based ByteBuffer storage instead of direct/off-heap memory, avoiding the need
 * for sun.misc.Unsafe operations.
 *
 * <p>This implementation is suitable for environments where direct memory access is restricted or
 * where heap-based memory management is preferred.
 */
public class DatabricksBufferAllocator implements BufferAllocator {
  private static final Logger logger = LoggerFactory.getLogger(DatabricksBufferAllocator.class);

  private final String name;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final DatabricksBufferAllocator parent;
  private final Set<DatabricksBufferAllocator> children = ConcurrentHashMap.newKeySet();

  // Empty buffer singleton
  private final ArrowBuf emptyBuffer;

  /** Creates a root allocator with default settings. */
  public DatabricksBufferAllocator() {
    this("DatabricksBufferAllocator");
  }

  /**
   * Creates a root allocator with specified limit.
   *
   * @param name the allocator name
   */
  public DatabricksBufferAllocator(String name) {
    this(name, null);
  }

  /**
   * Creates an allocator with full configuration.
   *
   * @param name the allocator name
   * @param parent the parent allocator (null for root)
   */
  public DatabricksBufferAllocator(String name, DatabricksBufferAllocator parent) {
    this.name = name;
    this.parent = parent;

    // Create an empty buffer with a no-op reference manager
    this.emptyBuffer = new DatabricksArrowBuf(DatabricksReferenceManagerNOOP.INSTANCE, null, 0);
  }

  @Override
  public ArrowBuf buffer(long size) {
    return buffer(size, null);
  }

  @Override
  public ArrowBuf buffer(long size, BufferManager manager) {
    assertOpen();
    Preconditions.checkArgument(size >= 0, "Buffer size must be non-negative");

    if (size == 0) {
      return getEmpty();
    }

    logger.debug("Allocating buffer of size {}", size);

    // Create the reference manager and buffer
    DatabricksReferenceManager refManager = new DatabricksReferenceManager(this, size);
    return new DatabricksArrowBuf(refManager, manager, size);
  }

  @Override
  public BufferAllocator getRoot() {
    if (parent == null) {
      return this;
    }
    return parent.getRoot();
  }

  @Override
  public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation) {
    return newChildAllocator(name, AllocationListener.NOOP, initReservation, maxAllocation);
  }

  @Override
  public BufferAllocator newChildAllocator(
      String name, AllocationListener listener, long initReservation, long maxAllocation) {
    assertOpen();

    DatabricksBufferAllocator child = new DatabricksBufferAllocator(name, this);

    children.add(child);

    return child;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    // Close all children first
    for (DatabricksBufferAllocator child : children) {
      child.close();
    }
    children.clear();

    // Remove from parent's children list
    if (parent != null) {
      parent.children.remove(this);
    }
  }

  @Override
  public long getAllocatedMemory() {
    return 0;
  }

  @Override
  public long getLimit() {
    return Integer.MAX_VALUE;
  }

  @Override
  public long getInitReservation() {
    return 0;
  }

  @Override
  public void setLimit(long newLimit) {
    // Do nothing.
  }

  @Override
  public long getPeakMemoryAllocation() {
    // Do nothing.
    return 0;
  }

  @Override
  public long getHeadroom() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean forceAllocate(long size) {
    if (parent != null) {
      parent.forceAllocate(size);
    }
    return true;
  }

  @Override
  public void releaseBytes(long size) {
    // Do nothing.
  }

  @Override
  public AllocationListener getListener() {
    return AllocationListener.NOOP;
  }

  @Override
  public BufferAllocator getParentAllocator() {
    return parent;
  }

  @Override
  public Collection<BufferAllocator> getChildAllocators() {
    return Collections.unmodifiableSet(children);
  }

  @Override
  public AllocationReservation newReservation() {
    assertOpen();
    return new DatabricksAllocationReservation(this);
  }

  @Override
  public ArrowBuf getEmpty() {
    return emptyBuffer;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isOverLimit() {
    // Never over limit.
    return false;
  }

  @Override
  public String toVerboseString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Allocator(").append(name).append(") ");
    if (!children.isEmpty()) {
      sb.append("\n  Children:\n");
      for (DatabricksBufferAllocator child : children) {
        sb.append("    ").append(child.toVerboseString().replace("\n", "\n    ")).append("\n");
      }
    }
    return sb.toString();
  }

  @Override
  public void assertOpen() {
    if (closed.get()) {
      throw new IllegalStateException("Allocator " + name + " is closed");
    }
  }

  @Override
  public RoundingPolicy getRoundingPolicy() {
    return DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY;
  }

  @Override
  public ArrowBuf wrapForeignAllocation(ForeignAllocation allocation) {
    throw new UnsupportedOperationException(
        "DatabricksBufferAllocator does not support foreign allocations");
  }
}
