package com.databricks.jdbc.api.impl.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/** Creates {@link BufferAllocator} instances. */
public class ArrowBufferAllocator {

  /**
   * @return an instance of the {@code BufferAllocator}.
   */
  public static BufferAllocator getBufferAllocator() {
    return new RootAllocator();
  }
}
