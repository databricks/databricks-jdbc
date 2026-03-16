package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.api.impl.arrow.ArrowBufferAllocatorTest.readAndWriteArrowData;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.IOException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DatabricksBufferAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that the fallback {@code DatabricksBufferAllocator} is used with Netty allocation manager
 * type when the creation of {@code RootAllocator} is not possible in the current JVM.
 *
 * <p>This test is in a separate class to ensure it runs in a fresh JVM with the system property set
 * before Arrow's static initialization.
 */
public class ArrowBufferAllocatorNettyManagerTest {
  private static final Logger logger =
      LoggerFactory.getLogger(ArrowBufferAllocatorNettyManagerTest.class);

  private static final String ARROW_ALLOCATION_MANAGER_TYPE = "arrow.allocation.manager.type";

  @BeforeAll
  static void setUpAllocationManagerType() {
    String originalValue = System.getProperty(ARROW_ALLOCATION_MANAGER_TYPE);
    logger.info("Original value of {} is {}", ARROW_ALLOCATION_MANAGER_TYPE, originalValue);
    System.setProperty(ARROW_ALLOCATION_MANAGER_TYPE, "Netty");
    logger.info("Setting system property {} to Netty", ARROW_ALLOCATION_MANAGER_TYPE);
  }

  @Test
  @Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
  @EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
  public void testCreateDatabricksBufferAllocatorWithNettyManagerType() throws IOException {
    try (BufferAllocator allocator = ArrowBufferAllocator.getBufferAllocator()) {
      assertInstanceOf(
          DatabricksBufferAllocator.class, allocator, "Should create DatabricksBufferAllocator");
      readAndWriteArrowData(allocator);
    }
  }
}
