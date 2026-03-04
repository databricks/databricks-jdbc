package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

/** Test buffer allocator */
@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
@EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
public class DatabricksBufferAllocatorTest {
  private static int LIMIT = Integer.MAX_VALUE;
  private static long ALLOCATED_MEMORY = 0;
  private static long INIT_RESERVATION = 0;
  private static long PEAK_MEMORY_ALLOCATION = 0;
  private static long HEADROOM = Integer.MAX_VALUE;
  private static boolean IS_OVERLIMIT = false;

  /** Test allocation. */
  @Test
  public void testAllocation() {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();

    for (int size = 0; size < 4096; size++) {
      ArrowBuf buffer = allocator.buffer(size);
      assertInstanceOf(DatabricksArrowBuf.class, buffer, "Should be of type DatabricksArrowBuf");
      assertTrue(buffer.capacity() >= size, "Should have expected capacity");
    }
  }

  /** Test parent child allocations. */
  @Test
  public void testParentChildAllocation() {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();

    for (int i = 0; i < 5_000; i++) {
      allocator.newChildAllocator("child" + i, 0, 0);

      allocator.getChildAllocators().stream()
          .forEach(
              c -> {
                assertInstanceOf(
                    DatabricksBufferAllocator.class,
                    c,
                    "Should be of type DatabricksBufferAllocator");
                assertEquals(
                    allocator, c.getParentAllocator(), "Allocator parent should be the same");
                assertEquals(allocator, c.getRoot(), "Allocator parent should be the same");
              });

      assertEquals(
          i + 1,
          allocator.getChildAllocators().size(),
          "Allocator children should " + "be" + " the same");
    }
  }

  /** Test get root in deeply nested allocators */
  @Test
  public void testGetRoot() {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();

    DatabricksBufferAllocator currentNode = allocator;
    for (int i = 0; i < 1000; i++) {
      currentNode = (DatabricksBufferAllocator) currentNode.newChildAllocator("child" + 1, 0, 0);
      assertEquals(allocator, currentNode.getRoot(), "Allocator root should be the same");
    }
  }

  /** Test constants returned by DatabricksBufferAllocator. */
  @Test
  public void testConstants() {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();

    for (int i = 0; i < 1000; i++) {
      allocator.newChildAllocator("child" + i, 0, 0);
      allocator.buffer(i);

      assertEquals(LIMIT, allocator.getLimit(), "Limit should be constant");
      assertEquals(
          ALLOCATED_MEMORY, allocator.getAllocatedMemory(), "Allocated memory should be constant");
      assertEquals(
          INIT_RESERVATION, allocator.getInitReservation(), "Init reservations should be constant");
      assertEquals(HEADROOM, allocator.getHeadroom(), "Headroom should be constant");
      assertEquals(LIMIT, allocator.getLimit(), "Limit should be constant");
      assertEquals(
          PEAK_MEMORY_ALLOCATION,
          allocator.getPeakMemoryAllocation(),
          "Peak memory should be constant");
      assertEquals(IS_OVERLIMIT, allocator.isOverLimit(), "Over limit should be constant");
      assertEquals(AllocationListener.NOOP, allocator.getListener(), "Listener should be NO-OP");
      assertEquals(
          DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY,
          allocator.getRoundingPolicy(),
          "Rounding policy should be default");
    }
  }

  /** Test verbose string */
  @Test
  public void testToVerboseString() {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();

    for (int i = 0; i < 1000; i++) {
      allocator.newChildAllocator("child" + i, 0, 0);
      allocator.buffer(i);
      assertDoesNotThrow(allocator::toVerboseString, "Verbose string should faile");
    }
  }

  /** Test force allocate */
  @Test
  public void testForceAllocate() {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();

    DatabricksBufferAllocator currentNode = allocator;
    for (int i = 0; i < 1000; i++) {
      currentNode = (DatabricksBufferAllocator) allocator.newChildAllocator("child" + i, 0, 0);
      currentNode.buffer(i);

      assertTrue(currentNode.forceAllocate(i), "Force allocate should succeed");
    }
  }

  /** Test assert open */
  @Test
  public void testAssertOpen() {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    assertDoesNotThrow(allocator::assertOpen, "Assert should not throw exception");
    allocator.close();
    assertThrows(
        IllegalStateException.class, allocator::assertOpen, "Assert should throw exception");
  }

  /** Test wrap foreign allocation fails */
  @Test
  public void testWrapForeignAllocationFails() {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    ForeignAllocation foreignAllocation =
        new ForeignAllocation(0, 0) {
          @Override
          protected void release0() {}
        };
    assertThrows(
        UnsupportedOperationException.class,
        () -> allocator.wrapForeignAllocation(foreignAllocation),
        "Wrap should fail");
  }
}
