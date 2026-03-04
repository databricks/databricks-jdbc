package org.apache.arrow.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;

/** Test allocation reservation */
@Tag("Jvm17PlusAndArrowToNioReflectionDisabled")
@EnabledOnJre({JRE.JAVA_17, JRE.JAVA_21})
public class DatabricksAllocationReservationTest {
  /** Test reserve and allocate */
  @Test
  public void testReservation() {
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    DatabricksAllocationReservation reservation = new DatabricksAllocationReservation(allocator);

    Random random = new Random();
    long totalReservation = 0;
    for (int i = 0; i < 1000; i++) {
      long size = random.nextInt(1000);
      assertTrue(reservation.reserve(size), "Reservation should return true");
      totalReservation += size;
      assertEquals(totalReservation, reservation.getSizeLong(), "Reservation should match");
    }

    ArrowBuf buffer = reservation.allocateBuffer();
    assertInstanceOf(DatabricksArrowBuf.class, buffer, "Buffer type should match");
    assertTrue(buffer.capacity() >= totalReservation, "Reservation should be allocated");
  }

  /** Test fail on reuse */
  @Test
  public void testFailureOnReuse() {
    long bufferSize = 1024;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    DatabricksAllocationReservation reservation = new DatabricksAllocationReservation(allocator);
    reservation.reserve(bufferSize);

    ArrowBuf buffer = reservation.allocateBuffer();
    assertInstanceOf(DatabricksArrowBuf.class, buffer, "Buffer type should match");
    assertTrue(buffer.capacity() >= bufferSize, "Reservation should be allocated");

    assertTrue(reservation.isUsed(), "Reservation should be used");

    assertThrows(
        IllegalStateException.class,
        () -> reservation.reserve(10L),
        "Reuse after allocate should fail");
    assertThrows(
        IllegalStateException.class,
        reservation::allocateBuffer,
        "Reuse after allocate should fail");
  }

  /** Test fail on reuse after close */
  @Test
  public void testFailureOnClose() {
    long bufferSize = 1024;
    DatabricksBufferAllocator allocator = new DatabricksBufferAllocator();
    DatabricksAllocationReservation reservation = new DatabricksAllocationReservation(allocator);
    reservation.reserve(bufferSize);

    ArrowBuf buffer = reservation.allocateBuffer();
    assertInstanceOf(DatabricksArrowBuf.class, buffer, "Buffer type should match");
    assertTrue(buffer.capacity() >= bufferSize, "Reservation should be allocated");

    reservation.close();
    assertTrue(reservation.isClosed(), "Reservation should have been closed");

    assertThrows(
        IllegalStateException.class,
        () -> reservation.reserve(10L),
        "Reuse after close should fail");
    assertThrows(
        IllegalStateException.class, reservation::allocateBuffer, "Reuse after close should fail");
  }
}
