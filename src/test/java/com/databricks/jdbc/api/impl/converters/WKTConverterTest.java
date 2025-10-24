package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksValidationException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Test class for WKTConverter utility. */
public class WKTConverterTest {

  @Test
  public void testToWKB_ValidWKT() throws DatabricksValidationException {
    String wkt = "POINT(1 2)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);
    // WKB is binary data, not UTF-8 bytes of WKT
    // We can verify it's valid WKB by converting it back to WKT
    String convertedBack = WKTConverter.toWKT(wkb);
    // JTS outputs "POINT (1 2)" with a space after POINT, which is valid WKT
    assertEquals("POINT (1 2)", convertedBack);
  }

  @Test
  public void testToWKB_NullWKT() {
    assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKB(null));
  }

  @Test
  public void testToWKB_EmptyWKT() {
    assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKB(""));
  }

  @Test
  public void testToWKB_WhitespaceWKT() {
    assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKB("   "));
  }

  @Test
  public void testExtractSRIDFromEWKT_WithSRID() {
    String ewkt = "SRID=4326;POINT(1 2)";
    int srid = WKTConverter.extractSRIDFromEWKT(ewkt);
    assertEquals(4326, srid);
  }

  @Test
  public void testExtractSRIDFromEWKT_WithoutSRID() {
    String wkt = "POINT(1 2)";
    int srid = WKTConverter.extractSRIDFromEWKT(wkt);
    assertEquals(0, srid);
  }

  @Test
  public void testExtractSRIDFromEWKT_Null() {
    int srid = WKTConverter.extractSRIDFromEWKT(null);
    assertEquals(0, srid);
  }

  @Test
  public void testExtractSRIDFromEWKT_Empty() {
    int srid = WKTConverter.extractSRIDFromEWKT("");
    assertEquals(0, srid);
  }

  @Test
  public void testExtractSRIDFromEWKT_InvalidSRID() {
    String ewkt = "SRID=invalid;POINT(1 2)";
    int srid = WKTConverter.extractSRIDFromEWKT(ewkt);
    assertEquals(0, srid); // Should return 0 for invalid SRID
  }

  @Test
  public void testExtractSRIDFromEWKT_NoSemicolon() {
    String ewkt = "SRID=4326POINT(1 2)";
    int srid = WKTConverter.extractSRIDFromEWKT(ewkt);
    assertEquals(0, srid); // Should return 0 if no semicolon
  }

  @Test
  public void testRemoveSRIDFromEWKT_WithSRID() {
    String ewkt = "SRID=4326;POINT(1 2)";
    String wkt = WKTConverter.removeSRIDFromEWKT(ewkt);
    assertEquals("POINT(1 2)", wkt);
  }

  @Test
  public void testRemoveSRIDFromEWKT_WithoutSRID() {
    String wkt = "POINT(1 2)";
    String result = WKTConverter.removeSRIDFromEWKT(wkt);
    assertEquals("POINT(1 2)", result);
  }

  @Test
  public void testRemoveSRIDFromEWKT_Null() {
    String result = WKTConverter.removeSRIDFromEWKT(null);
    assertNull(result);
  }

  @Test
  public void testRemoveSRIDFromEWKT_Empty() {
    String result = WKTConverter.removeSRIDFromEWKT("");
    assertEquals("", result);
  }

  @Test
  public void testRemoveSRIDFromEWKT_NoSemicolon() {
    String ewkt = "SRID=4326POINT(1 2)";
    String result = WKTConverter.removeSRIDFromEWKT(ewkt);
    assertEquals("SRID=4326POINT(1 2)", result); // Should return as-is if no semicolon
  }

  @Test
  public void testRemoveSRIDFromEWKT_OnlySRID() {
    String ewkt = "SRID=4326;";
    String result = WKTConverter.removeSRIDFromEWKT(ewkt);
    assertEquals("", result);
  }

  @Test
  public void testToWKB_InvalidWKTFormat_LogsError() {
    String invalidWkt = "POINT(1 2 3 4 5)"; // Too many coordinates for a POINT

    DatabricksValidationException exception =
        assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKB(invalidWkt));

    assertTrue(exception.getMessage().contains("Invalid WKT format"));
  }

  @Test
  public void testToWKT_ValidWKB() throws DatabricksValidationException {
    // First create valid WKB from WKT
    byte[] wkb = WKTConverter.toWKB("POINT (1 2)");
    String wkt = WKTConverter.toWKT(wkb);

    assertEquals("POINT (1 2)", wkt);
  }

  @Test
  public void testToWKT_NullWKB() {
    assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKT(null));
  }

  @Test
  public void testToWKT_EmptyWKB() {
    assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKT(new byte[0]));
  }

  @Test
  public void testToWKT_InvalidWKB_LogsError() {
    byte[] invalidWkb = new byte[] {1, 2, 3, 4, 5}; // Random invalid bytes

    DatabricksValidationException exception =
        assertThrows(DatabricksValidationException.class, () -> WKTConverter.toWKT(invalidWkb));

    assertTrue(exception.getMessage().contains("Invalid WKB format"));
  }

  @Test
  public void testConcurrency() throws Exception {
    int numThreads = 50;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch latch = new CountDownLatch(numThreads);
    List<Future<?>> futures = new ArrayList<>();

    for (int threadNum = 0; threadNum < numThreads; threadNum++) {
      final int threadId = threadNum;
      Future<?> future =
          executor.submit(
              () -> {
                try {
                  int x = threadId;
                  int y = threadId * 2;
                  String wkt = String.format("POINT (%d %d)", x, y);

                  byte[] wkb = WKTConverter.toWKB(wkt);
                  assertNotNull(wkb, "Thread " + threadId + ": WKB should not be null");

                  String convertedWkt = WKTConverter.toWKT(wkb);
                  assertEquals(wkt, convertedWkt, "Thread " + threadId + ": WKT mismatch");

                } catch (Exception e) {
                  fail("Thread " + threadId + " failed: " + e.getMessage());
                } finally {
                  latch.countDown();
                }
              });
      futures.add(future);
    }

    assertTrue(latch.await(10, TimeUnit.SECONDS), "Not all threads completed in time");
    executor.shutdown();

    // Verify all threads completed successfully
    for (Future<?> future : futures) {
      future.get();
    }
  }
}
