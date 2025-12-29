package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksValidationException;
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
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

  /** Inner class to hold test case data from JSON. */
  private static class WKBTestCase {
    String wkt;
    String wkb;
  }

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

    assertTrue(exception.getMessage().contains("Failed to parse WKT"));
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

    assertTrue(exception.getMessage().contains("Failed to parse WKB"));
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

  // ========== 3D and 4D Geometry Tests (Z, M, ZM coordinates) ==========

  @Test
  public void testToWKB_PointZ_3D() throws DatabricksValidationException {
    String wkt = "POINT Z (1 2 3)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    // Convert back and verify Z coordinate is preserved
    // Note: JTS outputs without space after Z
    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POINT Z(1 2 3)", convertedBack);
  }

  @Test
  public void testToWKB_PointM_3D() throws DatabricksValidationException {
    String wkt = "POINT M (1 2 3)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    // Convert back and verify M coordinate is preserved
    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POINT M(1 2 3)", convertedBack);
  }

  @Test
  public void testToWKB_PointZM_4D() throws DatabricksValidationException {
    String wkt = "POINT ZM (1 2 3 4)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    // Convert back and verify both Z and M coordinates are preserved
    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POINT ZM(1 2 3 4)", convertedBack);
  }

  @Test
  public void testToWKB_LineStringZ() throws DatabricksValidationException {
    String wkt = "LINESTRING Z (0 0 0, 1 1 1, 2 2 2)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("LINESTRING Z(0 0 0, 1 1 1, 2 2 2)", convertedBack);
  }

  @Test
  public void testToWKB_LineStringM() throws DatabricksValidationException {
    String wkt = "LINESTRING M (0 0 10, 1 1 20, 2 2 30)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("LINESTRING M(0 0 10, 1 1 20, 2 2 30)", convertedBack);
  }

  @Test
  public void testToWKB_LineStringZM() throws DatabricksValidationException {
    String wkt = "LINESTRING ZM (0 0 0 10, 1 1 1 20, 2 2 2 30)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("LINESTRING ZM(0 0 0 10, 1 1 1 20, 2 2 2 30)", convertedBack);
  }

  @Test
  public void testToWKB_PolygonZ() throws DatabricksValidationException {
    String wkt = "POLYGON Z ((0 0 0, 4 0 0, 4 4 0, 0 4 0, 0 0 0))";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POLYGON Z((0 0 0, 4 0 0, 4 4 0, 0 4 0, 0 0 0))", convertedBack);
  }

  @Test
  public void testToWKB_PolygonM() throws DatabricksValidationException {
    String wkt = "POLYGON M ((0 0 1, 4 0 2, 4 4 3, 0 4 4, 0 0 1))";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POLYGON M((0 0 1, 4 0 2, 4 4 3, 0 4 4, 0 0 1))", convertedBack);
  }

  @Test
  public void testToWKB_PolygonZM() throws DatabricksValidationException {
    String wkt = "POLYGON ZM ((0 0 0 1, 4 0 0 2, 4 4 0 3, 0 4 0 4, 0 0 0 1))";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POLYGON ZM((0 0 0 1, 4 0 0 2, 4 4 0 3, 0 4 0 4, 0 0 0 1))", convertedBack);
  }

  @Test
  public void testToWKB_MultiPointZ() throws DatabricksValidationException {
    String wkt = "MULTIPOINT Z ((1 2 3), (4 5 6))";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("MULTIPOINT Z((1 2 3), (4 5 6))", convertedBack);
  }

  @Test
  public void testToWKB_EmptyPoint() throws DatabricksValidationException {
    String wkt = "POINT EMPTY";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POINT EMPTY", convertedBack);
  }

  @Test
  public void testToWKB_EmptyLineString() throws DatabricksValidationException {
    String wkt = "LINESTRING EMPTY";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    assertTrue(wkb.length > 0);

    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("LINESTRING EMPTY", convertedBack);
  }

  @Test
  public void testToWKB_PointZ_NegativeCoordinates() throws DatabricksValidationException {
    String wkt = "POINT Z (-122.4 37.8 100.5)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POINT Z(-122.4 37.8 100.5)", convertedBack);
  }

  @Test
  public void testToWKB_PointM_DecimalMeasure() throws DatabricksValidationException {
    String wkt = "POINT M (10.5 20.3 1234567890.123)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POINT M(10.5 20.3 1234567890.123)", convertedBack);
  }

  @Test
  public void testToWKB_PointZM_AllDimensions() throws DatabricksValidationException {
    String wkt = "POINT ZM (-71.06 42.36 10.0 1609459200)";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("POINT ZM(-71.06 42.36 10 1609459200)", convertedBack);
  }

  @Test
  public void testToWKB_GeometryCollectionZ() throws DatabricksValidationException {
    String wkt = "GEOMETRYCOLLECTION Z (POINT Z (1 2 3), LINESTRING Z (0 0 0, 1 1 1))";
    byte[] wkb = WKTConverter.toWKB(wkt);

    assertNotNull(wkb);
    String convertedBack = WKTConverter.toWKT(wkb);
    assertEquals("GEOMETRYCOLLECTION Z(POINT Z(1 2 3), LINESTRING Z(0 0 0, 1 1 1))", convertedBack);
  }

  // ========== WKB → WKT → WKB Round-trip Tests ==========

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_Point() throws DatabricksValidationException {
    // Start with WKT, convert to WKB (our "original" WKB)
    String originalWkt = "POINT (1 2)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    // Round trip: WKB → WKT → WKB
    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    // Verify WKB bytes are identical
    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_LineString() throws DatabricksValidationException {
    String originalWkt = "LINESTRING (0 0, 1 1, 2 2)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_Polygon() throws DatabricksValidationException {
    String originalWkt = "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_PointZ() throws DatabricksValidationException {
    String originalWkt = "POINT Z (1 2 3)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    // Round trip: WKB → WKT → WKB
    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_PointM() throws DatabricksValidationException {
    String originalWkt = "POINT M (1 2 3)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_PointZM() throws DatabricksValidationException {
    String originalWkt = "POINT ZM (1 2 3 4)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_LineStringZ() throws DatabricksValidationException {
    String originalWkt = "LINESTRING Z (0 0 0, 1 1 1, 2 2 2)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_LineStringM() throws DatabricksValidationException {
    String originalWkt = "LINESTRING M (0 0 10, 1 1 20, 2 2 30)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_LineStringZM() throws DatabricksValidationException {
    String originalWkt = "LINESTRING ZM (0 0 0 10, 1 1 1 20, 2 2 2 30)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_PolygonZ() throws DatabricksValidationException {
    String originalWkt = "POLYGON Z ((0 0 0, 4 0 0, 4 4 0, 0 4 0, 0 0 0))";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_PolygonM() throws DatabricksValidationException {
    String originalWkt = "POLYGON M ((0 0 1, 4 0 2, 4 4 3, 0 4 4, 0 0 1))";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_PolygonZM() throws DatabricksValidationException {
    String originalWkt = "POLYGON ZM ((0 0 0 1, 4 0 0 2, 4 4 0 3, 0 4 0 4, 0 0 0 1))";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_MultiPointZ() throws DatabricksValidationException {
    String originalWkt = "MULTIPOINT Z ((1 2 3), (4 5 6))";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_GeometryCollectionZ()
      throws DatabricksValidationException {
    String originalWkt = "GEOMETRYCOLLECTION Z (POINT Z (1 2 3), LINESTRING Z (0 0 0, 1 1 1))";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_EmptyPoint() throws DatabricksValidationException {
    String originalWkt = "POINT EMPTY";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_NegativeCoordinates()
      throws DatabricksValidationException {
    String originalWkt = "POINT Z (-122.4 37.8 100.5)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testRoundTrip_WKB_to_WKT_to_WKB_LargeDecimalValues()
      throws DatabricksValidationException {
    String originalWkt = "POINT M (10.5 20.3 1234567890.123)";
    byte[] originalWkb = WKTConverter.toWKB(originalWkt);

    String wkt = WKTConverter.toWKT(originalWkb);
    byte[] finalWkb = WKTConverter.toWKB(wkt);

    assertArrayEquals(originalWkb, finalWkb);
  }

  @Test
  public void testWktToWkb() {
    // Load test cases from JSON file
    InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("wkb_test_cases.json");
    assertNotNull(inputStream, "wkb_test_cases.json file not found");

    Gson gson = new Gson();
    Type listType = new TypeToken<List<WKBTestCase>>() {}.getType();
    List<WKBTestCase> testCases = gson.fromJson(new InputStreamReader(inputStream), listType);

    assertNotNull(testCases, "Test cases should not be null");
    assertFalse(testCases.isEmpty(), "Test cases should not be empty");

    // Track statistics
    int passed = 0;
    List<String> failures = new ArrayList<>();

    // Test each case - WKT to WKB conversion
    for (int i = 0; i < testCases.size(); i++) {
      WKBTestCase testCase = testCases.get(i);
      try {
        byte[] actualWkb = WKTConverter.toWKB(testCase.wkt);
        byte[] expectedWkb = BaseEncoding.base16().decode(testCase.wkb.toUpperCase());

        // Assert WKB bytes match expected
        assertArrayEquals(
            expectedWkb,
            actualWkb,
            String.format("Test case %d WKB mismatch: WKT='%s'", i, testCase.wkt));

        passed++;

      } catch (AssertionError | Exception e) {
        failures.add(
            String.format("Test case %d: WKT='%s', Error: %s", i, testCase.wkt, e.getMessage()));
      }
    }

    // Fail if there are any failures
    if (!failures.isEmpty()) {
      fail(
          String.format(
              "Failed %d out of %d test cases:\n%s",
              failures.size(), testCases.size(), String.join("\n", failures)));
    }
  }
}
