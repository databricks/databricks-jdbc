package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksGeography;
import com.databricks.jdbc.api.impl.DatabricksGeometry;
import com.databricks.jdbc.exception.DatabricksSQLException;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

/** Test class for GeospatialConverter. */
public class GeospatialConverterTest {

  private final GeospatialConverter converter = new GeospatialConverter();

  // ===================================================================================
  // toDatabricksGeometry() tests
  // ===================================================================================

  @Test
  public void testToDatabricksGeometry_WithValidWKT() throws DatabricksSQLException {
    DatabricksGeometry result = converter.toDatabricksGeometry("POINT(1 2)");
    assertNotNull(result);
    assertEquals("POINT(1 2)", result.getWKT());
    assertEquals(0, result.getSRID());
  }

  @Test
  public void testToDatabricksGeometry_WithEWKT() throws DatabricksSQLException {
    DatabricksGeometry result = converter.toDatabricksGeometry("SRID=4326;POINT(1 2)");
    assertNotNull(result);
    assertEquals("POINT(1 2)", result.getWKT());
    assertEquals(4326, result.getSRID());
  }

  @Test
  public void testToDatabricksGeometry_WithTextInput() throws DatabricksSQLException {
    Text textInput = new Text("POINT(1 2)");
    DatabricksGeometry result = converter.toDatabricksGeometry(textInput);
    assertNotNull(result);
    assertEquals("POINT(1 2)", result.getWKT());
  }

  @Test
  public void testToDatabricksGeometry_WithGeometryInstance_ReturnsItself()
      throws DatabricksSQLException {
    DatabricksGeometry input = new DatabricksGeometry("POINT(1 2)", 4326);
    DatabricksGeometry result = converter.toDatabricksGeometry(input);
    assertSame(input, result);
  }

  @Test
  public void testToDatabricksGeometry_WithUnsupportedType_ThrowsException() {
    assertThrows(DatabricksSQLException.class, () -> converter.toDatabricksGeometry(123));
  }

  // ===================================================================================
  // toDatabricksGeography() tests
  // ===================================================================================

  @Test
  public void testToDatabricksGeography_WithValidWKT() throws DatabricksSQLException {
    DatabricksGeography result = converter.toDatabricksGeography("POINT(-122.4194 37.7749)");
    assertNotNull(result);
    assertEquals("POINT(-122.4194 37.7749)", result.getWKT());
    assertEquals(0, result.getSRID());
  }

  @Test
  public void testToDatabricksGeography_WithEWKT() throws DatabricksSQLException {
    DatabricksGeography result =
        converter.toDatabricksGeography("SRID=4326;POINT(-122.4194 37.7749)");
    assertNotNull(result);
    assertEquals("POINT(-122.4194 37.7749)", result.getWKT());
    assertEquals(4326, result.getSRID());
  }

  @Test
  public void testToDatabricksGeography_WithGeographyInstance_ReturnsItself()
      throws DatabricksSQLException {
    DatabricksGeography input = new DatabricksGeography("POINT(1 2)", 4326);
    DatabricksGeography result = converter.toDatabricksGeography(input);
    assertSame(input, result);
  }

  // ===================================================================================
  // toString() tests
  // ===================================================================================

  @Test
  public void testToString_WithGeometryObject() throws DatabricksSQLException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    String result = converter.toString(geometry);
    assertEquals("SRID=4326;POINT(1 2)", result);
  }

  @Test
  public void testToString_WithNull_ThrowsException() {
    assertThrows(DatabricksSQLException.class, () -> converter.toString(null));
  }

  // ===================================================================================
  // toByteArray() tests
  // ===================================================================================

  @Test
  public void testToByteArray_WithGeometryObject() throws DatabricksSQLException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    byte[] result = converter.toByteArray(geometry);
    assertNotNull(result);
    assertTrue(result.length > 0);
  }

  @Test
  public void testToByteArray_WithNonGeospatialType_ThrowsException() {
    assertThrows(DatabricksSQLException.class, () -> converter.toByteArray("not geospatial"));
  }
}
