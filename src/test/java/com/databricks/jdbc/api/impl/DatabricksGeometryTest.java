package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksValidationException;
import org.junit.jupiter.api.Test;

/** Test class for DatabricksGeometry. Reference: DatabricksArrayTest.java */
public class DatabricksGeometryTest {

  // ===================================================================================
  // Constructor tests
  // ===================================================================================

  @Test
  public void testConstructor_WithValidPoint() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 0);
    assertNotNull(geometry);
    assertEquals("POINT(1 2)", geometry.getWKT());
    assertEquals(0, geometry.getSRID());
  }

  @Test
  public void testConstructor_WithValidPointAndSRID() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    assertNotNull(geometry);
    assertEquals("POINT(1 2)", geometry.getWKT());
    assertEquals(4326, geometry.getSRID());
  }

  @Test
  public void testConstructor_WithLineString() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("LINESTRING(0 0, 10 10, 20 20)", 0);
    assertNotNull(geometry);
    assertTrue(geometry.getWKT().startsWith("LINESTRING"));
  }

  @Test
  public void testConstructor_WithPolygon() throws DatabricksValidationException {
    DatabricksGeometry geometry =
        new DatabricksGeometry("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", 0);
    assertNotNull(geometry);
    assertTrue(geometry.getWKT().startsWith("POLYGON"));
  }

  @Test
  public void testConstructor_WithNullWKT_ThrowsException() {
    assertThrows(DatabricksValidationException.class, () -> new DatabricksGeometry(null, 0));
  }

  @Test
  public void testConstructor_WithEmptyWKT_ThrowsException() {
    assertThrows(DatabricksValidationException.class, () -> new DatabricksGeometry("", 0));
  }

  @Test
  public void testConstructor_WithWhitespaceWKT_ThrowsException() {
    assertThrows(DatabricksValidationException.class, () -> new DatabricksGeometry("   ", 0));
  }

  // ===================================================================================
  // Accessor tests
  // ===================================================================================

  @Test
  public void testGetWkt() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(5 10)", 4326);
    assertEquals("POINT(5 10)", geometry.getWKT());
  }

  @Test
  public void testGetSrid_WithZero() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 0);
    assertEquals(0, geometry.getSRID());
  }

  @Test
  public void testGetSrid_With4326() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    assertEquals(4326, geometry.getSRID());
  }

  @Test
  public void testGetSrid_With3857() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 3857);
    assertEquals(3857, geometry.getSRID());
  }

  @Test
  public void testGetWkb_ReturnsValidBytes() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    byte[] wkb = geometry.getWKB();
    assertNotNull(wkb);
    assertTrue(wkb.length > 0);
  }

  // ===================================================================================
  // toString tests
  // ===================================================================================

  @Test
  public void testToString_WithZeroSRID() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 0);
    assertEquals("SRID=0;POINT(1 2)", geometry.toString());
  }

  @Test
  public void testToString_With4326() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    assertEquals("SRID=4326;POINT(1 2)", geometry.toString());
  }

  // ===================================================================================
  // equals and hashCode tests
  // ===================================================================================

  @Test
  public void testEquals_SameWKTAndSRID() throws DatabricksValidationException {
    DatabricksGeometry geo1 = new DatabricksGeometry("POINT(1 2)", 4326);
    DatabricksGeometry geo2 = new DatabricksGeometry("POINT(1 2)", 4326);
    assertEquals(geo1, geo2);
  }

  @Test
  public void testEquals_DifferentWKT() throws DatabricksValidationException {
    DatabricksGeometry geo1 = new DatabricksGeometry("POINT(1 2)", 4326);
    DatabricksGeometry geo2 = new DatabricksGeometry("POINT(3 4)", 4326);
    assertNotEquals(geo1, geo2);
  }

  @Test
  public void testEquals_DifferentSRID() throws DatabricksValidationException {
    DatabricksGeometry geo1 = new DatabricksGeometry("POINT(1 2)", 4326);
    DatabricksGeometry geo2 = new DatabricksGeometry("POINT(1 2)", 3857);
    assertNotEquals(geo1, geo2);
  }

  @Test
  public void testEquals_SameInstance() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    assertEquals(geometry, geometry);
  }

  @Test
  public void testEquals_WithNull() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    assertNotEquals(geometry, null);
  }

  @Test
  public void testEquals_WithDifferentClass() throws DatabricksValidationException {
    DatabricksGeometry geometry = new DatabricksGeometry("POINT(1 2)", 4326);
    assertNotEquals(geometry, "POINT(1 2)");
  }

  @Test
  public void testHashCode_EqualObjects() throws DatabricksValidationException {
    DatabricksGeometry geo1 = new DatabricksGeometry("POINT(1 2)", 4326);
    DatabricksGeometry geo2 = new DatabricksGeometry("POINT(1 2)", 4326);
    assertEquals(geo1.hashCode(), geo2.hashCode());
  }

  @Test
  public void testHashCode_DifferentObjects() throws DatabricksValidationException {
    DatabricksGeometry geo1 = new DatabricksGeometry("POINT(1 2)", 4326);
    DatabricksGeometry geo2 = new DatabricksGeometry("POINT(3 4)", 4326);
    assertNotEquals(geo1.hashCode(), geo2.hashCode());
  }
}
