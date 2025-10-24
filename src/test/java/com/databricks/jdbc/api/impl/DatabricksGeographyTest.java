package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksValidationException;
import org.junit.jupiter.api.Test;

/** Test class for DatabricksGeography. Reference: DatabricksGeometryTest.java */
public class DatabricksGeographyTest {

  // ===================================================================================
  // Constructor tests
  // ===================================================================================

  @Test
  public void testConstructor_WithValidPoint() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 0);
    assertNotNull(geography);
    assertEquals("POINT(-122.4194 37.7749)", geography.getWKT());
    assertEquals(0, geography.getSRID());
  }

  @Test
  public void testConstructor_WithValidPointAndSRID() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    assertNotNull(geography);
    assertEquals("POINT(-122.4194 37.7749)", geography.getWKT());
    assertEquals(4326, geography.getSRID());
  }

  @Test
  public void testConstructor_WithLineString() throws DatabricksValidationException {
    DatabricksGeography geography =
        new DatabricksGeography("LINESTRING(-122.4 37.7, -122.5 37.8, -122.6 37.9)", 4326);
    assertNotNull(geography);
    assertTrue(geography.getWKT().startsWith("LINESTRING"));
  }

  @Test
  public void testConstructor_WithPolygon() throws DatabricksValidationException {
    DatabricksGeography geography =
        new DatabricksGeography(
            "POLYGON((-122.4 37.7, -122.5 37.7, -122.5 37.8, -122.4 37.8, -122.4 37.7))", 4326);
    assertNotNull(geography);
    assertTrue(geography.getWKT().startsWith("POLYGON"));
  }

  @Test
  public void testConstructor_WithNullWKT_ThrowsException() {
    assertThrows(DatabricksValidationException.class, () -> new DatabricksGeography(null, 0));
  }

  @Test
  public void testConstructor_WithEmptyWKT_ThrowsException() {
    assertThrows(DatabricksValidationException.class, () -> new DatabricksGeography("", 0));
  }

  @Test
  public void testConstructor_WithWhitespaceWKT_ThrowsException() {
    assertThrows(DatabricksValidationException.class, () -> new DatabricksGeography("   ", 0));
  }

  // ===================================================================================
  // Accessor tests
  // ===================================================================================

  @Test
  public void testGetWkt() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    assertEquals("POINT(-122.4194 37.7749)", geography.getWKT());
  }

  @Test
  public void testGetSrid_WithZero() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 0);
    assertEquals(0, geography.getSRID());
  }

  @Test
  public void testGetSrid_With3857() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 3857);
    assertEquals(3857, geography.getSRID());
  }

  @Test
  public void testGetWkb_ReturnsValidBytes() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    byte[] wkb = geography.getWKB();
    assertNotNull(wkb);
    assertTrue(wkb.length > 0);
  }

  // ===================================================================================
  // toString tests
  // ===================================================================================

  @Test
  public void testToString_WithZeroSRID() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 0);
    assertEquals("SRID=0;POINT(-122.4194 37.7749)", geography.toString());
  }

  @Test
  public void testToString_With4326() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    assertEquals("SRID=4326;POINT(-122.4194 37.7749)", geography.toString());
  }

  // ===================================================================================
  // equals and hashCode tests
  // ===================================================================================

  @Test
  public void testEquals_SameWKTAndSRID() throws DatabricksValidationException {
    DatabricksGeography geo1 = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    DatabricksGeography geo2 = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    assertEquals(geo1, geo2);
  }

  @Test
  public void testEquals_DifferentWKT() throws DatabricksValidationException {
    DatabricksGeography geo1 = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    DatabricksGeography geo2 = new DatabricksGeography("POINT(-118.2437 34.0522)", 4326);
    assertNotEquals(geo1, geo2);
  }

  @Test
  public void testEquals_DifferentSRID() throws DatabricksValidationException {
    DatabricksGeography geo1 = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    DatabricksGeography geo2 = new DatabricksGeography("POINT(-122.4194 37.7749)", 3857);
    assertNotEquals(geo1, geo2);
  }

  @Test
  public void testEquals_SameInstance() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    assertEquals(geography, geography);
  }

  @Test
  public void testEquals_WithNull() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    assertNotEquals(geography, null);
  }

  @Test
  public void testEquals_WithDifferentClass() throws DatabricksValidationException {
    DatabricksGeography geography = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    assertNotEquals(geography, "POINT(-122.4194 37.7749)");
  }

  @Test
  public void testHashCode_EqualObjects() throws DatabricksValidationException {
    DatabricksGeography geo1 = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    DatabricksGeography geo2 = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    assertEquals(geo1.hashCode(), geo2.hashCode());
  }

  @Test
  public void testHashCode_DifferentObjects() throws DatabricksValidationException {
    DatabricksGeography geo1 = new DatabricksGeography("POINT(-122.4194 37.7749)", 4326);
    DatabricksGeography geo2 = new DatabricksGeography("POINT(-118.2437 34.0522)", 4326);
    assertNotEquals(geo1.hashCode(), geo2.hashCode());
  }
}
