package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.IGeography;
import com.databricks.jdbc.api.IGeometry;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import java.sql.*;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * E2E integration tests for geospatial POINT types across all configuration combinations: Protocol
 * (Thrift/SEA), Serialization (Arrow/Inline), CloudFetch, GeoSpatial support, and Complex type
 * support.
 *
 * <p>Geospatial objects (IGeometry/IGeography) are returned only when both EnableGeoSpatialSupport
 * and EnableComplexDatatypeSupport are enabled AND not in Thrift+Inline mode. Otherwise, returns as
 * STRING.
 */
public class GeospatialTests {

  private Connection connection;

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  private void setupConnection(
      int useThrift, int enableArrow, int enableGeoSupport, int enableComplexSupport)
      throws SQLException {
    connection =
        getValidJDBCConnection(
            List.of(
                List.of("UseThriftClient", String.valueOf(useThrift)),
                List.of("EnableArrow", String.valueOf(enableArrow)),
                List.of("EnableGeoSpatialSupport", String.valueOf(enableGeoSupport)),
                List.of("EnableComplexDatatypeSupport", String.valueOf(enableComplexSupport))));
  }

  private static Stream<Arguments> provideAllConfigurations() {
    // Note: CloudFetch requires Arrow, so CloudFetch=1 configurations only appear with Arrow=1
    return Stream.of(
        // ===================================================================
        // SEA (useThrift=0), Inline (arrow=0), No CloudFetch
        // ===================================================================
        Arguments.of(0, 0, 0, 0, 0, "SEA|Inline|NoCloudFetch|GeoOff|ComplexOff"),
        Arguments.of(0, 0, 0, 0, 1, "SEA|Inline|NoCloudFetch|GeoOff|ComplexOn"),
        Arguments.of(0, 0, 0, 1, 0, "SEA|Inline|NoCloudFetch|GeoOn|ComplexOff"),
        Arguments.of(0, 0, 0, 1, 1, "SEA|Inline|NoCloudFetch|GeoOn|ComplexOn"),

        // ===================================================================
        // SEA (useThrift=0), Arrow (arrow=1), No CloudFetch
        // ===================================================================
        Arguments.of(0, 1, 0, 0, 0, "SEA|Arrow|NoCloudFetch|GeoOff|ComplexOff"),
        Arguments.of(0, 1, 0, 0, 1, "SEA|Arrow|NoCloudFetch|GeoOff|ComplexOn"),
        Arguments.of(0, 1, 0, 1, 0, "SEA|Arrow|NoCloudFetch|GeoOn|ComplexOff"),
        Arguments.of(0, 1, 0, 1, 1, "SEA|Arrow|NoCloudFetch|GeoOn|ComplexOn"),

        // ===================================================================
        // SEA (useThrift=0), Arrow (arrow=1), CloudFetch enabled
        // ===================================================================
        Arguments.of(0, 1, 1, 0, 0, "SEA|Arrow|CloudFetch|GeoOff|ComplexOff"),
        Arguments.of(0, 1, 1, 0, 1, "SEA|Arrow|CloudFetch|GeoOff|ComplexOn"),
        Arguments.of(0, 1, 1, 1, 0, "SEA|Arrow|CloudFetch|GeoOn|ComplexOff"),
        Arguments.of(0, 1, 1, 1, 1, "SEA|Arrow|CloudFetch|GeoOn|ComplexOn"),

        // ===================================================================
        // Thrift (useThrift=1), Inline (arrow=0), No CloudFetch
        // ===================================================================
        Arguments.of(1, 0, 0, 0, 0, "Thrift|Inline|NoCloudFetch|GeoOff|ComplexOff"),
        Arguments.of(1, 0, 0, 0, 1, "Thrift|Inline|NoCloudFetch|GeoOff|ComplexOn"),
        Arguments.of(1, 0, 0, 1, 0, "Thrift|Inline|NoCloudFetch|GeoOn|ComplexOff"),
        Arguments.of(1, 0, 0, 1, 1, "Thrift|Inline|NoCloudFetch|GeoOn|ComplexOn"),

        // ===================================================================
        // Thrift (useThrift=1), Arrow (arrow=1), No CloudFetch
        // ===================================================================
        Arguments.of(1, 1, 0, 0, 0, "Thrift|Arrow|NoCloudFetch|GeoOff|ComplexOff"),
        Arguments.of(1, 1, 0, 0, 1, "Thrift|Arrow|NoCloudFetch|GeoOff|ComplexOn"),
        Arguments.of(1, 1, 0, 1, 0, "Thrift|Arrow|NoCloudFetch|GeoOn|ComplexOff"),
        Arguments.of(1, 1, 0, 1, 1, "Thrift|Arrow|NoCloudFetch|GeoOn|ComplexOn"),

        // ===================================================================
        // Thrift (useThrift=1), Arrow (arrow=1), CloudFetch enabled
        // ===================================================================
        Arguments.of(1, 1, 1, 0, 0, "Thrift|Arrow|CloudFetch|GeoOff|ComplexOff"),
        Arguments.of(1, 1, 1, 0, 1, "Thrift|Arrow|CloudFetch|GeoOff|ComplexOn"),
        Arguments.of(1, 1, 1, 1, 0, "Thrift|Arrow|CloudFetch|GeoOn|ComplexOff"),
        Arguments.of(1, 1, 1, 1, 1, "Thrift|Arrow|CloudFetch|GeoOn|ComplexOn"));
  }

  @ParameterizedTest(name = "{5}")
  @MethodSource("provideAllConfigurations")
  void testGeospatialPoint(
      int useThrift,
      int enableArrow,
      int cloudFetch,
      int enableGeoSupport,
      int enableComplexSupport,
      String desc)
      throws SQLException {

    setupConnection(useThrift, enableArrow, enableGeoSupport, enableComplexSupport);

    // Build SQL query - conditionally add sequence for CloudFetch
    String sql =
        "SELECT "
            + "ST_POINT(1, 2, 4326) as geom_point, "
            + "ST_GeogFromText('POINT(-122.4194 37.7749)') as geog_point";

    if (cloudFetch == 1) {
      sql += " FROM explode(sequence(1, 1000000)) AS seq";
    }

    ResultSet rs = executeQuery(connection, sql);
    assertNotNull(rs, "ResultSet should not be null for config: " + desc);

    ResultSetMetaData rsm = rs.getMetaData();
    assertEquals(2, rsm.getColumnCount(), "Should have 2 columns: " + desc);

    // Assert CloudFetch usage when configured
    if (cloudFetch == 1) {
      assertTrue(
          ((DatabricksResultSetMetaData) rsm).getIsCloudFetchUsed(),
          "CloudFetch should be used with 1M rows: " + desc);
    }

    // Validate ONLY first row
    assertTrue(rs.next(), "Should have at least one row for config: " + desc);

    // Geospatial objects returned only when:
    // 1. Both EnableGeoSpatialSupport=1 AND EnableComplexDatatypeSupport=1
    // 2. NOT in Thrift + Inline mode (Thrift doesn't support geospatial without Arrow)
    boolean shouldReturnGeospatialObjects =
        enableGeoSupport == 1 && enableComplexSupport == 1 && !(useThrift == 1 && enableArrow == 0);

    if (shouldReturnGeospatialObjects) {
      validateGeospatialEnabled(rs, rsm);
    } else {
      validateGeospatialDisabled(rs, rsm);
    }

    rs.close();
  }

  /** Validates geospatial objects are returned (IGeometry/IGeography with correct metadata). */
  private void validateGeospatialEnabled(ResultSet rs, ResultSetMetaData rsm) throws SQLException {

    // Column 1: GEOMETRY POINT
    assertEquals("geom_point", rsm.getColumnName(1));
    assertEquals(Types.OTHER, rsm.getColumnType(1));
    assertEquals("GEOMETRY(4326)", rsm.getColumnTypeName(1));
    assertEquals("com.databricks.jdbc.api.IGeometry", rsm.getColumnClassName(1));

    Object geomObj = rs.getObject("geom_point");
    assertNotNull(geomObj);
    assertInstanceOf(IGeometry.class, geomObj);

    IGeometry geom = (IGeometry) geomObj;
    assertEquals(4326, geom.getSRID());
    assertEquals("POINT(1 2)", geom.getWKT());

    // Column 2: GEOGRAPHY POINT
    assertEquals("geog_point", rsm.getColumnName(2));
    assertEquals(Types.OTHER, rsm.getColumnType(2));
    assertEquals("GEOGRAPHY(4326)", rsm.getColumnTypeName(2));
    assertEquals("com.databricks.jdbc.api.IGeography", rsm.getColumnClassName(2));

    Object geogObj = rs.getObject("geog_point");
    assertNotNull(geogObj);
    assertInstanceOf(IGeography.class, geogObj);

    IGeography geog = (IGeography) geogObj;
    assertEquals(4326, geog.getSRID());
    assertEquals("POINT(-122.4194 37.7749)", geog.getWKT());
  }

  /** Validates geospatial data is returned as STRING. */
  private void validateGeospatialDisabled(ResultSet rs, ResultSetMetaData rsm) throws SQLException {

    // Column 1: GEOMETRY POINT (as STRING)
    assertEquals("geom_point", rsm.getColumnName(1));
    assertEquals(Types.VARCHAR, rsm.getColumnType(1));
    assertEquals("STRING", rsm.getColumnTypeName(1));
    assertEquals("java.lang.String", rsm.getColumnClassName(1));

    Object geomObj = rs.getObject("geom_point");
    assertNotNull(geomObj);
    assertInstanceOf(String.class, geomObj);

    String geomStr = (String) geomObj;
    assertEquals("SRID=4326;POINT(1 2)", geomStr);

    // Column 2: GEOGRAPHY POINT (as STRING)
    assertEquals("geog_point", rsm.getColumnName(2));
    assertEquals(Types.VARCHAR, rsm.getColumnType(2));
    assertEquals("STRING", rsm.getColumnTypeName(2));
    assertEquals("java.lang.String", rsm.getColumnClassName(2));

    Object geogObj = rs.getObject("geog_point");
    assertNotNull(geogObj);
    assertInstanceOf(String.class, geogObj);

    String geogStr = (String) geogObj;
    assertEquals("SRID=4326;POINT(-122.4194 37.7749)", geogStr);
  }
}
