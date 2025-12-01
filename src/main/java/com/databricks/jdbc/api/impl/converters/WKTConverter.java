package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.nio.ByteOrder;
import java.util.EnumSet;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.Ordinate;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

/**
 * Utility class for converting between WKT (Well-Known Text) and WKB (Well-Known Binary) formats.
 *
 * <p>This class uses the JTS (Java Topology Suite) library to provide robust WKT/WKB conversion
 * functionality for geospatial data. JTS is a widely-used, well-tested library that implements the
 * OpenGIS Consortium's Simple Features Specification for SQL.
 */
public class WKTConverter {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(WKTConverter.class);

  /**
   * Converts WKT (Well-Known Text) to WKB (Well-Known Binary) format.
   *
   * <p>This implementation uses the JTS library to parse the WKT string into a Geometry object and
   * then converts it to WKB format using the custom OGC-compliant WKB writer.
   *
   * @param wkt the WKT string to convert
   * @return the WKB representation as a byte array
   * @throws DatabricksValidationException if the WKT is invalid
   */
  public static byte[] toWKB(String wkt) throws DatabricksValidationException {
    if (wkt == null || wkt.trim().isEmpty()) {
      throw new DatabricksValidationException("WKT string cannot be null or empty");
    }

    try {
      int ogcDimension = ogcDimensionFromWkt(wkt);
      EnumSet<Ordinate> ordinates = determineOrdinates(ogcDimension);
      WKTReader reader = new WKTReader();
      Geometry geometry = reader.read(wkt);
      JTSOGCWKBWriter writer = new JTSOGCWKBWriter(ordinates, ByteOrder.LITTLE_ENDIAN);
      return writer.write(geometry);
    } catch (ParseException e) {
      String errorMessage =
          String.format("Failed to parse WKT: %s. Error: %s", wkt, e.getMessage());
      LOGGER.error(errorMessage, e);
      throw new DatabricksValidationException(errorMessage, e);
    }
  }

  // USED ONLY IN TEST CASES (WITH NON-EMPTY GEOMETRIES) - DO NOT USE IN NORMAL FLOW
  // WKB READER HAS LIMITATIONS WITH EMPTY GEOMETRIES
  public static String toWKT(byte[] wkb) throws DatabricksValidationException {
    if (wkb == null || wkb.length == 0) {
      throw new DatabricksValidationException("WKB bytes cannot be null or empty");
    }

    try {
      int ogcDimension = ogcDimensionFromWkb(wkb);
      EnumSet<Ordinate> ordinates = determineOrdinates(ogcDimension);
      WKBReader reader = new WKBReader();
      Geometry geometry = reader.read(wkb);
      int outputDimension = ordinates.size();
      WKTWriter writer = new WKTWriter(outputDimension);
      writer.setOutputOrdinates(ordinates);
      return writer.write(geometry);
    } catch (Exception e) {
      String errorMessage =
          String.format("Failed to parse WKB: %d bytes. Error: %s", wkb.length, e.getMessage());
      LOGGER.error(errorMessage, e);
      throw new DatabricksValidationException(errorMessage, e);
    }
  }

  /**
   * Extracts the OGC dimension from a WKT string.
   *
   * <p>OGC dimension values: 0 = XY (2D) 1000 = XYZ (3D with Z) 2000 = XYM (3D with M) 3000 = XYZM
   * (4D)
   *
   * @param wkt the WKT string to parse
   * @return the OGC dimension value (0, 1000, 2000, or 3000)
   * @throws DatabricksValidationException if the WKT format is invalid
   */
  private static int ogcDimensionFromWkt(String wkt) throws DatabricksValidationException {
    for (int i = 0; i < wkt.length() - 2; ++i) {
      char c = wkt.charAt(i);
      if (c == '(') return 0;
      if (c == ' ') {
        char next = Character.toUpperCase(wkt.charAt(i + 1));
        if (next == 'E') return 0; // EMPTY
        if (next == 'M') return 2000; // M dimension
        if (next == 'Z') {
          // Check if it's Z or ZM
          return (i + 2 < wkt.length() && Character.toUpperCase(wkt.charAt(i + 2)) == 'M')
              ? 3000
              : 1000;
        }
      }
    }
    throw new DatabricksValidationException("Invalid WKT input: " + wkt);
  }

  /**
   * Extracts the OGC dimension from WKB bytes.
   *
   * <p>OGC dimension values: 0 = XY (2D) 1000 = XYZ (3D with Z) 2000 = XYM (3D with M) 3000 = XYZM
   * (4D)
   *
   * @param wkb the WKB bytes to parse
   * @return the OGC dimension value (0, 1000, 2000, or 3000)
   * @throws DatabricksValidationException if the WKB format is invalid
   */
  private static int ogcDimensionFromWkb(byte[] wkb) throws DatabricksValidationException {
    if (wkb.length < 5) {
      throw new DatabricksValidationException("Invalid WKB input: insufficient bytes");
    }
    ByteOrder endianness = (wkb[0] == 0) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    int type = java.nio.ByteBuffer.wrap(wkb).order(endianness).getInt(/* pos= */ 1);
    return type - type % 1000;
  }

  /**
   * Determines the ordinate set based on the OGC dimension.
   *
   * @param ogcDimension the OGC dimension value (0, 1000, 2000, or 3000)
   * @return the appropriate EnumSet of Ordinates
   */
  private static EnumSet<Ordinate> determineOrdinates(int ogcDimension) {
    switch (ogcDimension) {
      case 1000:
        return Ordinate.createXYZ();
      case 2000:
        return Ordinate.createXYM();
      case 3000:
        return Ordinate.createXYZM();
      default:
        return Ordinate.createXY();
    }
  }

  /**
   * Extracts the SRID from an EWKT (Extended Well-Known Text) string.
   *
   * <p>EWKT format includes SRID prefix: "SRID=4326;POINT(1 2)"
   *
   * @param ewkt the EWKT string
   * @return the SRID value, or 0 if no SRID is specified
   */
  public static int extractSRIDFromEWKT(String ewkt) {
    if (ewkt == null || ewkt.trim().isEmpty()) {
      return 0;
    }

    String trimmed = ewkt.trim();
    if (trimmed.startsWith("SRID=")) {
      int semicolonIndex = trimmed.indexOf(';');
      if (semicolonIndex > 0) {
        try {
          String sridStr = trimmed.substring(5, semicolonIndex);
          return Integer.parseInt(sridStr);
        } catch (NumberFormatException e) {
          LOGGER.warn("Invalid SRID format in EWKT: {}", ewkt);
          return 0;
        }
      }
    }

    return 0; // Default SRID if not specified
  }

  /**
   * Removes the SRID prefix from an EWKT string to get clean WKT.
   *
   * <p>Converts "SRID=4326;POINT(1 2)" to "POINT(1 2)"
   *
   * @param ewkt the EWKT string
   * @return the clean WKT string without SRID prefix
   */
  public static String removeSRIDFromEWKT(String ewkt) {
    if (ewkt == null || ewkt.trim().isEmpty()) {
      return ewkt;
    }

    String trimmed = ewkt.trim();
    if (trimmed.startsWith("SRID=")) {
      int semicolonIndex = trimmed.indexOf(';');
      if (semicolonIndex > 0) {
        return trimmed.substring(semicolonIndex + 1);
      }
    }

    return trimmed; // Return as-is if no SRID prefix
  }
}
