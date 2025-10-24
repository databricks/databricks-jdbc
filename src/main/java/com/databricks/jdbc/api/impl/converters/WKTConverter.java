package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
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

  private static final ThreadLocal<WKTReader> WKT_READER = ThreadLocal.withInitial(WKTReader::new);
  private static final ThreadLocal<WKTWriter> WKT_WRITER = ThreadLocal.withInitial(WKTWriter::new);
  private static final ThreadLocal<WKBReader> WKB_READER = ThreadLocal.withInitial(WKBReader::new);
  private static final ThreadLocal<WKBWriter> WKB_WRITER = ThreadLocal.withInitial(WKBWriter::new);

  /**
   * Converts WKT (Well-Known Text) to WKB (Well-Known Binary) format.
   *
   * <p>This implementation uses the JTS library to parse the WKT string into a Geometry object and
   * then converts it to WKB format. This provides robust, standards-compliant conversion.
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
      Geometry geometry = WKT_READER.get().read(wkt);
      return WKB_WRITER.get().write(geometry);
    } catch (ParseException e) {
      String errorMessage = String.format("Invalid WKT format: %s", wkt);
      LOGGER.error(errorMessage, e);
      throw new DatabricksValidationException(errorMessage, e);
    }
  }

  /**
   * Converts WKB (Well-Known Binary) to WKT (Well-Known Text) format.
   *
   * <p>This implementation uses the JTS library to parse the WKB bytes into a Geometry object and
   * then converts it to WKT format.
   *
   * @param wkb the WKB bytes to convert
   * @return the WKT representation as a string
   * @throws DatabricksValidationException if the WKB is invalid
   */
  public static String toWKT(byte[] wkb) throws DatabricksValidationException {
    if (wkb == null || wkb.length == 0) {
      throw new DatabricksValidationException("WKB bytes cannot be null or empty");
    }

    try {
      Geometry geometry = WKB_READER.get().read(wkb);
      return WKT_WRITER.get().write(geometry);
    } catch (Exception e) {
      String errorMessage = String.format("Invalid WKB format: %d bytes", wkb.length);
      LOGGER.error(errorMessage, e);
      throw new DatabricksValidationException(errorMessage, e);
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
