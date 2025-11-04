package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.IDatabricksGeospatial;
import com.databricks.jdbc.api.impl.converters.WKTConverter;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.util.Objects;

/**
 * Abstract base class for geospatial data types in Databricks JDBC driver.
 *
 * <p>This class provides common functionality for both GEOMETRY and GEOGRAPHY types, including
 * storage of WKT (Well-Known Text) format data and access to both WKT and WKB representations.
 */
public abstract class AbstractDatabricksGeospatial implements IDatabricksGeospatial {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(AbstractDatabricksGeospatial.class);

  private final String wkt;
  private final int srid; // Spatial Reference System Identifier
  private final byte[] wkb;

  /**
   * Constructs an AbstractDatabricksGeospatial with the specified WKT and SRID.
   *
   * @param wkt the Well-Known Text representation of the geospatial object
   * @param srid the Spatial Reference System Identifier
   * @throws DatabricksValidationException if the WKT is invalid
   */
  protected AbstractDatabricksGeospatial(String wkt, int srid)
      throws DatabricksValidationException {
    if (wkt == null || wkt.trim().isEmpty()) {
      LOGGER.error("WKT string cannot be null or empty");
      throw new DatabricksValidationException("WKT string cannot be null or empty");
    }

    this.wkt = wkt.trim();
    this.srid = srid;
    this.wkb = WKTConverter.toWKB(this.wkt);
  }

  /**
   * Returns the Well-Known Binary (WKB) representation of the geospatial object.
   *
   * @return the WKB representation as a byte array
   */
  @Override
  public byte[] getWKB() {
    return wkb;
  }

  /**
   * Returns the Spatial Reference System Identifier (SRID) of the geospatial object.
   *
   * @return the SRID value
   */
  @Override
  public int getSRID() {
    return srid;
  }

  /**
   * Returns the Well-Known Text (WKT) representation of the geospatial object.
   *
   * @return the WKT string
   */
  @Override
  public String getWKT() {
    return wkt;
  }

  /**
   * Returns a string representation of the geospatial object in EWKT format.
   *
   * @return the EWKT string representation
   */
  @Override
  public String toString() {
    return String.format("SRID=%d;%s", srid, wkt);
  }

  /**
   * Checks if this geospatial object is equal to another object.
   *
   * @param obj the object to compare
   * @return true if the objects are equal, false otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    AbstractDatabricksGeospatial that = (AbstractDatabricksGeospatial) obj;
    return srid == that.srid && wkt.equals(that.wkt);
  }

  /**
   * Returns the hash code for this geospatial object.
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return Objects.hash(wkt, srid);
  }

  /**
   * Returns the data type of the geospatial object.
   *
   * @return the type as a string, either "GEOMETRY" or "GEOGRAPHY"
   */
  @Override
  public abstract String getType();
}
