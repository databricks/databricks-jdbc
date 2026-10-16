package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.GEOMETRY;

import com.databricks.jdbc.api.IGeometry;
import com.databricks.jdbc.exception.DatabricksValidationException;

public class DatabricksGeometry extends AbstractDatabricksGeospatial implements IGeometry {

  /**
   * Constructs a DatabricksGeometry with the specified WKT and SRID.
   *
   * @param wkt the Well-Known Text representation of the geometry
   * @param srid the Spatial Reference System Identifier
   * @throws DatabricksValidationException if the WKT is invalid
   */
  public DatabricksGeometry(String wkt, int srid) throws DatabricksValidationException {
    super(wkt, srid);
  }

  /**
   * Creates a geometry from Databricks' native Arrow value.
   *
   * <p>The Arrow wire representation stores OGC WKB and SRID separately. The original WKB bytes are
   * preserved so {@link #getWKB()} does not need to re-encode the geometry.
   *
   * @param wkb the OGC Well-Known Binary representation
   * @param srid the Spatial Reference System Identifier
   * @return a geometry backed by the supplied WKB value
   * @throws DatabricksValidationException if the WKB is null, empty, or malformed
   */
  public static DatabricksGeometry fromWKB(byte[] wkb, int srid)
      throws DatabricksValidationException {
    return new DatabricksGeometry(wkb, srid);
  }

  private DatabricksGeometry(byte[] wkb, int srid) throws DatabricksValidationException {
    super(wkb, srid);
  }

  @Override
  public String getType() {
    return GEOMETRY;
  }
}
