package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.GEOGRAPHY;

import com.databricks.jdbc.api.IGeography;
import com.databricks.jdbc.exception.DatabricksValidationException;

public class DatabricksGeography extends AbstractDatabricksGeospatial implements IGeography {

  /**
   * Constructs a DatabricksGeography with the specified WKT and SRID.
   *
   * @param wkt the Well-Known Text representation of the geography
   * @param srid the Spatial Reference System Identifier
   * @throws DatabricksValidationException if the WKT is invalid
   */
  public DatabricksGeography(String wkt, int srid) throws DatabricksValidationException {
    super(wkt, srid);
  }

  /**
   * Creates a geography from Databricks' native Arrow value.
   *
   * <p>The Arrow wire representation stores OGC WKB and SRID separately. The original WKB bytes are
   * preserved so {@link #getWKB()} does not need to re-encode the geography.
   *
   * @param wkb the OGC Well-Known Binary representation
   * @param srid the Spatial Reference System Identifier
   * @return a geography backed by the supplied WKB value
   * @throws DatabricksValidationException if the WKB is null, empty, or malformed
   */
  public static DatabricksGeography fromWKB(byte[] wkb, int srid)
      throws DatabricksValidationException {
    return new DatabricksGeography(wkb, srid);
  }

  private DatabricksGeography(byte[] wkb, int srid) throws DatabricksValidationException {
    super(wkb, srid);
  }

  @Override
  public String getType() {
    return GEOGRAPHY;
  }
}
