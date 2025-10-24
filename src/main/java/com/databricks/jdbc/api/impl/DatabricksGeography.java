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

  @Override
  public String getType() {
    return GEOGRAPHY;
  }
}
