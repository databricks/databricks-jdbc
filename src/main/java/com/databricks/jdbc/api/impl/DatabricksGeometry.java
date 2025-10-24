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

  @Override
  public String getType() {
    return GEOMETRY;
  }
}
