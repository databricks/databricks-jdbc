package com.databricks.jdbc.api.impl.converters;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.GEOGRAPHY;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.GEOMETRY;

import com.databricks.jdbc.api.IDatabricksGeospatial;
import com.databricks.jdbc.api.impl.DatabricksGeography;
import com.databricks.jdbc.api.impl.DatabricksGeometry;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.Map;
import org.apache.arrow.vector.util.Text;

public class GeospatialConverter implements ObjectConverter {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(GeospatialConverter.class);

  @Override
  public DatabricksGeometry toDatabricksGeometry(Object object) throws DatabricksSQLException {
    if (object instanceof DatabricksGeometry) {
      return (DatabricksGeometry) object;
    }
    return convertToGeospatial(
        object, GEOMETRY, DatabricksGeometry::new, DatabricksGeometry::fromWKB);
  }

  @Override
  public DatabricksGeography toDatabricksGeography(Object object) throws DatabricksSQLException {
    if (object instanceof DatabricksGeography) {
      return (DatabricksGeography) object;
    }
    return convertToGeospatial(
        object, GEOGRAPHY, DatabricksGeography::new, DatabricksGeography::fromWKB);
  }

  private <T extends IDatabricksGeospatial> T convertToGeospatial(
      Object object,
      String typeName,
      TextGeospatialFactory<T> textFactory,
      BinaryGeospatialFactory<T> binaryFactory)
      throws DatabricksSQLException {
    if (object instanceof String || object instanceof Text) {
      String ewktString = object.toString();
      try {
        int srid = WKTConverter.extractSRIDFromEWKT(ewktString);
        String cleanWKT = WKTConverter.removeSRIDFromEWKT(ewktString);
        return textFactory.create(cleanWKT, srid);
      } catch (Exception e) {
        String errorMessage =
            String.format("Failed to convert EWKT to %s: %s", typeName, ewktString);
        LOGGER.warn(errorMessage, e);
        throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
      }
    }

    if (object instanceof Map<?, ?>) {
      Map<?, ?> nativeValue = (Map<?, ?>) object;
      Object srid = nativeValue.get("srid");
      Object wkb = nativeValue.get("wkb");
      if (!(srid instanceof Integer) || !(wkb instanceof byte[])) {
        throw malformedNativeValue(typeName);
      }
      try {
        return binaryFactory.create((byte[]) wkb, (Integer) srid);
      } catch (Exception e) {
        String errorMessage = String.format("Failed to convert native Arrow value to %s", typeName);
        LOGGER.warn(errorMessage, e);
        throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
      }
    }

    throw new DatabricksSQLException(
        String.format(
            "Unsupported %s conversion from type: %s",
            typeName.substring(0, 1).toUpperCase() + typeName.substring(1), object.getClass()),
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  private DatabricksSQLException malformedNativeValue(String typeName) {
    return new DatabricksSQLException(
        String.format(
            "Malformed native Arrow %s value: expected struct<srid:int32,wkb:binary>", typeName),
        DatabricksDriverErrorCode.INVALID_STATE);
  }

  @FunctionalInterface
  private interface TextGeospatialFactory<T extends IDatabricksGeospatial> {
    T create(String wkt, int srid) throws Exception;
  }

  @FunctionalInterface
  private interface BinaryGeospatialFactory<T extends IDatabricksGeospatial> {
    T create(byte[] wkb, int srid) throws Exception;
  }

  @Override
  public String toString(Object object) throws DatabricksSQLException {
    if (object != null) {
      return object.toString();
    }
    throw new DatabricksSQLException(
        "Cannot convert null to String", DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }

  @Override
  public byte[] toByteArray(Object object) throws DatabricksSQLException {
    if (object instanceof IDatabricksGeospatial) {
      return ((IDatabricksGeospatial) object).getWKB();
    }
    throw new DatabricksSQLException(
        "Unsupported byte array conversion operation for geospatial types",
        DatabricksDriverErrorCode.UNSUPPORTED_OPERATION);
  }
}
