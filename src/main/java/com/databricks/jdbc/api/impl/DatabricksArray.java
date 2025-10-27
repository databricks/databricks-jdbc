package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/** Class for representation of Array complex object. */
public class DatabricksArray implements Array {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksArray.class);

  private final Object[] elements;
  private final String typeName;

  /**
   * Constructs a DatabricksArray with the specified elements and metadata.
   *
   * @param elements the elements of the array as a list
   * @param metadata the metadata describing the type of array elements
   */
  public DatabricksArray(List<Object> elements, String metadata) {
    LOGGER.debug("Initializing DatabricksArray with metadata: {}", metadata);
    String elementType = MetadataParser.parseArrayMetadata(metadata);
    this.elements = convertElements(elements, elementType);
    this.typeName = metadata;
  }

  /**
   * Converts the elements based on specified element type.
   *
   * @param elements the original elements to be converted
   * @param elementType the type of each element
   * @return an array of converted elements
   */
  private Object[] convertElements(List<Object> elements, String elementType) {
    LOGGER.debug("Converting elements with element type: {}", elementType);
    Object[] convertedElements = new Object[elements.size()];

    for (int i = 0; i < elements.size(); i++) {
      Object element = elements.get(i);
      try {
        if (elementType.startsWith(DatabricksTypeUtil.STRUCT)) {
          if (element instanceof Map) {
            convertedElements[i] = new DatabricksStruct((Map<String, Object>) element, elementType);
          } else if (element instanceof DatabricksStruct) {
            convertedElements[i] = element;
          } else {
            throw new DatabricksDriverException(
                "Expected a Map for STRUCT but found: " + element.getClass().getSimpleName(),
                DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
          }
        } else if (elementType.startsWith(DatabricksTypeUtil.ARRAY)) {
          if (element instanceof List) {
            convertedElements[i] = new DatabricksArray((List<Object>) element, elementType);
          } else if (element instanceof DatabricksArray) {
            convertedElements[i] = element;
          } else {
            throw new DatabricksDriverException(
                "Expected a List for ARRAY but found: " + element.getClass().getSimpleName(),
                DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
          }
        } else if (elementType.startsWith(DatabricksTypeUtil.MAP)) {
          if (element instanceof Map) {
            convertedElements[i] = new DatabricksMap<>((Map<String, Object>) element, elementType);
          } else if (element instanceof DatabricksMap) {
            convertedElements[i] = element;
          } else {
            throw new DatabricksDriverException(
                "Expected a Map for MAP but found: " + element.getClass().getSimpleName(),
                DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
          }
        } else {
          convertedElements[i] = convertValue(element, elementType);
        }
      } catch (Exception e) {
        String errorMessage =
            String.format("Error converting element at index %s: %s", i, e.getMessage());
        LOGGER.error(e, errorMessage);
        throw new DatabricksDriverException(
            "Error converting elements",
            e,
            DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
      }
    }
    return convertedElements;
  }

  /**
   * Converts a simple element to the specified type.
   *
   * @param value the value to convert
   * @param type the type to convert the value to
   * @return the converted value
   */
  private Object convertValue(Object value, String type) {
    LOGGER.trace("Converting simple value of type: {}", type);
    if (value == null) {
      return null;
    }

    try {
      switch (type.toUpperCase()) {
        case DatabricksTypeUtil.INT:
          return Integer.parseInt(value.toString());
        case DatabricksTypeUtil.BIGINT:
          return Long.parseLong(value.toString());
        case DatabricksTypeUtil.SMALLINT:
          return Short.parseShort(value.toString());
        case DatabricksTypeUtil.FLOAT:
          return Float.parseFloat(value.toString());
        case DatabricksTypeUtil.DOUBLE:
          return Double.parseDouble(value.toString());
        case DatabricksTypeUtil.DECIMAL:
          return new BigDecimal(value.toString());
        case DatabricksTypeUtil.BOOLEAN:
          return Boolean.parseBoolean(value.toString());
        case DatabricksTypeUtil.DATE:
          return Date.valueOf(value.toString());
        case DatabricksTypeUtil.TIMESTAMP:
          return Timestamp.valueOf(value.toString());
        case DatabricksTypeUtil.TIME:
          return Time.valueOf(value.toString());
        case DatabricksTypeUtil.BINARY:
          return value instanceof byte[] ? value : value.toString().getBytes();
        case DatabricksTypeUtil.STRING:
        default:
          return value.toString();
      }
    } catch (Exception e) {
      String errorMessage =
          String.format("Error converting simple value of type %s: %s", type, e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksDriverException(
          errorMessage, DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_ARRAY_CONVERSION_ERROR);
    }
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    LOGGER.debug("Getting base type name");
    return this.typeName;
  }

  @Override
  public int getBaseType() throws SQLException {
    LOGGER.debug("Getting base type");
    return java.sql.Types.OTHER; // Or appropriate SQL type
  }

  @Override
  public Object getArray() throws SQLException {
    LOGGER.debug("Getting array elements");
    return this.elements;
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting array with type map");
    return this.getArray();
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    LOGGER.debug("Getting subarray from index {} with count {}", index, count);
    return java.util.Arrays.copyOfRange(this.elements, (int) index - 1, (int) index - 1 + count);
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("Getting subarray with type map from index {} with count {}", index, count);
    return this.getArray(index, count);
  }

  @Override
  public void free() throws SQLException {
    LOGGER.debug("Freeing resources (if any)");
  }

  @Override
  public java.sql.ResultSet getResultSet() throws SQLException {
    LOGGER.error("getResultSet() not implemented");
    throw new DatabricksSQLFeatureNotSupportedException("getResultSet() not implemented");
  }

  @Override
  public java.sql.ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    LOGGER.error("getResultSet(Map<String, Class<?>> map) not implemented");
    throw new DatabricksSQLFeatureNotSupportedException(
        "getResultSet(Map<String, Class<?>> map) not implemented");
  }

  @Override
  public java.sql.ResultSet getResultSet(long index, int count) throws SQLException {
    LOGGER.error("getResultSet(long index, int count) not implemented");
    throw new DatabricksSQLFeatureNotSupportedException(
        "getResultSet(long index, int count) not implemented");
  }

  @Override
  public java.sql.ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    LOGGER.error("getResultSet(long index, int count, Map<String, Class<?>> map) not implemented");
    throw new DatabricksSQLFeatureNotSupportedException(
        "getResultSet(long index, int count, Map<String, Class<?>> map) not implemented");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < elements.length; i++) {
      if (i > 0) {
        sb.append(",");
      }

      Object element = elements[i];
      if (element == null) {
        // JSON-like null
        sb.append("null");
      } else if (element instanceof String || DatabricksTypeUtil.isTemporalType(element)) {
        // Strings and temporal types get quoted
        sb.append("\"").append(element.toString()).append("\"");
      } else {
        // For non-string, just call toString()
        sb.append(element.toString());
      }
    }
    sb.append("]");
    return sb.toString();
  }
}
