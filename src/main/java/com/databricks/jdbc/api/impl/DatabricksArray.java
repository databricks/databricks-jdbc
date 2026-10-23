package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.JsonUtil;
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
import java.util.Locale;
import java.util.Map;

/** Class for representation of Array complex object. */
public class DatabricksArray implements Array {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksArray.class);

  private final Object[] elements;
  private final String typeName;
  private final String elementTypeName;

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
    this.elementTypeName = elementType;
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
          } else if (element == null) {
            convertedElements[i] = null;
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
          } else if (element == null) {
            convertedElements[i] = null;
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
          } else if (element == null) {
            convertedElements[i] = null;
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
    if (DatabricksTypeUtil.isGeospatialType(type.toUpperCase(Locale.ROOT))) {
      return value;
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
        case DatabricksTypeUtil.TIMESTAMP_NTZ:
          return value instanceof Timestamp ? value : Timestamp.valueOf(value.toString());
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

  /**
   * Returns the SQL type name of the elements in this array, as required by {@link
   * Array#getBaseTypeName()} (e.g. {@code INT} for {@code ARRAY<INT>}).
   */
  @Override
  public String getBaseTypeName() throws SQLException {
    LOGGER.debug("Getting base type name");
    return this.elementTypeName;
  }

  /**
   * Returns the JDBC type code ({@link java.sql.Types}) of the elements in this array, as required
   * by {@link Array#getBaseType()}.
   */
  @Override
  public int getBaseType() throws SQLException {
    LOGGER.debug("Getting base type");
    return getSqlTypeForElement(this.elementTypeName);
  }

  /**
   * Maps an element type name (possibly parameterized, e.g. {@code DECIMAL(10,2)} or {@code
   * STRUCT<a:INT>}) to its {@link java.sql.Types} code.
   */
  private static int getSqlTypeForElement(String elementTypeName) {
    if (elementTypeName == null || elementTypeName.trim().isEmpty()) {
      return java.sql.Types.OTHER;
    }
    String upper = elementTypeName.trim().toUpperCase(Locale.ROOT);
    if (DatabricksTypeUtil.recoverStringType(upper) != null) {
      return java.sql.Types.VARCHAR;
    }
    int end = upper.length();
    for (int i = 0; i < upper.length(); i++) {
      char c = upper.charAt(i);
      if (c == '<' || c == '(') {
        end = i;
        break;
      }
    }
    String baseName = upper.substring(0, end).trim();
    // Report the type of the objects actually stored by convertElements/convertValue, which can
    // differ from the column-level ResultSetMetaData mapping: MAP elements are DatabricksMap
    // instances (not strings, so not VARCHAR), and TIME elements are java.sql.Time values.
    if (DatabricksTypeUtil.MAP.equals(baseName)) {
      return java.sql.Types.JAVA_OBJECT;
    }
    if (DatabricksTypeUtil.TIME.equals(baseName)) {
      return java.sql.Types.TIME;
    }
    try {
      return DatabricksTypeUtil.getColumnType(DatabricksTypeUtil.getColumnInfoType(baseName));
    } catch (Exception e) {
      LOGGER.debug("Unable to map array element type {}: {}", elementTypeName, e.getMessage());
      return java.sql.Types.OTHER;
    }
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

      sb.append(JsonUtil.formatComplexValue(elements[i]));
    }
    sb.append("]");
    return sb.toString();
  }
}
