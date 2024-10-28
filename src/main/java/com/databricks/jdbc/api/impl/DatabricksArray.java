package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;

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

  public DatabricksArray(List<Object> elements, String metadata) {
    LOGGER.debug("Initializing DatabricksArray with metadata: {}", metadata);
    String elementType = MetadataParser.parseArrayMetadata(metadata);
    this.elements = convertElements(elements, elementType);
    this.typeName = metadata;
  }

  private Object[] convertElements(List<Object> elements, String elementType) {
    LOGGER.debug("Converting elements with element type: {}", elementType);
    Object[] convertedElements = new Object[elements.size()];

    for (int i = 0; i < elements.size(); i++) {
      Object element = elements.get(i);
      try {
        if (elementType.startsWith("STRUCT")) {
          LOGGER.trace("Processing STRUCT element at index {}", i);
          if (element instanceof Map) {
            convertedElements[i] = new DatabricksStruct((Map<String, Object>) element, elementType);
          } else if (element instanceof String) {
            ComplexDataTypeParser parser = new ComplexDataTypeParser();
            Map<String, Object> structMap = parser.parseToMap((String) element, elementType);
            convertedElements[i] = new DatabricksStruct(structMap, elementType);
          } else {
            throw new IllegalArgumentException(
                    "Expected a Map or String for STRUCT but found: " + element.getClass().getSimpleName());
          }
        } else if (elementType.startsWith("ARRAY")) {
          LOGGER.trace("Processing ARRAY element at index {}", i);
          if (element instanceof List) {
            convertedElements[i] = new DatabricksArray((List<Object>) element, elementType);
          } else if (element instanceof String) {
            ComplexDataTypeParser parser = new ComplexDataTypeParser();
            List<Object> arrayList = parser.parseToArray(parser.parse((String) element), elementType);
            convertedElements[i] = new DatabricksArray(arrayList, elementType);
          } else {
            throw new IllegalArgumentException(
                    "Expected a List or String for ARRAY but found: " + element.getClass().getSimpleName());
          }
        } else if (elementType.startsWith("MAP")) {
          LOGGER.trace("Processing MAP element at index {}", i);
          if (element instanceof Map) {
            convertedElements[i] = new DatabricksMap<>((Map<String, Object>) element, elementType);
          } else if (element instanceof String) {
            ComplexDataTypeParser parser = new ComplexDataTypeParser();
            Map<String, Object> map = parser.parseToMap((String) element, elementType);
            convertedElements[i] = new DatabricksMap<>(map, elementType);
          } else {
            throw new IllegalArgumentException(
                    "Expected a Map or String for MAP but found: " + element.getClass().getSimpleName());
          }
        } else {
          convertedElements[i] = convertValue(element, elementType);
        }
      } catch (Exception e) {
        LOGGER.error("Error converting element at index {}: {}", i, e.getMessage(), e);
        throw new IllegalArgumentException("Error converting elements", e);
      }
    }
    return convertedElements;
  }

  private Object convertValue(Object value, String type) {
    LOGGER.trace("Converting simple value of type: {}", type);
    if (value == null) {
      return null;
    }

    try {
      switch (type.toUpperCase()) {
        case "INT":
        case "INTEGER":
          return Integer.parseInt(value.toString());
        case "BIGINT":
          return Long.parseLong(value.toString());
        case "SMALLINT":
          return Short.parseShort(value.toString());
        case "FLOAT":
          return Float.parseFloat(value.toString());
        case "DOUBLE":
          return Double.parseDouble(value.toString());
        case "DECIMAL":
        case "NUMERIC":
          return new BigDecimal(value.toString());
        case "BOOLEAN":
          return Boolean.parseBoolean(value.toString());
        case "DATE":
          return Date.valueOf(value.toString());
        case "TIMESTAMP":
          return Timestamp.valueOf(value.toString());
        case "TIME":
          return Time.valueOf(value.toString());
        case "BINARY":
          return value instanceof byte[] ? value : value.toString().getBytes();
        case "STRING":
        case "VARCHAR":
        case "CHAR":
        default:
          return value.toString();
      }
    } catch (Exception e) {
      LOGGER.error("Error converting simple value of type {}: {}", type, e.getMessage(), e);
      throw new IllegalArgumentException(
              "Failed to convert value " + value + " to type " + type, e);
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
    // No resources to free in this implementation
  }

  @Override
  public java.sql.ResultSet getResultSet() throws SQLException {
    LOGGER.error("getResultSet() not implemented");
    throw new UnsupportedOperationException("getResultSet() not implemented");
  }

  @Override
  public java.sql.ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    LOGGER.error("getResultSet(Map<String, Class<?>> map) not implemented");
    throw new UnsupportedOperationException("getResultSet(Map<String, Class<?>> map) not implemented");
  }

  @Override
  public java.sql.ResultSet getResultSet(long index, int count) throws SQLException {
    LOGGER.error("getResultSet(long index, int count) not implemented");
    throw new UnsupportedOperationException("getResultSet(long index, int count) not implemented");
  }

  @Override
  public java.sql.ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
          throws SQLException {
    LOGGER.error("getResultSet(long index, int count, Map<String, Class<?>> map) not implemented");
    throw new UnsupportedOperationException(
            "getResultSet(long index, int count, Map<String, Class<?>> map) not implemented");
  }
}
