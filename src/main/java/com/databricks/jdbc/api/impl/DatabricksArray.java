package com.databricks.jdbc.api.impl;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class DatabricksArray implements Array {
  private final Object[] elements;
  private final String typeName;

  public DatabricksArray(List<Object> elements, String metadata) {
    String elementType = MetadataParser.parseArrayMetadata(metadata);
    this.elements = convertElements(elements, elementType);
    this.typeName = metadata;
  }

  private Object[] convertElements(List<Object> elements, String elementType) {
    Object[] convertedElements = new Object[elements.size()];

    for (int i = 0; i < elements.size(); i++) {
      Object element = elements.get(i);
      if (elementType.startsWith("STRUCT")) {
        if (element instanceof Map) {
          convertedElements[i] = new DatabricksStruct((Map<String, Object>) element, elementType);
        } else if (element instanceof String) {
          ComplexDataTypeParser parser = new ComplexDataTypeParser();
          Map<String, Object> structMap = parser.parseToMap((String) element, elementType);
          convertedElements[i] = new DatabricksStruct(structMap, elementType);
        } else {
          throw new IllegalArgumentException("Expected a Map or String for STRUCT but found: " + element.getClass().getSimpleName());
        }
      } else if (elementType.startsWith("ARRAY")) {
        if (element instanceof List) {
          convertedElements[i] = new DatabricksArray((List<Object>) element, elementType);
        } else if (element instanceof String) {
          ComplexDataTypeParser parser = new ComplexDataTypeParser();
          List<Object> arrayList = parser.parseToArray(parser.parse((String) element), elementType);
          convertedElements[i] = new DatabricksArray(arrayList, elementType);
        } else {
          throw new IllegalArgumentException("Expected a List or String for ARRAY but found: " + element.getClass().getSimpleName());
        }
      } else if (elementType.startsWith("MAP")) {
        if (element instanceof Map) {
          convertedElements[i] = new DatabricksMap<>((Map<String, Object>) element, elementType);
        } else if (element instanceof String) {
          ComplexDataTypeParser parser = new ComplexDataTypeParser();
          Map<String, Object> map = parser.parseToMap((String) element, elementType);
          convertedElements[i] = new DatabricksMap<>(map, elementType);
        } else {
          throw new IllegalArgumentException("Expected a Map or String for MAP but found: " + element.getClass().getSimpleName());
        }
      } else {
        convertedElements[i] = convertValue(element, elementType);
      }
    }
    return convertedElements;
  }

  private Object convertValue(Object value, String type) {
    if (value == null) {
      return null;
    }

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
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    return this.typeName;
  }

  @Override
  public int getBaseType() throws SQLException {
    return java.sql.Types.OTHER; // Or appropriate SQL type
  }

  @Override
  public Object getArray() throws SQLException {
    return this.elements;
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    return this.getArray();
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    return java.util.Arrays.copyOfRange(this.elements, (int) index - 1, (int) index - 1 + count);
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    return this.getArray(index, count);
  }

  @Override
  public void free() throws SQLException {
    // No resources to free in this implementation
  }

  @Override
  public java.sql.ResultSet getResultSet() throws SQLException {
    throw new UnsupportedOperationException("getResultSet() not implemented");
  }

  @Override
  public java.sql.ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException("getResultSet(Map<String, Class<?>> map) not implemented");
  }

  @Override
  public java.sql.ResultSet getResultSet(long index, int count) throws SQLException {
    throw new UnsupportedOperationException("getResultSet(long index, int count) not implemented");
  }

  @Override
  public java.sql.ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
          throws SQLException {
    throw new UnsupportedOperationException("getResultSet(long index, int count, Map<String, Class<?>> map) not implemented");
  }
}
