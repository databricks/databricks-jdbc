package com.databricks.jdbc.api.impl;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class DatabricksStruct implements Struct {
  private final Object[] attributes;
  private final String typeName;

  public DatabricksStruct(Map<String, Object> attributes, String metadata) {
    Map<String, Object> typeMap = MetadataParser.parseStructMetadata(metadata);
    this.attributes = convertAttributes(attributes, typeMap);
    this.typeName = metadata;
  }

  private Object[] convertAttributes(Map<String, Object> attributes, Map<String, Object> typeMap) {
    Object[] convertedAttributes = new Object[typeMap.size()];
    int index = 0;

    for (Map.Entry<String, Object> entry : typeMap.entrySet()) {
      String fieldName = entry.getKey();
      Object fieldType = entry.getValue();
      Object value = attributes.get(fieldName);

      if (fieldType instanceof String) {
        String fieldTypeStr = (String) fieldType;

        if (fieldTypeStr.startsWith("STRUCT")) {
          if (value instanceof Map) {
            convertedAttributes[index] =
                new DatabricksStruct((Map<String, Object>) value, fieldTypeStr);
          } else {
            throw new IllegalArgumentException(
                "Expected a Map for STRUCT but found: " + value.getClass().getSimpleName());
          }
        } else if (fieldTypeStr.startsWith("ARRAY")) {
          if (value instanceof List) {
            convertedAttributes[index] = new DatabricksArray((List<Object>) value, fieldTypeStr);
          } else {
            throw new IllegalArgumentException(
                "Expected a List for ARRAY but found: " + value.getClass().getSimpleName());
          }
        } else if (fieldTypeStr.startsWith("MAP")) {
          if (value instanceof Map) {
            convertedAttributes[index] =
                new DatabricksMap<>((Map<String, Object>) value, fieldTypeStr);
          } else {
            throw new IllegalArgumentException(
                "Expected a Map for MAP but found: " + value.getClass().getSimpleName());
          }
        } else {
          // Handle SQL types conversion
          convertedAttributes[index] = convertValue(value, fieldTypeStr);
        }
      } else {
        convertedAttributes[index] = value;
      }

      index++;
    }

    return convertedAttributes;
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
  public String getSQLTypeName() throws SQLException {
    return this.typeName;
  }

  @Override
  public Object[] getAttributes() throws SQLException {
    return this.attributes;
  }

  @Override
  public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
    return this.getAttributes();
  }
}
