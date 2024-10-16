package com.databricks.jdbc.api.impl;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class DatabricksStruct implements Struct {
  private final Object[] attributes;
  private final String typeName;

  public DatabricksStruct(Map<String, Object> attributes, String metadata) {
    // Parse the metadata into a type map
    Map<String, String> typeMap = MetadataParser.parseStructMetadata(metadata);
    this.attributes = convertAttributes(attributes, typeMap);
    this.typeName = metadata;
  }

  private Object[] convertAttributes(Map<String, Object> attributes, Map<String, String> typeMap) {
    Object[] convertedAttributes = new Object[typeMap.size()];
    int index = 0;

    for (Map.Entry<String, String> entry : typeMap.entrySet()) {
      String fieldName = entry.getKey();
      String fieldType = entry.getValue();
      Object value = attributes.get(fieldName);

      if (fieldType.startsWith("STRUCT")) {
        if (value instanceof Map) {
          convertedAttributes[index] = new DatabricksStruct((Map<String, Object>) value, fieldType);
        } else if (value instanceof String) {
          // Parse the JSON string for the nested struct
          ComplexDataTypeParser parser = new ComplexDataTypeParser();
          Map<String, Object> structMap = parser.parseToStruct(parser.parse((String) value), MetadataParser.parseStructMetadata(fieldType));
          convertedAttributes[index] = new DatabricksStruct(structMap, fieldType);
        } else {
          throw new IllegalArgumentException("Expected a Map or String for STRUCT but found: " + value.getClass().getSimpleName());
        }
      } else if (fieldType.startsWith("ARRAY")) {
        if (value instanceof List) {
          convertedAttributes[index] = new DatabricksArray((List<Object>) value, fieldType);
        } else if (value instanceof String) {
          // Parse the JSON string for the array
          ComplexDataTypeParser parser = new ComplexDataTypeParser();
          List<Object> arrayList = parser.parseToArray(parser.parse((String) value), MetadataParser.parseArrayMetadata(fieldType));
          convertedAttributes[index] = new DatabricksArray(arrayList, fieldType);
        } else {
          throw new IllegalArgumentException("Expected a List or String for ARRAY but found: " + value.getClass().getSimpleName());
        }
      } else if (fieldType.startsWith("MAP")) {
        if (value instanceof Map) {
          convertedAttributes[index] = new DatabricksMap<>((Map<String, Object>) value, fieldType);
        } else if (value instanceof String) {
          // Parse the JSON string for the map
          ComplexDataTypeParser parser = new ComplexDataTypeParser();
          Map<String, Object> map = parser.parseToMap((String) value, fieldType);
          convertedAttributes[index] = new DatabricksMap<>(map, fieldType);
        } else {
          throw new IllegalArgumentException("Expected a Map or String for MAP but found: " + value.getClass().getSimpleName());
        }
      } else {
        // Handle SQL types conversion for simple fields
        convertedAttributes[index] = convertSimpleValue(value, fieldType);
      }

      index++;
    }

    return convertedAttributes;
  }

  private Object convertSimpleValue(Object value, String type) {
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
