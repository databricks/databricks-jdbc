package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Class for representation of Struct complex object. */
public class DatabricksStruct implements Struct {
  private final Object[] attributes;
  private final String typeName;

  /**
   * Constructs a DatabricksStruct with the specified attributes and metadata.
   *
   * @param attributes the attributes of the struct as a map
   * @param metadata the metadata describing types of struct fields
   */
  public DatabricksStruct(Map<String, Object> attributes, String metadata) {
    Map<String, String> typeMap = MetadataParser.parseStructMetadata(metadata);
    this.attributes = convertAttributes(attributes, typeMap);
    this.typeName = metadata;
  }

  /**
   * Converts the provided attributes based on specified type metadata.
   *
   * @param attributes the original attributes to be converted
   * @param typeMap a map specifying the type of each attribute
   * @return an array of converted attributes
   */
  private Object[] convertAttributes(Map<String, Object> attributes, Map<String, String> typeMap) {
    Object[] convertedAttributes = new Object[typeMap.size()];
    int index = 0;

    for (Map.Entry<String, String> entry : typeMap.entrySet()) {
      String fieldName = entry.getKey();
      String fieldType = entry.getValue();
      Object value = attributes.get(fieldName);

      if (fieldType.startsWith(DatabricksTypeUtil.STRUCT)) {
        if (value instanceof Map) {
          convertedAttributes[index] = new DatabricksStruct((Map<String, Object>) value, fieldType);
        } else if (value instanceof DatabricksStruct) {
          convertedAttributes[index] = value;
        } else {
          throw new IllegalArgumentException(
              "Expected a Map for STRUCT but found: "
                  + (value == null ? "null" : value.getClass().getSimpleName()));
        }
      } else if (fieldType.startsWith(DatabricksTypeUtil.ARRAY)) {
        if (value instanceof List) {
          convertedAttributes[index] = new DatabricksArray((List<Object>) value, fieldType);
        } else if (value instanceof DatabricksArray) {
          convertedAttributes[index] = value;
        } else {
          throw new IllegalArgumentException(
              "Expected a List for ARRAY but found: "
                  + (value == null ? "null" : value.getClass().getSimpleName()));
        }
      } else if (fieldType.startsWith(DatabricksTypeUtil.MAP)) {
        if (value instanceof Map) {
          convertedAttributes[index] = new DatabricksMap<>((Map<String, Object>) value, fieldType);
        } else if (value instanceof DatabricksMap) {
          convertedAttributes[index] = value;
        } else {
          throw new IllegalArgumentException(
              "Expected a Map for MAP but found: "
                  + (value == null ? "null" : value.getClass().getSimpleName()));
        }
      } else {
        convertedAttributes[index] = convertSimpleValue(value, fieldType);
      }

      index++;
    }

    return convertedAttributes;
  }

  /**
   * Converts a simple attribute to the specified type.
   *
   * @param value the value to convert
   * @param type the type to convert the value to
   * @return the converted value
   */
  private Object convertSimpleValue(Object value, String type) {
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
      throw new IllegalArgumentException(
          "Failed to convert value " + value + " to type " + type, e);
    }
  }

  /**
   * Retrieves the SQL type name of this Struct.
   *
   * @return the SQL type name of this Struct
   * @throws SQLException if a database access error occurs
   */
  @Override
  public String getSQLTypeName() throws SQLException {
    return this.typeName;
  }

  /**
   * Retrieves the attributes of this Struct as an array.
   *
   * @return an array containing the attributes of the Struct
   * @throws SQLException if a database access error occurs
   */
  @Override
  public Object[] getAttributes() throws SQLException {
    return this.attributes;
  }

  /**
   * Retrieves the attributes of this Struct as an array, using the specified type map.
   *
   * @param map a Map object that contains the mapping of SQL types to Java classes
   * @return an array containing the attributes of the Struct
   * @throws SQLException if a database access error occurs
   */
  @Override
  public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
    return this.getAttributes();
  }

  @Override
  public String toString() {
    return Arrays.deepToString(attributes);
  }
}
