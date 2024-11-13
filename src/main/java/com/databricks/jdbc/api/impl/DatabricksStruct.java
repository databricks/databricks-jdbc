package com.databricks.jdbc.api.impl;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.Map;

/** Class for representation of Struct complex object. */
public class DatabricksStruct implements Struct {
  private final Object[] attributes;
  private final String typeName;

  // Constants for type names
  private static final String TYPE_STRUCT = "STRUCT";
  private static final String TYPE_ARRAY = "ARRAY";
  private static final String TYPE_MAP = "MAP";
  private static final String TYPE_INT = "INT";
  private static final String TYPE_INTEGER = "INTEGER";
  private static final String TYPE_BIGINT = "BIGINT";
  private static final String TYPE_SMALLINT = "SMALLINT";
  private static final String TYPE_FLOAT = "FLOAT";
  private static final String TYPE_DOUBLE = "DOUBLE";
  private static final String TYPE_DECIMAL = "DECIMAL";
  private static final String TYPE_NUMERIC = "NUMERIC";
  private static final String TYPE_BOOLEAN = "BOOLEAN";
  private static final String TYPE_DATE = "DATE";
  private static final String TYPE_TIMESTAMP = "TIMESTAMP";
  private static final String TYPE_TIME = "TIME";
  private static final String TYPE_BINARY = "BINARY";
  private static final String TYPE_STRING = "STRING";
  private static final String TYPE_VARCHAR = "VARCHAR";
  private static final String TYPE_CHAR = "CHAR";

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

      if (fieldType.startsWith(TYPE_STRUCT)) {
        if (value instanceof Map) {
          convertedAttributes[index] = new DatabricksStruct((Map<String, Object>) value, fieldType);
        } else if (value instanceof String) {
          ComplexDataTypeParser parser = new ComplexDataTypeParser();
          Map<String, Object> structMap =
              parser.parseToStruct(
                  parser.parse((String) value), MetadataParser.parseStructMetadata(fieldType));
          convertedAttributes[index] = new DatabricksStruct(structMap, fieldType);
        } else {
          throw new IllegalArgumentException(
              "Expected a Map or String for STRUCT but found: " + value.getClass().getSimpleName());
        }
      } else if (fieldType.startsWith(TYPE_ARRAY)) {
        if (value instanceof List) {
          convertedAttributes[index] = new DatabricksArray((List<Object>) value, fieldType);
        } else if (value instanceof String) {
          ComplexDataTypeParser parser = new ComplexDataTypeParser();
          List<Object> arrayList =
              parser.parseToArray(
                  parser.parse((String) value), MetadataParser.parseArrayMetadata(fieldType));
          convertedAttributes[index] = new DatabricksArray(arrayList, fieldType);
        } else {
          throw new IllegalArgumentException(
              "Expected a List or String for ARRAY but found: " + value.getClass().getSimpleName());
        }
      } else if (fieldType.startsWith(TYPE_MAP)) {
        if (value instanceof Map) {
          convertedAttributes[index] = new DatabricksMap<>((Map<String, Object>) value, fieldType);
        } else if (value instanceof String) {
          ComplexDataTypeParser parser = new ComplexDataTypeParser();
          Map<String, Object> map = parser.parseToMap((String) value, fieldType);
          convertedAttributes[index] = new DatabricksMap<>(map, fieldType);
        } else {
          throw new IllegalArgumentException(
              "Expected a Map or String for MAP but found: " + value.getClass().getSimpleName());
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

    switch (type.toUpperCase()) {
      case TYPE_INT:
      case TYPE_INTEGER:
        return Integer.parseInt(value.toString());
      case TYPE_BIGINT:
        return Long.parseLong(value.toString());
      case TYPE_SMALLINT:
        return Short.parseShort(value.toString());
      case TYPE_FLOAT:
        return Float.parseFloat(value.toString());
      case TYPE_DOUBLE:
        return Double.parseDouble(value.toString());
      case TYPE_DECIMAL:
      case TYPE_NUMERIC:
        return new BigDecimal(value.toString());
      case TYPE_BOOLEAN:
        return Boolean.parseBoolean(value.toString());
      case TYPE_DATE:
        return Date.valueOf(value.toString());
      case TYPE_TIMESTAMP:
        return Timestamp.valueOf(value.toString());
      case TYPE_TIME:
        return Time.valueOf(value.toString());
      case TYPE_BINARY:
        return value instanceof byte[] ? value : value.toString().getBytes();
      case TYPE_STRING:
      case TYPE_VARCHAR:
      case TYPE_CHAR:
      default:
        return value.toString();
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
}
