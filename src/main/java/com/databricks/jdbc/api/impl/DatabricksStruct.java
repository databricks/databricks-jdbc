package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Class for representation of Struct complex object. */
public class DatabricksStruct implements Struct {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksStruct.class);
  private final Object[] attributes;
  private final String typeName;

  // Field names to preserve ordering and allow mapping back in toString().
  private final List<String> fieldNames;

  /**
   * Constructs a DatabricksStruct with the specified attributes and metadata.
   *
   * @param attributes the attributes of the struct as a map
   * @param metadata the metadata describing types of struct fields
   */
  public DatabricksStruct(Map<String, Object> attributes, String metadata) {
    // Parse the metadata into a map: fieldName -> fieldType
    Map<String, String> typeMap = MetadataParser.parseStructMetadata(metadata);

    // Capture field names (in the same order they appear in typeMap).
    this.fieldNames = new ArrayList<>(typeMap.keySet());

    // Convert attributes to the appropriate array of Objects.
    this.attributes = convertAttributes(attributes, typeMap);

    // Store the entire type definition for getSQLTypeName().
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

    for (String fieldName : fieldNames) {
      String fieldType = typeMap.get(fieldName);
      Object value = attributes.get(fieldName);

      if (value == null) {
        convertedAttributes[index] = value;
      } else if (fieldType.startsWith(DatabricksTypeUtil.STRUCT)) {
        if (value instanceof Map) {
          convertedAttributes[index] = new DatabricksStruct((Map<String, Object>) value, fieldType);
        } else if (value instanceof DatabricksStruct) {
          convertedAttributes[index] = value;
        } else {
          throwConversionException("Map for STRUCT", value);
        }
      } else if (fieldType.startsWith(DatabricksTypeUtil.ARRAY)) {
        if (value instanceof List) {
          convertedAttributes[index] = new DatabricksArray((List<Object>) value, fieldType);
        } else if (value instanceof DatabricksArray) {
          convertedAttributes[index] = value;
        } else {
          throwConversionException("List for ARRAY", value);
        }
      } else if (fieldType.startsWith(DatabricksTypeUtil.MAP)) {
        if (value instanceof Map) {
          convertedAttributes[index] = new DatabricksMap<>((Map<String, Object>) value, fieldType);
        } else if (value instanceof DatabricksMap) {
          convertedAttributes[index] = value;
        } else {
          throwConversionException("Map for MAP", value);
        }
      } else if (fieldType.startsWith(DatabricksTypeUtil.VARIANT)) {
        // For VARIANT, preserve JsonNode objects as-is, otherwise convert to string
        convertedAttributes[index] = value;
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
      String errorMessage = String.format("Failed to convert value %s to type %s", value, type);
      LOGGER.error(errorMessage);
      throw new DatabricksDriverException(
          errorMessage, DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_STRUCT_CONVERSION_ERROR);
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

  /** Returns a JSON-like string with field names. */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < fieldNames.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append("\"").append(fieldNames.get(i)).append("\":");

      Object val = attributes[i];
      if (val == null) {
        // JSON-like null
        sb.append("null");
      } else if (val instanceof JsonNode) {
        // VARIANT fields represented as JsonNode - preserve JSON structure without quotes
        sb.append(val.toString());
      } else if (val instanceof String || DatabricksTypeUtil.isTemporalType(val)) {
        // Strings and temporal types get quoted and escaped
        sb.append("\"")
            .append(val.toString().replace("\\", "\\\\").replace("\"", "\\\""))
            .append("\"");
      } else {
        // For non-string values, rely on their existing toString()
        sb.append(val);
      }
    }
    sb.append("}");
    return sb.toString();
  }

  void throwConversionException(String datatype, Object value) {
    String errorMessage =
        String.format(
            "Expected a %s but found: %s",
            datatype, (value == null ? "null" : value.getClass().getSimpleName()));
    LOGGER.error(errorMessage);
    throw new DatabricksDriverException(
        errorMessage, DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_STRUCT_CONVERSION_ERROR);
  }
}
