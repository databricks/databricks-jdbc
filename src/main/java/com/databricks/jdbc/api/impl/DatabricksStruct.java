package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.impl.converters.JavaTimeConverter;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Class for representation of Struct complex object. */
public class DatabricksStruct implements Struct {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksStruct.class);
  private final Object[] attributes;
  private final String typeName;

  // Field names to preserve ordering and allow mapping back in toString().
  private final List<String> fieldNames;

  // Field types, parallel to fieldNames.
  private final List<String> fieldTypes;

  // Sources of the exact java.time values of DATE/TIMESTAMP fields (JSON leaves, converted on
  // demand, or java.time values), parallel to attributes; null when there are none.
  private final Object[] temporalAttributes;

  // Set once an attribute fell back to its java.sql value, so the warning is logged once.
  private boolean warnedInexact;

  /**
   * Constructs a DatabricksStruct with the specified attributes and metadata.
   *
   * @param attributes the attributes of the struct as a map
   * @param metadata the metadata describing types of struct fields
   */
  public DatabricksStruct(Map<String, Object> attributes, String metadata) {
    this(attributes, metadata, null);
  }

  /**
   * Constructs a DatabricksStruct that also keeps the exact {@code java.time} values of DATE and
   * TIMESTAMP fields. {@link #getAttributes()} still returns {@code java.sql} values; the exact
   * values are returned by {@link #getAttributes(Map)} with a {@code java.time} mapping.
   *
   * @param attributes the attributes of the struct as a map
   * @param metadata the metadata describing types of struct fields
   * @param temporalAttributes sources of the exact values by field name (JSON leaves, converted
   *     only when requested, or {@code java.time} values), may be null; missing fields fall back to
   *     the {@code java.sql} attribute
   */
  DatabricksStruct(
      Map<String, Object> attributes, String metadata, Map<String, Object> temporalAttributes) {
    // Parse the metadata into a map: fieldName -> fieldType
    Map<String, String> typeMap = MetadataParser.parseStructMetadata(metadata);

    // Capture field names (in the same order they appear in typeMap).
    this.fieldNames = new ArrayList<>(typeMap.keySet());
    this.fieldTypes = new ArrayList<>(typeMap.values());

    // Convert attributes to the appropriate array of Objects.
    this.attributes = convertAttributes(attributes, typeMap);

    if (temporalAttributes == null || temporalAttributes.isEmpty()) {
      this.temporalAttributes = null;
    } else {
      this.temporalAttributes = new Object[fieldNames.size()];
      for (int i = 0; i < fieldNames.size(); i++) {
        this.temporalAttributes[i] = temporalAttributes.get(fieldNames.get(i));
      }
    }

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
          // Keep a parsed Date: rebuilding it from toString() drops the BC era.
          return value instanceof Date ? value : Date.valueOf(value.toString());
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
   * <p>If {@code map} maps a field's type (DATE, TIMESTAMP or TIMESTAMP_NTZ) to a {@code java.time}
   * class, for example {@code Map.of("TIMESTAMP", Instant.class)}, that field is returned as the
   * class, built from the exact value instead of the {@code java.sql} one. Other mappings are
   * ignored.
   *
   * @param map a Map object that contains the mapping of SQL types to Java classes
   * @return an array containing the attributes of the Struct
   * @throws SQLException if a database access error occurs or a mapped conversion is not supported
   */
  @Override
  public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
    Object[] converted = null;
    for (int i = 0; i < attributes.length; i++) {
      String fieldType = fieldTypes.get(i);
      Class<?> javaTimeType = JavaTimeConverter.getJavaTimeMapping(map, fieldType);
      if (javaTimeType == null) {
        continue;
      }
      if (converted == null) {
        converted = attributes.clone();
      }
      Object exact = exactAttribute(i, fieldType);
      Object value = exact != null ? exact : attributes[i];
      converted[i] = JavaTimeConverter.convertNested(javaTimeType, fieldType, value);
    }
    return converted != null ? converted : this.getAttributes();
  }

  /**
   * The exact {@code java.time} value of the attribute at {@code i}, or null if unknown. A JSON
   * leaf is parsed on the first call and replaced by its result, so later reads neither parse it
   * again nor repeat the warning for a value that has no exact form.
   */
  private Object exactAttribute(int i, String fieldType) {
    if (temporalAttributes == null) {
      return null;
    }
    Object source = temporalAttributes[i];
    Object exact = ComplexDataTypeParser.toExactTemporal(source, fieldType);
    if (ComplexDataTypeParser.isUnparsedLeaf(source)) {
      if (exact == null && !warnedInexact) {
        warnedInexact = true;
        ComplexDataTypeParser.warnInexactNestedValue(fieldType);
      }
      temporalAttributes[i] = exact;
    }
    return exact;
  }

  /** Returns a JSON-like string with field names. */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("{");
    for (int i = 0; i < fieldNames.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(JsonUtil.formatComplexValue(fieldNames.get(i))).append(":");
      sb.append(JsonUtil.formatComplexValue(attributes[i]));
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
