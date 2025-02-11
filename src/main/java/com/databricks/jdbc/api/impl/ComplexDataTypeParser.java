package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for parsing complex data type objects (map, array, struct) into the custom JDBC
 * wrapper objects (DatabricksArray, DatabricksMap, DatabricksStruct).
 */
public class ComplexDataTypeParser {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ComplexDataTypeParser.class);

  private final ObjectMapper objectMapper;

  /** Constructor class for ComplexDataTypeParser. */
  public ComplexDataTypeParser() {
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Parses a JSON string representing an ARRAY into a DatabricksArray.
   *
   * @param json The JSON string (e.g. "[1,2,3]" or "[{\"name\":\"John\"}, ...]")
   * @param arrayMetadata The type metadata (e.g. "ARRAY<INT>" or "ARRAY<STRUCT<...>>")
   * @return A DatabricksArray implementing java.sql.Array
   */
  public DatabricksArray parseJsonStringToDbArray(String json, String arrayMetadata) {
    try {
      JsonNode node = objectMapper.readTree(json);
      return parseToArray(node, arrayMetadata);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse JSON array from: " + json, e);
    }
  }

  /**
   * Parses a JSON string representing a MAP into a DatabricksMap.
   *
   * @param json The JSON string (e.g. "{\"key1\": 123, \"key2\": 456}" or
   *     "[{\"key\":...,\"value\":...},...]")
   * @param mapMetadata The type metadata (e.g. "MAP<STRING,INT>" or "MAP<STRING,ARRAY<INT>>")
   * @return A DatabricksMap implementing a JDBC-like Map interface
   */
  public DatabricksMap<String, Object> parseJsonStringToDbMap(String json, String mapMetadata) {
    try {
      JsonNode node = objectMapper.readTree(json);
      return parseToMap(node, mapMetadata);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse JSON map from: " + json, e);
    }
  }

  /**
   * Parses a JSON string representing a STRUCT into a DatabricksStruct.
   *
   * @param json The JSON string (e.g. "{\"name\":\"John\", \"age\":30}")
   * @param structMetadata The type metadata (e.g. "STRUCT<name:STRING, age:INT>")
   * @return A DatabricksStruct implementing java.sql.Struct
   */
  public DatabricksStruct parseJsonStringToDbStruct(String json, String structMetadata) {
    try {
      JsonNode node = objectMapper.readTree(json);
      return parseToStruct(node, structMetadata);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse JSON struct from: " + json, e);
    }
  }

  /** Parses a JsonNode into a DatabricksArray, given full array metadata (e.g. "ARRAY<INT>"). */
  public DatabricksArray parseToArray(JsonNode node, String arrayMetadata) {
    if (!node.isArray()) {
      throw new IllegalArgumentException("Expected JSON array, got: " + node);
    }
    LOGGER.debug("Parsing array with metadata: {}", arrayMetadata);

    String elementType = MetadataParser.parseArrayMetadata(arrayMetadata);

    List<Object> list = new ArrayList<>();
    for (JsonNode elementNode : node) {
      Object converted = convertValueNode(elementNode, elementType);
      list.add(converted);
    }

    return new DatabricksArray(list, arrayMetadata);
  }

  /**
   * Parses a JsonNode into a DatabricksMap, given full map metadata (e.g. "MAP<STRING,
   * ARRAY<INT>>").
   */
  public DatabricksMap<String, Object> parseToMap(JsonNode node, String mapMetadata) {
    if (!mapMetadata.startsWith(DatabricksTypeUtil.MAP)) {
      throw new IllegalArgumentException("Type is not a MAP: " + mapMetadata);
    }
    LOGGER.debug("Parsing map with metadata: {}", mapMetadata);

    String[] kv = MetadataParser.parseMapMetadata(mapMetadata).split(",", 2);
    String keyType = kv[0].trim();
    String valueType = kv[1].trim();

    Map<String, Object> rawMap = convertJsonNodeToJavaMap(node, keyType, valueType);

    return new DatabricksMap<>(rawMap, mapMetadata);
  }

  /**
   * Parses a JsonNode into a DatabricksStruct, given full struct metadata. e.g.
   * "STRUCT<name:STRING, age:INT>".
   */
  public DatabricksStruct parseToStruct(JsonNode node, String structMetadata) {
    if (!node.isObject()) {
      throw new IllegalArgumentException("Expected JSON object for STRUCT, got: " + node);
    }
    LOGGER.debug("Parsing struct with metadata: {}", structMetadata);

    Map<String, String> fieldTypeMap = MetadataParser.parseStructMetadata(structMetadata);

    Map<String, Object> structMap = new LinkedHashMap<>();

    node.fields()
        .forEachRemaining(
            entry -> {
              String fieldName = entry.getKey();
              JsonNode fieldNode = entry.getValue();

              String fieldType = fieldTypeMap.getOrDefault(fieldName, DatabricksTypeUtil.STRING);

              Object convertedValue = convertValueNode(fieldNode, fieldType);
              structMap.put(fieldName, convertedValue);
            });

    return new DatabricksStruct(structMap, structMetadata);
  }

  /**
   * Converts a single JsonNode into the correct Java object based on `expectedType`. If
   * `expectedType` is a complex type (ARRAY, MAP, STRUCT), we recurse.
   */
  private Object convertValueNode(JsonNode node, String expectedType) {
    if (node == null || node.isNull()) {
      return null;
    }

    if (expectedType.startsWith(DatabricksTypeUtil.ARRAY)) {
      return parseToArray(node, expectedType);
    }
    if (expectedType.startsWith(DatabricksTypeUtil.STRUCT)) {
      return parseToStruct(node, expectedType);
    }
    if (expectedType.startsWith(DatabricksTypeUtil.MAP)) {
      return parseToMap(node, expectedType);
    }

    return convertPrimitive(node.asText(), expectedType);
  }

  /**
   * Converts the given JsonNode into a Java Map<String,Object>, according to keyType and valueType.
   * - If the node is a JSON Object: each field is "key" -> value - If the node is a JSON Array:
   * assume each element has "key" and "value" fields
   */
  private Map<String, Object> convertJsonNodeToJavaMap(
      JsonNode node, String keyType, String valueType) {
    Map<String, Object> result = new LinkedHashMap<>();

    if (node.isObject()) {
      node.fields()
          .forEachRemaining(
              entry -> {
                String keyString = entry.getKey();
                JsonNode valNode = entry.getValue();

                Object typedKey = convertValueNode(objectMapper.valueToTree(keyString), keyType);
                String mapKey = (typedKey == null) ? "null" : typedKey.toString();

                Object typedVal = convertValueNode(valNode, valueType);
                result.put(mapKey, typedVal);
              });

    } else if (node.isArray()) {
      for (JsonNode element : node) {
        if (!element.has("key")) {
          throw new IllegalArgumentException(
              "Expected array element with at least 'key' field. Found: " + element);
        }

        JsonNode keyNode = element.get("key");
        Object typedKey = convertValueNode(keyNode, keyType);
        String mapKey = (typedKey == null) ? "null" : typedKey.toString();

        JsonNode valueNode = element.get("value");
        Object typedVal = null;
        if (valueNode != null && !valueNode.isNull()) {
          typedVal = convertValueNode(valueNode, valueType);
        }

        result.put(mapKey, typedVal);
      }
    } else {
      throw new IllegalArgumentException("Expected JSON object or array for a MAP. Found: " + node);
    }

    return result;
  }

  /**
   * Converts a primitive value (String -> int, String -> long, etc.) based on the type (INT,
   * BIGINT, FLOAT, DOUBLE, DECIMAL, BOOLEAN, DATE, TIMESTAMP, TIME, BINARY, STRING, etc.).
   */
  private Object convertPrimitive(String text, String type) {
    if (text == null) {
      return null;
    }
    switch (type.toUpperCase()) {
      case DatabricksTypeUtil.INT:
        return Integer.parseInt(text);
      case DatabricksTypeUtil.BIGINT:
        return Long.parseLong(text);
      case DatabricksTypeUtil.SMALLINT:
        return Short.parseShort(text);
      case DatabricksTypeUtil.FLOAT:
        return Float.parseFloat(text);
      case DatabricksTypeUtil.DOUBLE:
        return Double.parseDouble(text);
      case DatabricksTypeUtil.DECIMAL:
        return new BigDecimal(text);
      case DatabricksTypeUtil.BOOLEAN:
        return Boolean.parseBoolean(text);
      case DatabricksTypeUtil.DATE:
        return Date.valueOf(text);
      case DatabricksTypeUtil.TIMESTAMP:
        return Timestamp.valueOf(text);
      case DatabricksTypeUtil.TIME:
        return Time.valueOf(text);
      case DatabricksTypeUtil.BINARY:
        return text.getBytes();
      case DatabricksTypeUtil.STRING:
      default:
        return text;
    }
  }
}
