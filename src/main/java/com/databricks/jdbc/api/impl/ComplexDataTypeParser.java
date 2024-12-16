package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

/** Utility class for parsing complex data type objects (map, array, struct). */
public class ComplexDataTypeParser {
  private final ObjectMapper objectMapper;

  /** Constructor class for ComplexDataTypeParser. */
  public ComplexDataTypeParser() {
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Parses a JSON string into a JsonNode.
   *
   * @param json the JSON string to parse
   * @return the parsed JsonNode
   */
  public JsonNode parse(String json) {
    try {
      return objectMapper.readTree(json);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON: " + json, e);
    }
  }

  /**
   * Parses a JsonNode into a Struct representation using the provided type map.
   *
   * @param node the JsonNode to parse
   * @param typeMap the type map defining the structure of the Struct
   * @return a Map representing the Struct
   */
  public Map<String, Object> parseToStruct(JsonNode node, Map<String, String> typeMap) {
    if (!node.isObject()) {
      throw new IllegalArgumentException("Expected JSON object, but got: " + node);
    }

    Map<String, Object> structMap = new LinkedHashMap<>();
    node.fields()
        .forEachRemaining(
            entry -> {
              String fieldName = entry.getKey();
              JsonNode fieldNode = entry.getValue();

              String fieldType = typeMap.getOrDefault(fieldName, DatabricksTypeUtil.STRING);

              if (fieldType.startsWith(DatabricksTypeUtil.STRUCT)) {
                Map<String, String> nestedTypeMap = MetadataParser.parseStructMetadata(fieldType);
                structMap.put(fieldName, parseToStruct(fieldNode, nestedTypeMap));
              } else if (fieldType.startsWith(DatabricksTypeUtil.ARRAY)) {
                String nestedArrayType = MetadataParser.parseArrayMetadata(fieldType);
                structMap.put(fieldName, parseToArray(fieldNode, nestedArrayType));
              } else if (fieldType.startsWith(DatabricksTypeUtil.MAP)) {
                structMap.put(fieldName, parseToMap(fieldNode.toString(), fieldType));
              } else {
                structMap.put(fieldName, convertValueNode(fieldNode, fieldType));
              }
            });
    return structMap;
  }

  /**
   * Parses a JsonNode into an Array representation using the provided element type.
   *
   * @param node the JsonNode to parse
   * @param elementType the type of elements in the array
   * @return a List representing the Array
   */
  public List<Object> parseToArray(JsonNode node, String elementType) {
    if (!node.isArray()) {
      throw new IllegalArgumentException("Expected JSON array, but got: " + node);
    }

    List<Object> arrayList = new ArrayList<>();
    for (JsonNode element : node) {
      if (elementType.startsWith(DatabricksTypeUtil.STRUCT)) {
        Map<String, String> structTypeMap = MetadataParser.parseStructMetadata(elementType);
        arrayList.add(parseToStruct(element, structTypeMap));
      } else if (elementType.startsWith(DatabricksTypeUtil.ARRAY)) {
        String nestedArrayType = MetadataParser.parseArrayMetadata(elementType);
        arrayList.add(parseToArray(element, nestedArrayType));
      } else if (elementType.startsWith(DatabricksTypeUtil.MAP)) {
        arrayList.add(parseToMap(element.toString(), elementType));
      } else {
        arrayList.add(convertValueNode(element, elementType));
      }
    }

    return arrayList;
  }

  /**
   * Parses a JSON string into a Map representation based on the provided metadata.
   *
   * @param json the JSON string to parse
   * @param metadata the metadata defining the structure of the Map
   * @return a Map representing the parsed JSON
   */
  public Map<String, Object> parseToMap(String json, String metadata) {
    try {
      JsonNode node = objectMapper.readTree(json);
      if (node.isObject()) {
        if (metadata.startsWith("MAP")) {
          return convertToMap(node, metadata);
        }
        return parseToStruct(node, MetadataParser.parseStructMetadata(metadata));
      } else if (node.isArray()) {
        return convertArrayToMap(node, metadata);
      } else {
        throw new IllegalArgumentException(
            "Expected JSON object or array for Map, but got: " + node);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON: " + json, e);
    }
  }

  private Map<String, Object> convertToMap(JsonNode node, String metadata) {
    Map<String, Object> map = new LinkedHashMap<>();
    String[] mapMetadata = MetadataParser.parseMapMetadata(metadata).split(",", 2);
    String keyType = mapMetadata[0].trim();
    String valueType = mapMetadata[1].trim();

    node.fields()
        .forEachRemaining(
            entry -> {
              String key = entry.getKey();
              JsonNode valueNode = entry.getValue();
              Object value = convertValueNode(valueNode, valueType);
              map.put(key, value);
            });

    return map;
  }

  private Map<String, Object> convertArrayToMap(JsonNode arrayNode, String metadata) {
    Map<String, Object> map = new LinkedHashMap<>();
    String[] mapMetadata = MetadataParser.parseMapMetadata(metadata).split(",", 2);
    String keyType = mapMetadata[0].trim();
    String valueType = mapMetadata[1].trim();

    for (JsonNode element : arrayNode) {
      if (element.isObject() && element.has("key") && element.has("value")) {
        Object key = convertValueNode(element.get("key"), keyType);
        Object value = convertValueNode(element.get("value"), valueType);
        map.put(key.toString(), value);
      } else {
        throw new IllegalArgumentException(
            "Expected array elements with 'key' and 'value' fields, but got: " + element);
      }
    }

    return map;
  }

  private Object parseToJavaObject(JsonNode node) {
    if (node.isObject()) {
      return parseToStruct(node, Collections.emptyMap());
    } else if (node.isArray()) {
      return parseToArray(node, DatabricksTypeUtil.STRING);
    } else if (node.isValueNode()) {
      return convertValueNode(node, DatabricksTypeUtil.STRING);
    }
    return null;
  }

  /**
   * Converts a JsonNode value to the specified type.
   *
   * @param node the JsonNode value to convert
   * @param expectedType the expected type of the value
   * @return the converted value
   */
  private Object convertValueNode(JsonNode node, String expectedType) {
    if (node.isNull()) {
      return null;
    }

    try {
      switch (expectedType.toUpperCase()) {
        case DatabricksTypeUtil.INT:
          return node.isNumber() ? node.intValue() : Integer.parseInt(node.asText());
        case DatabricksTypeUtil.BIGINT:
          return node.isNumber() ? node.longValue() : Long.parseLong(node.asText());
        case DatabricksTypeUtil.FLOAT:
          return node.isNumber() ? node.floatValue() : Float.parseFloat(node.asText());
        case DatabricksTypeUtil.DOUBLE:
          return node.isNumber() ? node.doubleValue() : Double.parseDouble(node.asText());
        case DatabricksTypeUtil.DECIMAL:
          return new BigDecimal(node.asText());
        case DatabricksTypeUtil.BOOLEAN:
          return node.isBoolean() ? node.booleanValue() : Boolean.parseBoolean(node.asText());
        case DatabricksTypeUtil.DATE:
          return Date.valueOf(node.asText());
        case DatabricksTypeUtil.TIMESTAMP:
          return Timestamp.valueOf(node.asText());
        case DatabricksTypeUtil.STRING:
        default:
          return node.asText();
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to convert value " + node + " to type " + expectedType, e);
    }
  }
}
