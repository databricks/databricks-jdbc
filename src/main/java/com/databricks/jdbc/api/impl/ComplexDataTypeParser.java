package com.databricks.jdbc.api.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

public class ComplexDataTypeParser {
  private final ObjectMapper objectMapper;

  public ComplexDataTypeParser() {
    this.objectMapper = new ObjectMapper();
  }

  public JsonNode parse(String json) {
    try {
      return objectMapper.readTree(json);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse JSON: " + json, e);
    }
  }

  public Map<String, Object> parseToStruct(JsonNode node, Map<String, String> typeMap) {
    if (!node.isObject()) {
      throw new IllegalArgumentException("Expected JSON object, but got: " + node);
    }

    Map<String, Object> structMap = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();

    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String fieldName = entry.getKey();
      JsonNode fieldNode = entry.getValue();

      String fieldType = typeMap.getOrDefault(fieldName, "STRING");

      if (fieldType.startsWith("STRUCT")) {
        Map<String, String> nestedTypeMap = MetadataParser.parseStructMetadata(fieldType);
        structMap.put(fieldName, parseToStruct(fieldNode, nestedTypeMap));
      } else if (fieldType.startsWith("ARRAY")) {
        String nestedArrayType = MetadataParser.parseArrayMetadata(fieldType);
        structMap.put(fieldName, parseToArray(fieldNode, nestedArrayType));
      } else if (fieldType.startsWith("MAP")) {
        structMap.put(fieldName, parseToMap(fieldNode.toString(), fieldType));
      } else {
        structMap.put(fieldName, convertValueNode(fieldNode, fieldType));
      }
    }

    return structMap;
  }

  public List<Object> parseToArray(JsonNode node, String elementType) {
    if (!node.isArray()) {
      throw new IllegalArgumentException("Expected JSON array, but got: " + node);
    }

    List<Object> arrayList = new ArrayList<>();

    for (JsonNode element : node) {
      if (elementType.startsWith("STRUCT")) {
        Map<String, String> structTypeMap = MetadataParser.parseStructMetadata(elementType);
        arrayList.add(parseToStruct(element, structTypeMap));
      } else if (elementType.startsWith("ARRAY")) {
        String nestedArrayType = MetadataParser.parseArrayMetadata(elementType);
        arrayList.add(parseToArray(element, nestedArrayType));
      } else if (elementType.startsWith("MAP")) {
        arrayList.add(parseToMap(element.toString(), elementType));
      } else {
        arrayList.add(convertValueNode(element, elementType));
      }
    }

    return arrayList;
  }

  public Map<String, Object> parseToMap(String json, String metadata) {
    try {
      JsonNode node = objectMapper.readTree(json);
      if (node.isObject()) {
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
      return parseToArray(node, "STRING");
    } else if (node.isValueNode()) {
      return convertValueNode(node, "STRING");
    }
    return null;
  }

  private Object convertValueNode(JsonNode node, String expectedType) {
    if (node.isNull()) {
      return null;
    }

    switch (expectedType.toUpperCase()) {
      case "INT":
      case "INTEGER":
        return node.isNumber() ? node.intValue() : Integer.parseInt(node.asText());
      case "BIGINT":
        return node.isNumber() ? node.longValue() : Long.parseLong(node.asText());
      case "FLOAT":
        return node.isNumber() ? node.floatValue() : Float.parseFloat(node.asText());
      case "DOUBLE":
        return node.isNumber() ? node.doubleValue() : Double.parseDouble(node.asText());
      case "DECIMAL":
        return new BigDecimal(node.asText());
      case "BOOLEAN":
        return node.isBoolean() ? node.booleanValue() : Boolean.parseBoolean(node.asText());
      case "DATE":
        return Date.valueOf(node.asText());
      case "TIMESTAMP":
        return Timestamp.valueOf(node.asText());
      case "STRING":
      default:
        return node.asText();
    }
  }
}
