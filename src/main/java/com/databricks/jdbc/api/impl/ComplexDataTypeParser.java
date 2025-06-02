package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ComplexDataTypeParser {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ComplexDataTypeParser.class);

  public DatabricksArray parseJsonStringToDbArray(String json, String arrayMetadata)
      throws DatabricksParsingException {
    try {
      JsonNode node = JsonUtil.getMapper().readTree(json);
      return parseToArray(node, arrayMetadata);
    } catch (IOException e) {
      throw new DatabricksParsingException(
          "Failed to parse JSON array from: " + json, DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
  }

  public DatabricksMap<String, Object> parseJsonStringToDbMap(String json, String mapMetadata)
      throws DatabricksParsingException {
    try {
      JsonNode node = JsonUtil.getMapper().readTree(json);
      return parseToMap(node, mapMetadata);
    } catch (IOException e) {
      throw new DatabricksParsingException(
          "Failed to parse JSON map from: " + json, DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
  }

  public DatabricksStruct parseJsonStringToDbStruct(String json, String structMetadata)
      throws DatabricksParsingException {
    try {
      JsonNode node = JsonUtil.getMapper().readTree(json);
      return parseToStruct(node, structMetadata);
    } catch (IOException e) {
      throw new DatabricksParsingException(
          "Failed to parse JSON struct from: " + json,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
  }

  public DatabricksArray parseToArray(JsonNode node, String arrayMetadata)
      throws DatabricksParsingException {
    if (!node.isArray()) {
      throw new DatabricksParsingException(
          "Unexpected metadata format. Type is not a ARRAY: " + arrayMetadata,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
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

  public DatabricksMap<String, Object> parseToMap(JsonNode node, String mapMetadata)
      throws DatabricksParsingException {
    if (!mapMetadata.startsWith(DatabricksTypeUtil.MAP)) {
      throw new DatabricksParsingException(
          "Unexpected metadata format. Type is not a MAP: " + mapMetadata,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
    LOGGER.debug("Parsing map with metadata: {}", mapMetadata);
    String[] kv = MetadataParser.parseMapMetadata(mapMetadata).split(",", 2);
    String keyType = kv[0].trim();
    String valueType = kv[1].trim();
    Map<String, Object> rawMap = convertJsonNodeToJavaMap(node, keyType, valueType);
    return new DatabricksMap<>(rawMap, mapMetadata);
  }

  public DatabricksStruct parseToStruct(JsonNode node, String structMetadata)
      throws DatabricksParsingException {
    if (!node.isObject()) {
      throw new DatabricksParsingException(
          "Unexpected metadata format. Type is not a STRUCT: " + structMetadata,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
    LOGGER.debug("Parsing struct with metadata: {}", structMetadata);
    Map<String, String> fieldTypeMap = MetadataParser.parseStructMetadata(structMetadata);
    Map<String, Object> structMap = new LinkedHashMap<>();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String fieldName = entry.getKey();
      JsonNode fieldNode = entry.getValue();
      String fieldType = fieldTypeMap.getOrDefault(fieldName, DatabricksTypeUtil.STRING);
      Object convertedValue = convertValueNode(fieldNode, fieldType);
      structMap.put(fieldName, convertedValue);
    }
    return new DatabricksStruct(structMap, structMetadata);
  }

  private Object convertValueNode(JsonNode node, String expectedType)
      throws DatabricksParsingException {
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

  private Map<String, Object> convertJsonNodeToJavaMap(
      JsonNode node, String keyType, String valueType) throws DatabricksParsingException {
    Map<String, Object> result = new LinkedHashMap<>();
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
      while (iter.hasNext()) {
        Map.Entry<String, JsonNode> entry = iter.next();
        String keyString = entry.getKey();
        JsonNode valNode = entry.getValue();
        Object typedKey = convertValueNode(JsonUtil.getMapper().valueToTree(keyString), keyType);
        String mapKey = (typedKey == null) ? "null" : typedKey.toString();
        Object typedVal = convertValueNode(valNode, valueType);
        result.put(mapKey, typedVal);
      }
    } else if (node.isArray()) {
      for (JsonNode element : node) {
        if (!element.has("key")) {
          throw new DatabricksParsingException(
              "Expected array element with at least 'key' field. Found: " + element,
              DatabricksDriverErrorCode.JSON_PARSING_ERROR);
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
      throw new DatabricksParsingException(
          "Expected JSON object or array for a MAP. Found: " + node,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
    return result;
  }

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

  /**
   * Formats a complex type JSON string into a consistent string format. This is primarily used when
   * complex datatype support is disabled.
   *
   * @param jsonString The JSON string representation of the complex type
   * @param complexType The type of complex data (MAP, ARRAY, STRUCT)
   * @param typeMetadata The metadata for the type (e.g., "MAP<INT,INT>")
   * @return A consistently formatted string representation
   */
  public String formatComplexTypeString(
      String jsonString, String complexType, String typeMetadata) {
    if (jsonString == null || complexType == null) {
      return jsonString;
    }

    try {
      if (complexType.equals(DatabricksTypeUtil.MAP)) {
        return formatMapString(jsonString, typeMetadata);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to format complex type representation: {}", e.getMessage());
    }

    return jsonString;
  }

  /**
   * Formats a map JSON string into the standard {key:value} format.
   *
   * @param jsonString The JSON string representation of the map
   * @param mapMetadata The metadata for the map type (e.g., "MAP<INT,INT>")
   * @return A map string in the format {key:value,key:value}
   */
  public String formatMapString(String jsonString, String mapMetadata) {
    try {
      JsonNode node = JsonUtil.getMapper().readTree(jsonString);
      if (node.isArray() && node.size() > 0 && node.get(0).has("key")) {
        String[] kv = new String[] {"STRING", "STRING"};
        if (mapMetadata != null && mapMetadata.startsWith(DatabricksTypeUtil.MAP)) {
          kv = MetadataParser.parseMapMetadata(mapMetadata).split(",", 2);
        }

        String keyType = kv[0].trim();
        String valueType = kv[1].trim();
        boolean isStringKey = keyType.equalsIgnoreCase(DatabricksTypeUtil.STRING);
        boolean isStringValue = valueType.equalsIgnoreCase(DatabricksTypeUtil.STRING);

        StringBuilder result = new StringBuilder("{");

        for (int i = 0; i < node.size(); i++) {
          JsonNode entry = node.get(i);
          JsonNode keyNode = entry.get("key");
          JsonNode valueNode = entry.get("value");

          // Throw error if keyNode is null
          if (keyNode == null || keyNode.isNull()) {
            throw new DatabricksParsingException(
                "Map entry found with null key in JSON: " + entry.toString(),
                DatabricksDriverErrorCode.JSON_PARSING_ERROR);
          }

          if (i > 0) {
            result.append(",");
          }

          if (isStringKey) {
            result.append("\"").append(keyNode.asText()).append("\"");
          } else {
            result.append(keyNode.asText());
          }

          result.append(":");

          // Handle null valueNode
          if (valueNode == null || valueNode.isNull()) {
            result.append("null");
          } else if (isStringValue) {
            result.append("\"").append(valueNode.asText()).append("\"");
          } else {
            result.append(valueNode.asText());
          }
        }

        result.append("}");
        return result.toString();
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to format map representation: {}", e.getMessage());
    }

    return jsonString;
  }
}
