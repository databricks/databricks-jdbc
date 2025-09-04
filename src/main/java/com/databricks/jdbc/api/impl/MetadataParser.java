package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.LinkedHashMap;
import java.util.Map;

/** Utility class for parsing metadata descriptions into structured type mappings. */
public class MetadataParser {

  /**
   * Parses STRUCT metadata to extract field types.
   *
   * @param metadata the metadata string representing a STRUCT type
   * @return a map where each key is a field name, and the value is the field's data type
   */
  public static Map<String, String> parseStructMetadata(String metadata) {
    Map<String, String> typeMap = new LinkedHashMap<>();
    metadata = metadata.substring("STRUCT<".length(), metadata.length() - 1);
    String[] fields = splitFields(metadata);

    for (String field : fields) {
      String[] parts = field.split(":", 2);
      String fieldName = parts[0].trim();
      String fieldType = cleanTypeName(parts[1].trim());

      if (fieldType.startsWith("STRUCT")) {
        typeMap.put(fieldName, fieldType);
      } else if (fieldType.startsWith("ARRAY")) {
        typeMap.put(fieldName, "ARRAY<" + parseArrayMetadata(fieldType) + ">");
      } else if (fieldType.startsWith("MAP")) {
        typeMap.put(fieldName, "MAP<" + parseMapMetadata(fieldType) + ">");
      } else {
        typeMap.put(fieldName, fieldType);
      }
    }

    return typeMap;
  }

  /**
   * Parses ARRAY metadata to retrieve the element type.
   *
   * @param metadata the metadata string representing an ARRAY type
   * @return the element type contained within the array
   */
  public static String parseArrayMetadata(String metadata) {
    return cleanTypeName(metadata.substring("ARRAY<".length(), metadata.length() - 1).trim());
  }

  /**
   * Parses MAP metadata to retrieve key and value types.
   *
   * @param metadata the metadata string representing a MAP type
   * @return a string formatted as "keyType, valueType"
   * @throws DatabricksDriverException if the MAP metadata format is invalid
   */
  public static String parseMapMetadata(String metadata) {
    metadata = metadata.substring("MAP<".length(), metadata.length() - 1).trim();

    int depth = 0;
    int splitIndex = -1;

    for (int i = 0; i < metadata.length(); i++) {
      char ch = metadata.charAt(i);
      if (ch == '<') {
        depth++;
      } else if (ch == '>') {
        depth--;
      }

      if (ch == ',' && depth == 0) {
        splitIndex = i;
        break;
      }
    }

    if (splitIndex == -1) {
      throw new DatabricksDriverException(
          "Invalid MAP metadata: " + metadata,
          DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_MAP_CONVERSION_ERROR);
    }

    String keyType = cleanTypeName(metadata.substring(0, splitIndex).trim());
    String valueType = cleanTypeName(metadata.substring(splitIndex + 1).trim());

    return keyType + ", " + valueType;
  }

  /**
   * Splits fields in a STRUCT metadata string, accounting for nested types.
   *
   * @param metadata the STRUCT metadata string to split
   * @return an array of field definitions in the STRUCT
   */
  private static String[] splitFields(String metadata) {
    int depth = 0;
    StringBuilder currentField = new StringBuilder();
    java.util.List<String> fields = new java.util.ArrayList<>();

    for (char ch : metadata.toCharArray()) {
      if (ch == '<') {
        depth++;
      } else if (ch == '>') {
        depth--;
      }

      if (ch == ',' && depth == 0) {
        fields.add(currentField.toString().trim());
        currentField.setLength(0);
      } else {
        currentField.append(ch);
      }
    }
    fields.add(currentField.toString().trim());
    return fields.toArray(new String[0]);
  }

  /**
   * Removes any "NOT NULL" constraints and trims the type name.
   *
   * @param typeName the type name to clean
   * @return the cleaned type name without "NOT NULL" constraints
   */
  private static String cleanTypeName(String typeName) {
    return typeName.replaceAll(" NOT NULL", "").trim();
  }
}
