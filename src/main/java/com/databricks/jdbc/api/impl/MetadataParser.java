package com.databricks.jdbc.api.impl;

import java.util.LinkedHashMap;
import java.util.Map;

public class MetadataParser {

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

  public static String parseArrayMetadata(String metadata) {
    return cleanTypeName(metadata.substring("ARRAY<".length(), metadata.length() - 1).trim());
  }

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
      throw new IllegalArgumentException("Invalid MAP metadata: " + metadata);
    }

    String keyType = cleanTypeName(metadata.substring(0, splitIndex).trim());
    String valueType = cleanTypeName(metadata.substring(splitIndex + 1).trim());

    return keyType + ", " + valueType;
  }

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

  private static String cleanTypeName(String typeName) {
    return typeName.replaceAll(" NOT NULL", "").trim();
  }
}
