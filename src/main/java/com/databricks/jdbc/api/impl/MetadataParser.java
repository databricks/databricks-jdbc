package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
      int separator = findTopLevelDelimiter(field, ':');
      if (separator < 0) {
        throw new DatabricksDriverException(
            "Invalid STRUCT field metadata: " + field,
            DatabricksDriverErrorCode.COMPLEX_DATA_TYPE_STRUCT_CONVERSION_ERROR);
      }
      String fieldName = parseFieldName(field.substring(0, separator));
      String fieldType = cleanTypeName(field.substring(separator + 1).trim());

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
    String[] keyValueTypes = parseMapMetadataParts(metadata);
    return keyValueTypes[0] + ", " + keyValueTypes[1];
  }

  /** Returns whether a type contains a GEOMETRY or GEOGRAPHY leaf at any nesting depth. */
  public static boolean containsGeospatialType(String metadata) {
    if (metadata == null) {
      return false;
    }

    boolean quoted = false;
    for (int index = 0; index < metadata.length(); ) {
      char ch = metadata.charAt(index);
      if (ch == '`') {
        if (quoted && index + 1 < metadata.length() && metadata.charAt(index + 1) == '`') {
          index += 2;
          continue;
        }
        quoted = !quoted;
        index++;
        continue;
      }
      if (quoted || !isIdentifierCharacter(ch)) {
        index++;
        continue;
      }

      int start = index;
      while (index < metadata.length() && isIdentifierCharacter(metadata.charAt(index))) {
        index++;
      }
      int length = index - start;
      boolean isGeometry =
          length == "GEOMETRY".length()
              && metadata.regionMatches(true, start, "GEOMETRY", 0, length);
      boolean isGeography =
          length == "GEOGRAPHY".length()
              && metadata.regionMatches(true, start, "GEOGRAPHY", 0, length);
      if (!isGeometry && !isGeography) {
        continue;
      }

      int next = index;
      while (next < metadata.length() && Character.isWhitespace(metadata.charAt(next))) {
        next++;
      }
      // An unquoted struct field may itself be named geometry/geography.
      if (next >= metadata.length() || metadata.charAt(next) != ':') {
        return true;
      }
    }
    return false;
  }

  /** Parses MAP metadata into its key and value types without flattening nested type syntax. */
  static String[] parseMapMetadataParts(String metadata) {
    metadata = metadata.substring("MAP<".length(), metadata.length() - 1).trim();

    int angleBracketDepth = 0;
    int parenDepth = 0;
    int splitIndex = -1;

    for (int i = 0; i < metadata.length(); i++) {
      char ch = metadata.charAt(i);
      if (ch == '<') {
        angleBracketDepth++;
      } else if (ch == '>') {
        angleBracketDepth--;
      } else if (ch == '(') {
        parenDepth++;
      } else if (ch == ')') {
        parenDepth--;
      }

      if (ch == ',' && angleBracketDepth == 0 && parenDepth == 0) {
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

    return new String[] {keyType, valueType};
  }

  /**
   * Splits fields in a STRUCT metadata string, accounting for nested types.
   *
   * @param metadata the STRUCT metadata string to split
   * @return an array of field definitions in the STRUCT
   */
  private static String[] splitFields(String metadata) {
    int angleBracketDepth = 0;
    int parenDepth = 0;
    boolean quoted = false;
    StringBuilder currentField = new StringBuilder();
    java.util.List<String> fields = new java.util.ArrayList<>();

    for (int index = 0; index < metadata.length(); index++) {
      char ch = metadata.charAt(index);
      if (ch == '`') {
        currentField.append(ch);
        if (quoted && index + 1 < metadata.length() && metadata.charAt(index + 1) == '`') {
          currentField.append('`');
          index++;
        } else {
          quoted = !quoted;
        }
        continue;
      }
      if (!quoted && ch == '<') {
        angleBracketDepth++;
      } else if (!quoted && ch == '>') {
        angleBracketDepth--;
      } else if (!quoted && ch == '(') {
        parenDepth++;
      } else if (!quoted && ch == ')') {
        parenDepth--;
      }

      // Only split on commas when we're at the top level (both depths are 0)
      if (!quoted && ch == ',' && angleBracketDepth == 0 && parenDepth == 0) {
        String field = currentField.toString().trim();
        fields.add(field);
        currentField.setLength(0);
      } else {
        currentField.append(ch);
      }
    }
    String finalField = currentField.toString().trim();
    fields.add(finalField);
    return fields.toArray(new String[0]);
  }

  private static int findTopLevelDelimiter(String value, char delimiter) {
    int angleBracketDepth = 0;
    int parenDepth = 0;
    boolean quoted = false;
    for (int index = 0; index < value.length(); index++) {
      char ch = value.charAt(index);
      if (ch == '`') {
        if (quoted && index + 1 < value.length() && value.charAt(index + 1) == '`') {
          index++;
        } else {
          quoted = !quoted;
        }
        continue;
      }
      if (quoted) {
        continue;
      }
      if (ch == '<') {
        angleBracketDepth++;
      } else if (ch == '>') {
        angleBracketDepth--;
      } else if (ch == '(') {
        parenDepth++;
      } else if (ch == ')') {
        parenDepth--;
      } else if (ch == delimiter && angleBracketDepth == 0 && parenDepth == 0) {
        return index;
      }
    }
    return -1;
  }

  private static String parseFieldName(String fieldName) {
    String trimmed = fieldName.trim();
    if (trimmed.length() >= 2
        && trimmed.charAt(0) == '`'
        && trimmed.charAt(trimmed.length() - 1) == '`') {
      return trimmed.substring(1, trimmed.length() - 1).replace("``", "`");
    }
    return trimmed;
  }

  private static boolean isIdentifierCharacter(char value) {
    return Character.isLetterOrDigit(value) || value == '_';
  }

  /**
   * Removes any "NOT NULL" constraints (case-insensitively) and trims the type name. Text inside
   * quoted literals (e.g. {@code COMMENT 'value is not null'}) or backtick-quoted identifiers is
   * left untouched.
   *
   * @param typeName the type name to clean
   * @return the cleaned type name without "NOT NULL" constraints
   */
  private static String cleanTypeName(String typeName) {
    Matcher matcher = NOT_NULL_PATTERN.matcher(typeName);
    StringBuilder result = new StringBuilder();
    while (matcher.find()) {
      String quoted = matcher.group(1);
      matcher.appendReplacement(result, quoted == null ? "" : Matcher.quoteReplacement(quoted));
    }
    matcher.appendTail(result);
    return result.toString().trim();
  }

  /**
   * Matches either a quoted segment (group 1: single-quoted, double-quoted, or backtick-quoted),
   * which is preserved, or an unquoted NOT NULL constraint, which is removed.
   */
  private static final Pattern NOT_NULL_PATTERN =
      Pattern.compile(
          "('(?:[^'\\\\]|\\\\.)*'|\"(?:[^\"\\\\]|\\\\.)*\"|`(?:[^`]|``)*`)|\\s+NOT\\s+NULL\\b",
          Pattern.CASE_INSENSITIVE);
}
