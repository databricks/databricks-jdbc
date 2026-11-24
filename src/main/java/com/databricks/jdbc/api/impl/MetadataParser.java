package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/** Utility class for parsing metadata descriptions into structured type mappings. */
public class MetadataParser {

  static boolean mayContainTime(String metadata) {
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
      if (index - start != 4 || !metadata.regionMatches(true, start, "TIME", 0, 4)) {
        continue;
      }
      int next = index;
      while (next < metadata.length() && Character.isWhitespace(metadata.charAt(next))) {
        next++;
      }
      if (next >= metadata.length() || metadata.charAt(next) != ':') {
        return true;
      }
    }
    return false;
  }

  static final class TimeType {
    static final TimeType NO_TIME = new TimeType();
    final String metadata;
    final boolean time;
    final boolean containsTime;
    final TimeUnit unit;
    final Map<String, TimeType> children = new LinkedHashMap<>();
    final String formattedMetadata;

    private TimeType() {
      metadata = null;
      time = false;
      containsTime = false;
      unit = null;
      formattedMetadata = null;
    }

    TimeType(String metadata, Field field) {
      this.metadata = metadata;
      this.time = DatabricksTypeUtil.isTimeType(metadata);
      this.unit =
          field != null && field.getType() instanceof ArrowType.Time
              ? ((ArrowType.Time) field.getType()).getUnit()
              : null;
      try {
        if (metadata != null && metadata.startsWith("ARRAY<")) {
          children.put("element", new TimeType(parseArrayMetadata(metadata), child(field, 0)));
        } else if (metadata != null && metadata.startsWith("MAP<")) {
          String[] types = parseMapMetadataParts(metadata);
          Field entries = child(field, 0);
          children.put("key", new TimeType(types[0], child(entries, 0)));
          children.put("value", new TimeType(types[1], child(entries, 1)));
        } else if (metadata != null && metadata.startsWith("STRUCT<")) {
          for (Map.Entry<String, String> entry : parseStructMetadata(metadata).entrySet()) {
            Field childField = null;
            if (field != null) {
              for (Field candidate : field.getChildren()) {
                if (candidate.getName().equals(entry.getKey())) {
                  childField = candidate;
                  break;
                }
              }
            }
            children.put(entry.getKey(), new TimeType(entry.getValue(), childField));
          }
        }
      } catch (RuntimeException exception) {
        children.clear();
      }
      this.containsTime = time || children.values().stream().anyMatch(child -> child.containsTime);
      if (!containsTime) {
        formattedMetadata = metadata;
      } else if (time) {
        formattedMetadata = "STRING";
      } else if (metadata.startsWith("ARRAY<")) {
        formattedMetadata = "ARRAY<" + children.get("element").formattedMetadata + ">";
      } else if (metadata.startsWith("MAP<")) {
        formattedMetadata =
            "MAP<"
                + children.get("key").formattedMetadata
                + ","
                + children.get("value").formattedMetadata
                + ">";
      } else {
        StringBuilder rewritten = new StringBuilder("STRUCT<");
        for (Map.Entry<String, TimeType> entry : children.entrySet()) {
          if (rewritten.length() > "STRUCT<".length()) {
            rewritten.append(',');
          }
          rewritten
              .append('`')
              .append(entry.getKey().replace("`", "``"))
              .append("`:")
              .append(entry.getValue().formattedMetadata);
        }
        formattedMetadata = rewritten.append('>').toString();
      }
    }

    private static Field child(Field field, int index) {
      return field != null && field.getChildren().size() > index
          ? field.getChildren().get(index)
          : null;
    }
  }

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
   * Removes any "NOT NULL" constraints and trims the type name.
   *
   * @param typeName the type name to clean
   * @return the cleaned type name without "NOT NULL" constraints
   */
  private static String cleanTypeName(String typeName) {
    return typeName.replaceAll(" NOT NULL", "").trim();
  }
}
