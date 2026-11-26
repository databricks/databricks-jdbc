package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.IDatabricksGeospatial;
import com.databricks.jdbc.api.impl.converters.GeospatialConverter;
import com.databricks.jdbc.api.impl.converters.JavaTimeConverter;
import com.databricks.jdbc.api.impl.converters.TimestampConverter;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.common.util.WildcardUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ComplexDataTypeParser {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ComplexDataTypeParser.class);
  private static final TimestampConverter TIMESTAMP_CONVERTER = new TimestampConverter();
  private static final GeospatialConverter GEOSPATIAL_CONVERTER = new GeospatialConverter();
  private static final ObjectWriter PLAIN_DECIMAL_WRITER =
      JsonUtil.getMapper().writer().with(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
  private final boolean geoSpatialSupportEnabled;
  private final ZoneId timestampZone;

  public ComplexDataTypeParser() {
    this(true);
  }

  public ComplexDataTypeParser(boolean geoSpatialSupportEnabled) {
    this(geoSpatialSupportEnabled, null);
  }

  /**
   * Creates a parser for values serialized from Arrow.
   *
   * @param geoSpatialSupportEnabled whether geospatial values are returned as geospatial objects
   * @param timestampZone the time zone of the nested Arrow TIMESTAMP fields, which is the session
   *     time zone, or null if unknown. The {@code java.time} values of nested TIMESTAMP values are
   *     reported in it (see {@link #inTimestampZone}); {@code java.sql} values are unchanged.
   */
  public ComplexDataTypeParser(boolean geoSpatialSupportEnabled, ZoneId timestampZone) {
    this.geoSpatialSupportEnabled = geoSpatialSupportEnabled;
    this.timestampZone = timestampZone;
  }

  public DatabricksArray parseJsonStringToDbArray(String json, String arrayMetadata)
      throws DatabricksParsingException {
    try {
      JsonNode node = readDecimalJson(json, arrayMetadata);
      return parseToArray(node, arrayMetadata);
    } catch (IOException | NumberFormatException | ArithmeticException e) {
      throw new DatabricksParsingException(
          "Failed to parse JSON array from: " + json,
          e,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR.name(),
          DatabricksDriverErrorCode.JSON_PARSING_ERROR.getCode());
    }
  }

  public DatabricksMap<String, Object> parseJsonStringToDbMap(String json, String mapMetadata)
      throws DatabricksParsingException {
    try {
      JsonNode node = readDecimalJson(json, mapMetadata);
      return parseToMap(node, mapMetadata);
    } catch (IOException | NumberFormatException | ArithmeticException e) {
      throw new DatabricksParsingException(
          "Failed to parse JSON map from: " + json,
          e,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR.name(),
          DatabricksDriverErrorCode.JSON_PARSING_ERROR.getCode());
    }
  }

  public DatabricksStruct parseJsonStringToDbStruct(String json, String structMetadata)
      throws DatabricksParsingException {
    try {
      JsonNode node = readDecimalJson(json, structMetadata);
      return parseToStruct(node, structMetadata);
    } catch (IOException | NumberFormatException | ArithmeticException e) {
      throw new DatabricksParsingException(
          "Failed to parse JSON struct from: " + json,
          e,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR.name(),
          DatabricksDriverErrorCode.JSON_PARSING_ERROR.getCode());
    }
  }

  public DatabricksArray parseToArray(JsonNode node, String arrayMetadata)
      throws DatabricksParsingException {
    if (node == null || !node.isArray()) {
      throw new DatabricksParsingException(
          "Unexpected metadata format. Type is not a ARRAY: " + arrayMetadata,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
    LOGGER.debug("Parsing array with metadata: {}", arrayMetadata);
    ComplexTypePlan elementPlan = ComplexTypePlan.of(arrayMetadata).element;
    String elementType = elementPlan.type;
    List<Object> list = new ArrayList<>();
    // Keep the JSON leaves of DATE/TIMESTAMP elements; their exact java.time values are built
    // only if requested (see toExactTemporal).
    List<Object> temporalList =
        JavaTimeConverter.isTemporalTypeName(elementType) ? new ArrayList<>() : null;
    for (JsonNode elementNode : node) {
      Object converted = convertValueNode(elementNode, elementPlan);
      list.add(converted);
      if (temporalList != null) {
        temporalList.add(elementNode);
      }
    }
    return new DatabricksArray(list, arrayMetadata, temporalList, timestampZone);
  }

  public DatabricksMap<String, Object> parseToMap(JsonNode node, String mapMetadata)
      throws DatabricksParsingException {
    if (!mapMetadata.startsWith(DatabricksTypeUtil.MAP)) {
      throw new DatabricksParsingException(
          "Unexpected metadata format. Type is not a MAP: " + mapMetadata,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
    LOGGER.debug("Parsing map with metadata: {}", mapMetadata);
    ComplexTypePlan plan = ComplexTypePlan.of(mapMetadata);
    Map<String, Object> rawMap = convertJsonNodeToJavaMap(node, plan.key, plan.value);
    return new DatabricksMap<>(rawMap, mapMetadata);
  }

  public DatabricksStruct parseToStruct(JsonNode node, String structMetadata)
      throws DatabricksParsingException {
    if (node == null || !node.isObject()) {
      throw new DatabricksParsingException(
          "Unexpected metadata format. Type is not a STRUCT: " + structMetadata,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
    LOGGER.debug("Parsing struct with metadata: {}", structMetadata);
    ComplexTypePlan plan = ComplexTypePlan.of(structMetadata);
    Map<String, Object> structMap = new LinkedHashMap<>();
    // JSON leaves of DATE/TIMESTAMP fields, created only for structs that have one.
    Map<String, Object> temporalMap = null;
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String fieldName = entry.getKey();
      JsonNode fieldNode = entry.getValue();
      ComplexTypePlan fieldPlan = plan.fields.getOrDefault(fieldName, ComplexTypePlan.STRING);
      String fieldType = fieldPlan.type;
      Object convertedValue = convertValueNode(fieldNode, fieldPlan);
      structMap.put(fieldName, convertedValue);
      if (!fieldNode.isNull() && JavaTimeConverter.isTemporalTypeName(fieldType)) {
        if (temporalMap == null) {
          temporalMap = new HashMap<>();
        }
        temporalMap.put(fieldName, fieldNode);
      }
    }
    return new DatabricksStruct(structMap, structMetadata, temporalMap, timestampZone);
  }

  /**
   * Returns the exact {@code java.time} value of a nested DATE, TIMESTAMP or TIMESTAMP_NTZ leaf
   * kept by {@link #parseToArray} or {@link #parseToStruct}. A JSON leaf is converted now, so the
   * cost is paid only when a {@code java.time} value is requested; other values are returned as is.
   *
   * <p>A leaf in a form only the {@code java.sql} parser accepts (not ISO text, epoch days or
   * microseconds, or TIMESTAMP_NTZ components) has no exact value. The caller then uses the {@code
   * java.sql} value, which is exact after the 1582 Gregorian cutover, and logs {@link
   * #warnInexactNestedValue} once.
   *
   * @param source a JSON leaf, a {@code java.time} value, or null
   * @param expectedType the leaf's type name, e.g. {@code DATE}
   * @return the exact value, or null if there is none and the {@code java.sql} value should be used
   */
  static Object toExactTemporal(Object source, String expectedType) {
    return source instanceof JsonNode
        ? convertTemporalNode((JsonNode) source, expectedType)
        : source;
  }

  /**
   * Returns an exact TIMESTAMP value in {@code timestampZone}, so that its {@code LocalDate} and
   * {@code LocalDateTime} are the session-local ones, as for a top-level TIMESTAMP column. The
   * instant is unchanged. Other values, or a null zone, return {@code exact} as is.
   *
   * <p>Only Arrow results have a zone, and Arrow serializes nested TIMESTAMP values as epoch
   * microseconds, which {@link #toExactTemporal} reads at UTC; text keeps its rendered offset.
   */
  static Object inTimestampZone(Object exact, ZoneId timestampZone) {
    return timestampZone != null && exact instanceof OffsetDateTime
        ? ((OffsetDateTime) exact).atZoneSameInstant(timestampZone).toOffsetDateTime()
        : exact;
  }

  /** Returns true if {@code source} is a non-null leaf that {@link #toExactTemporal} parses. */
  static boolean isUnparsedLeaf(Object source) {
    return source instanceof JsonNode && !((JsonNode) source).isNull();
  }

  /** Logs that a nested value fell back to its {@code java.sql} value, without the value. */
  static void warnInexactNestedValue(String expectedType) {
    LOGGER.warn(
        "Could not read a nested {} value exactly; using its java.sql value, which is shifted for"
            + " dates before 1582-10-15",
        expectedType);
  }

  /**
   * Returns the exact {@code java.time} value of a nested DATE, TIMESTAMP or TIMESTAMP_NTZ leaf, or
   * null if it cannot be read exactly; callers then fall back to the {@code java.sql} value.
   *
   * <p>Arrow serializes these as epoch days, epoch microseconds, or TIMESTAMP_NTZ components, which
   * {@link #convertPrimitive} turns into {@code java.sql} values that cannot represent BC or
   * pre-1582 values. TIMESTAMP epoch microseconds are read at UTC, matching the {@code
   * java.sql.Timestamp} built from them; {@link #inTimestampZone} moves them to the session time
   * zone for {@code java.time} results. Text keeps its rendered fields and offset, as at the top
   * level.
   */
  private static Temporal convertTemporalNode(JsonNode node, String expectedType) {
    if (node == null || node.isNull()) {
      return null;
    }
    String baseType = expectedType.trim().toUpperCase(Locale.ROOT);
    boolean isTimestampNtz = baseType.equals(DatabricksTypeUtil.TIMESTAMP_NTZ);
    try {
      if (node.isArray()) {
        return isTimestampNtz && node.size() >= 5 ? timestampNtzFromComponents(node) : null;
      }
      if (node.isIntegralNumber()) {
        return fromEpochNumber(node.longValue(), baseType);
      }
      String text = node.asText().trim();
      if (baseType.equals(DatabricksTypeUtil.DATE)) {
        LocalDate date = JavaTimeConverter.parseDateText(text);
        return date != null ? date : fromEpochNumber(Long.parseLong(text), baseType);
      }
      Temporal temporal = JavaTimeConverter.toTemporal(Types.TIMESTAMP, baseType, text);
      return temporal != null ? temporal : fromEpochNumber(Long.parseLong(text), baseType);
    } catch (NumberFormatException | DateTimeException e) {
      return null;
    }
  }

  /** Epoch days (DATE) or epoch microseconds (TIMESTAMP, TIMESTAMP_NTZ) as {@code java.time}. */
  private static Temporal fromEpochNumber(long number, String baseType) {
    if (baseType.equals(DatabricksTypeUtil.DATE)) {
      return LocalDate.ofEpochDay(number);
    }
    Instant instant =
        Instant.ofEpochSecond(
            Math.floorDiv(number, 1_000_000L), Math.floorMod(number, 1_000_000L) * 1_000);
    return baseType.equals(DatabricksTypeUtil.TIMESTAMP_NTZ)
        ? LocalDateTime.ofInstant(instant, ZoneOffset.UTC)
        : instant.atOffset(ZoneOffset.UTC);
  }

  /**
   * Reads a TIMESTAMP_NTZ that Arrow serializes as a JSON array of components [year, month, day,
   * hour, minute, second, nanosecond]. Trailing zero components are omitted, so a value with zero
   * seconds and nanoseconds has five elements.
   */
  private static LocalDateTime timestampNtzFromComponents(JsonNode arrayNode) {
    int second = arrayNode.size() > 5 ? arrayNode.get(5).asInt() : 0;
    int nano = arrayNode.size() > 6 && arrayNode.get(6) != null ? arrayNode.get(6).asInt(0) : 0;
    return LocalDateTime.of(
        arrayNode.get(0).asInt(),
        arrayNode.get(1).asInt(),
        arrayNode.get(2).asInt(),
        arrayNode.get(3).asInt(),
        arrayNode.get(4).asInt(),
        second,
        nano);
  }

  private Object convertValueNode(JsonNode node, ComplexTypePlan plan)
      throws DatabricksParsingException {
    if (node == null || node.isNull()) {
      return null;
    }
    if (plan.decimal) {
      return plan.decimalValue(node.isNumber() ? node.decimalValue() : node.asText());
    }
    String expectedType = plan.type;
    if (expectedType.startsWith(DatabricksTypeUtil.ARRAY)) {
      return parseToArray(node, expectedType);
    }
    if (expectedType.startsWith(DatabricksTypeUtil.STRUCT)) {
      return parseToStruct(node, expectedType);
    }
    if (expectedType.startsWith(DatabricksTypeUtil.MAP)) {
      return parseToMap(node, expectedType);
    }
    if (DatabricksTypeUtil.isGeospatialType(expectedType.toUpperCase(Locale.ROOT))) {
      return convertGeospatial(node, expectedType);
    }
    if (expectedType.equalsIgnoreCase(DatabricksTypeUtil.VARIANT)) {
      // For VARIANT, the node contains escaped JSON string, we need to unescape it
      // node.asText() gives us the content: "{\"nestedKey\":\"nestedValue\"}"
      // We want to return: {"nestedKey":"nestedValue"}
      String jsonText = node.asText();
      try {
        // Parse and re-serialize to unescape the JSON
        return JsonUtil.getMapper().readTree(jsonText);
      } catch (Exception e) {
        // If parsing fails, return the original text
        LOGGER.error(e, "Failed to parse VARIANT: {}", jsonText);
        return jsonText;
      }
    }
    // Arrow serializes TIMESTAMP_NTZ inside nested types as a JSON array of components:
    // [year, month, day, hour, minute, second] (and optionally nanoseconds as a 7th element).
    // Trailing zero seconds are omitted, so a value at :00 seconds has five elements.
    // e.g., [{"event_ts_ntz":[2023,10,5,15,20,30]}]
    // We must handle this before calling node.asText(), which returns "" for array nodes.
    if (node.isArray() && expectedType.equalsIgnoreCase(DatabricksTypeUtil.TIMESTAMP_NTZ)) {
      return convertTimestampNtzArray(node);
    }
    return convertPrimitive(node.asText(), expectedType);
  }

  private Object convertGeospatial(JsonNode node, String expectedType)
      throws DatabricksParsingException {
    String normalizedType = expectedType.toUpperCase(Locale.ROOT);
    try {
      IDatabricksGeospatial value;
      if (node.isObject()) {
        JsonNode sridNode = node.get("srid");
        JsonNode wkbNode = node.get("wkb");
        if (sridNode == null
            || !sridNode.isIntegralNumber()
            || !sridNode.canConvertToInt()
            || wkbNode == null
            || !wkbNode.isTextual()) {
          throw new IllegalArgumentException(
              "expected an object with int32 srid and base64 wkb fields");
        }
        Map<String, Object> nativeValue = new LinkedHashMap<>();
        nativeValue.put("srid", sridNode.intValue());
        nativeValue.put("wkb", Base64.getDecoder().decode(wkbNode.textValue()));
        value =
            normalizedType.startsWith(DatabricksTypeUtil.GEOMETRY)
                ? GEOSPATIAL_CONVERTER.toDatabricksGeometry(nativeValue)
                : GEOSPATIAL_CONVERTER.toDatabricksGeography(nativeValue);
      } else if (node.isTextual()) {
        value =
            normalizedType.startsWith(DatabricksTypeUtil.GEOMETRY)
                ? GEOSPATIAL_CONVERTER.toDatabricksGeometry(node.textValue())
                : GEOSPATIAL_CONVERTER.toDatabricksGeography(node.textValue());
      } else {
        throw new IllegalArgumentException("expected a native Arrow object or an EWKT string");
      }
      return geoSpatialSupportEnabled ? value : GeospatialConverter.formatStringFallback(value);
    } catch (DatabricksSQLException | IllegalArgumentException e) {
      String message = String.format("Failed to parse nested %s value", expectedType);
      throw new DatabricksParsingException(
          message,
          e,
          DatabricksDriverErrorCode.INVALID_STATE.name(),
          DatabricksDriverErrorCode.INVALID_STATE.getCode());
    }
  }

  private Map<String, Object> convertJsonNodeToJavaMap(
      JsonNode node, ComplexTypePlan keyType, ComplexTypePlan valueType)
      throws DatabricksParsingException {
    Map<String, Object> result = new LinkedHashMap<>();
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
      while (iter.hasNext()) {
        Map.Entry<String, JsonNode> entry = iter.next();
        String keyString = entry.getKey();
        JsonNode valNode = entry.getValue();
        Object typedKey = convertValueNode(JsonUtil.getMapper().valueToTree(keyString), keyType);
        String mapKey = mapKeyText(typedKey);
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
        String mapKey = mapKeyText(typedKey);
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

  private static String mapKeyText(Object value) {
    return value instanceof BigDecimal
        ? ((BigDecimal) value).toPlainString()
        : String.valueOf(value);
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
        try {
          return Date.valueOf(text);
        } catch (IllegalArgumentException e) {
          // Arrow serializes DATE fields in nested types as epoch day integers.
          // Fall back to parsing as epoch day count (days since 1970-01-01).
          try {
            return Date.valueOf(LocalDate.ofEpochDay(Long.parseLong(text)));
          } catch (NumberFormatException | DateTimeException nfe) {
            LOGGER.error(e, "Failed to parse DATE value '{}' as epoch day integer", text);
            throw e;
          }
        }
      case DatabricksTypeUtil.TIMESTAMP:
      case DatabricksTypeUtil.TIMESTAMP_NTZ:
        try {
          return parseTimestamp(text);
        } catch (IllegalArgumentException e) {
          // Arrow serializes TIMESTAMP/TIMESTAMP_NTZ inside nested types as epoch microseconds.
          // e.g., {"ts":1696519230000000} for 2023-10-05 15:20:30 UTC
          try {
            long micros = Long.parseLong(text);
            long seconds = Math.floorDiv(micros, 1_000_000L);
            long microsRemainder = Math.floorMod(micros, 1_000_000L);
            Instant instant = Instant.ofEpochSecond(seconds, microsRemainder * 1_000);
            // Build from the UTC wall-clock; Timestamp.from(instant) gets re-rendered in the JVM
            // default timezone, shifting nested TIMESTAMP fields (ES-1978662).
            return Timestamp.valueOf(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
          } catch (NumberFormatException nfe) {
            LOGGER.error(e, "Failed to parse TIMESTAMP value '{}' as epoch microseconds", text);
            throw e;
          }
        }
      case DatabricksTypeUtil.TIME:
        return Time.valueOf(text);
      case DatabricksTypeUtil.BINARY:
        // Arrow serializes BINARY inside nested types as base64-encoded strings.
        // e.g., {"bin_data":"QUJD"} for CAST('ABC' AS BINARY)
        try {
          return Base64.getDecoder().decode(text);
        } catch (IllegalArgumentException e) {
          // Not base64 encoded, fall back to raw bytes
          return text.getBytes(StandardCharsets.UTF_8);
        }
      case DatabricksTypeUtil.STRING:
      default:
        return text;
    }
  }

  /**
   * Converts a TIMESTAMP_NTZ value serialized as a JSON array of components
   * [year,month,day,hour,minute(,second(,nanosecond))] into a {@link Timestamp}.
   */
  private Timestamp convertTimestampNtzArray(JsonNode arrayNode) throws DatabricksParsingException {
    if (arrayNode == null || !arrayNode.isArray() || arrayNode.size() < 5) {
      throw new DatabricksParsingException(
          "Invalid TIMESTAMP_NTZ array representation: expected at least 5 elements "
              + "[year,month,day,hour,minute], but got: "
              + arrayNode,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
    return Timestamp.valueOf(timestampNtzFromComponents(arrayNode));
  }

  private Timestamp parseTimestamp(String text) {
    if (WildcardUtil.isNullOrEmpty(text)) {
      return null;
    }

    try {
      return TIMESTAMP_CONVERTER.toTimestamp(text);
    } catch (DatabricksSQLException e) {
      // Convert to IllegalArgumentException to maintain existing API contract
      throw new IllegalArgumentException("Invalid timestamp format: " + text, e);
    }
  }

  /**
   * Formats a complex type JSON string into a consistent string format. This is primarily used when
   * complex datatype support is disabled.
   *
   * @param jsonString The JSON string representation of the complex type
   * @param complexType The type of complex data (MAP, ARRAY, STRUCT)
   * @param typeMetadata The metadata for the type (e.g., {@code MAP<INT,INT>})
   * @return A consistently formatted string representation
   */
  public String formatComplexTypeString(
      String jsonString, String complexType, String typeMetadata) {
    if (jsonString == null || complexType == null) {
      return jsonString;
    }

    try {
      ComplexTypePlan plan = ComplexTypePlan.of(typeMetadata);
      if (plan.containsStruct) {
        // Arrow StructVector.getObject omits null children. Restore their field
        // presence from the schema without coercing any non-null scalar values.
        JsonNode node = readComplexJson(jsonString);
        restoreStructNullFields(node, plan);
        jsonString = node.toString();
      }
      if (complexType.equals(DatabricksTypeUtil.MAP)) {
        return formatMapString(jsonString, typeMetadata);
      }
      if (plan.containsDecimal) {
        return formatDecimalText(jsonString, plan);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to format complex type representation: {}", e.getMessage());
    }

    return jsonString;
  }

  private static JsonNode readComplexJson(String json) throws IOException {
    return JsonUtil.getMapper()
        .reader()
        .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .without(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
        .readTree(json);
  }

  private static void restoreStructNullFields(JsonNode node, ComplexTypePlan plan) {
    if (node == null || node.isNull()) {
      return;
    }
    if (plan.type.startsWith(DatabricksTypeUtil.STRUCT) && node.isObject()) {
      for (Map.Entry<String, ComplexTypePlan> field : plan.fields.entrySet()) {
        if (!node.has(field.getKey())) {
          ((ObjectNode) node).putNull(field.getKey());
        } else {
          restoreStructNullFields(node.get(field.getKey()), field.getValue());
        }
      }
    } else if (plan.element != null && node.isArray()) {
      for (JsonNode element : node) {
        restoreStructNullFields(element, plan.element);
      }
    } else if (plan.key != null) {
      if (node.isArray()) {
        for (JsonNode entry : node) {
          restoreStructNullFields(entry.get("key"), plan.key);
          restoreStructNullFields(entry.get("value"), plan.value);
        }
      } else if (node.isObject()) {
        for (JsonNode value : node) {
          restoreStructNullFields(value, plan.value);
        }
      }
    }
  }

  /** Replaces only geospatial leaves in raw Arrow JSON when complex support is disabled. */
  public String formatGeospatialComplexTypeString(String json, String complexType, String metadata)
      throws DatabricksParsingException {
    try {
      // Do not coerce other fields through JDBC scalar objects: dates/timestamps
      // must keep their wire representation and decimals must retain precision.
      JsonNode node = readComplexJson(json);
      JsonNode formatted = new ComplexDataTypeParser(false).geospatialLeavesAsText(node, metadata);
      return formatComplexTypeString(formatted.toString(), complexType, metadata);
    } catch (IOException e) {
      throw new DatabricksParsingException(
          "Failed to format nested geospatial values",
          e,
          DatabricksDriverErrorCode.JSON_PARSING_ERROR);
    }
  }

  private JsonNode geospatialLeavesAsText(JsonNode node, String metadata)
      throws DatabricksParsingException {
    if (node == null || node.isNull() || !MetadataParser.containsGeospatialType(metadata)) {
      return node;
    }
    if (DatabricksTypeUtil.isGeospatialType(metadata.toUpperCase(Locale.ROOT))) {
      return JsonUtil.getMapper().valueToTree(convertGeospatial(node, metadata));
    }
    if (metadata.startsWith(DatabricksTypeUtil.ARRAY) && node.isArray()) {
      String elementType = MetadataParser.parseArrayMetadata(metadata);
      for (int i = 0; i < node.size(); i++) {
        ((ArrayNode) node).set(i, geospatialLeavesAsText(node.get(i), elementType));
      }
    } else if (metadata.startsWith(DatabricksTypeUtil.STRUCT) && node.isObject()) {
      for (Map.Entry<String, String> field :
          MetadataParser.parseStructMetadata(metadata).entrySet()) {
        if (node.has(field.getKey())) {
          ((ObjectNode) node)
              .set(
                  field.getKey(),
                  geospatialLeavesAsText(node.get(field.getKey()), field.getValue()));
        }
      }
    } else if (metadata.startsWith(DatabricksTypeUtil.MAP)) {
      String[] types = MetadataParser.parseMapMetadataParts(metadata);
      if (node.isArray()) {
        for (JsonNode entry : node) {
          ((ObjectNode) entry)
              .set("key", geospatialLeavesAsText(entry.get("key"), types[0].trim()));
          ((ObjectNode) entry)
              .set("value", geospatialLeavesAsText(entry.get("value"), types[1].trim()));
        }
      } else if (node.isObject()) {
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> field = fields.next();
          field.setValue(geospatialLeavesAsText(field.getValue(), types[1].trim()));
        }
      }
    }
    return node;
  }

  /**
   * Formats a map JSON string into the standard {key:value} format.
   *
   * @param jsonString The JSON string representation of the map
   * @param mapMetadata The metadata for the map type (e.g., {@code MAP<INT,INT>})
   * @return A map string in the format {key:value,key:value}
   */
  public String formatMapString(String jsonString, String mapMetadata) {
    try {
      ComplexTypePlan plan = ComplexTypePlan.of(mapMetadata);
      boolean hasDecimal = plan.containsDecimal;
      JsonNode node =
          hasDecimal
              ? readDecimalJson(jsonString, plan)
              : JsonUtil.getMapper().readTree(jsonString);
      if (node.isArray() && node.size() > 0 && node.get(0).has("key")) {
        // recoverStringType also accepts collated strings such as "STRING COLLATE UTF8_LCASE".
        boolean isStringKey = plan.key == null || plan.key.string;
        boolean isStringValue = plan.value == null || plan.value.string;

        StringBuilder result = new StringBuilder("{");
        // Strict JSON carries '"', '\' and control characters in strings only as backslash
        // escapes, so without a backslash in the input no string needs escaping.
        boolean escapeStrings = jsonString.indexOf('\\') >= 0;

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
            appendJsonString(result, keyNode.asText(), escapeStrings);
          } else {
            result.append(numberText(keyNode));
          }

          result.append(":");

          // Handle null valueNode
          if (valueNode == null || valueNode.isNull()) {
            result.append("null");
          } else if (valueNode.isContainerNode()) {
            // Nested complex value (ARRAY/STRUCT/MAP): asText() returns "" for
            // container nodes, so emit the JSON directly (e.g. {0:[34277,0]}).
            result.append(
                hasDecimal
                    ? PLAIN_DECIMAL_WRITER.writeValueAsString(valueNode)
                    : valueNode.toString());
          } else if (isStringValue) {
            appendJsonString(result, valueNode.asText(), escapeStrings);
          } else {
            result.append(numberText(valueNode));
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

  private static String numberText(JsonNode node) {
    return node.isBigDecimal() ? node.decimalValue().toPlainString() : node.asText();
  }

  private static JsonNode readDecimalJson(String json, String metadata) throws IOException {
    return readDecimalJson(json, ComplexTypePlan.of(metadata));
  }

  private static JsonNode readDecimalJson(String json, ComplexTypePlan plan) throws IOException {
    JsonNode node;
    if (!plan.containsDecimal) {
      node = JsonUtil.getMapper().readTree(json);
    } else {
      try (JsonParser parser = JsonUtil.getMapper().createParser(json)) {
        parser.nextToken();
        node = readDecimalNode(parser, plan);
      }
    }
    if (node == null || node.isNull() || node.isMissingNode()) {
      throw new IOException("Expected a non-null complex JSON root");
    }
    return node;
  }

  private static JsonNode readDecimalNode(JsonParser parser, ComplexTypePlan plan)
      throws IOException {
    JsonToken token = parser.currentToken();
    if (token == null) {
      throw new IOException("Unexpected end of complex JSON");
    }
    if (plan.decimal && token != JsonToken.VALUE_NULL) {
      return DecimalNode.valueOf(
          plan.decimalValue(token.isNumeric() ? parser.getDecimalValue() : parser.getText()));
    }
    if (!plan.containsDecimal && token.isStructStart()) {
      return JsonUtil.getMapper().readTree(parser);
    }
    if (parser.currentToken() == JsonToken.START_ARRAY) {
      ComplexTypePlan child =
          plan.element != null
              ? plan.element
              : plan.entry != null ? plan.entry : ComplexTypePlan.STRING;
      ArrayNode array = JsonUtil.getMapper().createArrayNode();
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        array.add(readDecimalNode(parser, child));
      }
      return array;
    }
    if (parser.currentToken() == JsonToken.START_OBJECT) {
      ComplexTypePlan valueType = plan.value != null ? plan.value : ComplexTypePlan.STRING;
      ObjectNode object = JsonUtil.getMapper().createObjectNode();
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        String name = parser.currentName();
        parser.nextToken();
        object.set(name, readDecimalNode(parser, plan.fields.getOrDefault(name, valueType)));
      }
      return object;
    }
    JsonNodeFactory nodes = JsonUtil.getMapper().getNodeFactory();
    switch (token) {
      case VALUE_NUMBER_INT:
        switch (parser.getNumberType()) {
          case BIG_INTEGER:
            return nodes.numberNode(parser.getBigIntegerValue());
          case LONG:
            return nodes.numberNode(parser.getLongValue());
          default:
            return nodes.numberNode(parser.getIntValue());
        }
      case VALUE_NUMBER_FLOAT:
        return nodes.numberNode(parser.getDoubleValue());
      case VALUE_STRING:
        return nodes.textNode(parser.getText());
      case VALUE_TRUE:
      case VALUE_FALSE:
        return nodes.booleanNode(token == JsonToken.VALUE_TRUE);
      case VALUE_NULL:
        return nodes.nullNode();
      default:
        throw new IOException("Unexpected token in complex JSON: " + token);
    }
  }

  private static String formatDecimalText(String json, ComplexTypePlan plan) throws IOException {
    StringBuilder output = new StringBuilder(json.length());
    int[] copied = {0};
    try (JsonParser parser = JsonUtil.getMapper().createParser(json)) {
      parser.nextToken();
      appendDecimalText(parser, plan, json, output, copied);
    }
    return output.append(json, copied[0], json.length()).toString();
  }

  private static void appendDecimalText(
      JsonParser parser, ComplexTypePlan plan, String json, StringBuilder output, int[] copied)
      throws IOException {
    JsonToken token = parser.currentToken();
    if (token == null) {
      throw new IOException("Unexpected end of complex JSON");
    }
    if (!plan.containsDecimal) {
      parser.skipChildren();
    } else if (token == JsonToken.START_ARRAY) {
      ComplexTypePlan child = plan.element != null ? plan.element : plan.entry;
      while (parser.nextToken() != JsonToken.END_ARRAY) {
        appendDecimalText(
            parser, child == null ? ComplexTypePlan.STRING : child, json, output, copied);
      }
    } else if (token == JsonToken.START_OBJECT) {
      while (parser.nextToken() != JsonToken.END_OBJECT) {
        String name = parser.currentName();
        parser.nextToken();
        ComplexTypePlan child =
            plan.fields.getOrDefault(
                name, plan.value == null ? ComplexTypePlan.STRING : plan.value);
        appendDecimalText(parser, child, json, output, copied);
      }
    } else if (plan.decimal && token != JsonToken.VALUE_NULL) {
      String decimal =
          plan.decimalValue(token.isNumeric() ? parser.getDecimalValue() : parser.getText())
              .toPlainString();
      int start = (int) parser.currentTokenLocation().getCharOffset();
      int end = (int) parser.currentLocation().getCharOffset();
      output.append(json, copied[0], start).append(decimal);
      copied[0] = end;
    }
  }

  private static void appendJsonString(StringBuilder out, String text, boolean escape) {
    out.append('"');
    if (escape) {
      JsonStringEncoder.getInstance().quoteAsString(text, out);
    } else {
      out.append(text);
    }
    out.append('"');
  }
}
