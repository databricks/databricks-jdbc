package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

/** Reconstructs the IPC schema of APC bare record batches from the result manifest. */
public final class ArrowBatchSchemaBuilder {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern DECIMAL = Pattern.compile("decimal\\((\\d+),(\\d+)\\)");
  private static final String SQL_TYPE_METADATA = "Spark:DataType:SqlName";

  private ArrowBatchSchemaBuilder() {}

  public static byte[] serialize(ResultSchema resultSchema) throws DatabricksParsingException {
    return serialize(resultSchema, "UTC");
  }

  public static byte[] serialize(ResultSchema resultSchema, String sessionTimeZone)
      throws DatabricksParsingException {
    try {
      return SchemaUtility.serialize(build(resultSchema, sessionTimeZone));
    } catch (IOException e) {
      throw invalidSchema("Unable to serialize APC Arrow schema", e);
    }
  }

  public static Schema build(ResultSchema resultSchema) throws DatabricksParsingException {
    return build(resultSchema, "UTC");
  }

  public static Schema build(ResultSchema resultSchema, String sessionTimeZone)
      throws DatabricksParsingException {
    if (resultSchema == null || resultSchema.getColumns() == null) {
      throw invalidSchema("APC result manifest has no schema", null);
    }
    List<Field> fields = new ArrayList<>();
    for (ColumnInfo column : resultSchema.getColumns()) {
      if (column.getName() == null) {
        throw invalidSchema("APC result schema has an unnamed column", null);
      }
      JsonNode type = parseType(column.getTypeJson(), column.getTypeText());
      String sqlType = column.getTypeText() != null ? column.getTypeText() : typeName(type);
      fields.add(field(column.getName(), type, true, sqlType, sessionTimeZone));
    }
    return new Schema(fields);
  }

  private static JsonNode parseType(String typeJson, String typeText)
      throws DatabricksParsingException {
    String source = typeJson != null && !typeJson.isEmpty() ? typeJson : typeText;
    if (source == null || source.isEmpty()) {
      throw invalidSchema("APC result column has no type", null);
    }
    source = source.trim();
    try {
      return source.startsWith("{") || source.startsWith("\"")
          ? MAPPER.readTree(source)
          : MAPPER.getNodeFactory().textNode(source);
    } catch (IOException e) {
      throw invalidSchema("Invalid APC result type_json", e);
    }
  }

  private static String typeName(JsonNode type) {
    return type.isTextual() ? type.asText() : type.path("type").asText();
  }

  private static Field field(
      String name, JsonNode type, boolean nullable, String sqlType, String sessionTimeZone)
      throws DatabricksParsingException {
    String nameLower = typeName(type).toLowerCase(java.util.Locale.ROOT);
    List<Field> children = Collections.emptyList();
    ArrowType arrowType;
    Matcher decimal = DECIMAL.matcher(nameLower.replace(" ", ""));
    if (decimal.matches() || "decimal".equals(nameLower)) {
      int precision =
          decimal.matches() ? Integer.parseInt(decimal.group(1)) : type.path("precision").asInt(10);
      int scale =
          decimal.matches() ? Integer.parseInt(decimal.group(2)) : type.path("scale").asInt(0);
      arrowType = new ArrowType.Decimal(precision, scale, precision > 38 ? 256 : 128);
    } else if (nameLower.startsWith("geometry") || nameLower.startsWith("geography")) {
      // The logical geospatial type is carried in metadata; Arrow stores SRID and WKB.
      arrowType = ArrowType.Struct.INSTANCE;
      children =
          java.util.Arrays.asList(
              new Field("srid", FieldType.notNullable(new ArrowType.Int(32, true)), null),
              new Field("wkb", FieldType.notNullable(ArrowType.Binary.INSTANCE), null));
    } else if (nameLower.startsWith("interval")
        || nameLower.equals("yearmonthinterval")
        || nameLower.equals("daytimeinterval")) {
      String intervalName = (nameLower + " " + sqlType).toLowerCase(java.util.Locale.ROOT);
      arrowType =
          new ArrowType.Interval(
              intervalName.contains("year") || intervalName.contains("month")
                  ? IntervalUnit.YEAR_MONTH
                  : IntervalUnit.DAY_TIME);
    } else if (nameLower.startsWith("char(")
        || nameLower.startsWith("varchar(")
        || nameLower.startsWith("string collate ")) {
      arrowType = ArrowType.Utf8.INSTANCE;
    } else {
      switch (nameLower) {
        case "boolean":
          arrowType = ArrowType.Bool.INSTANCE;
          break;
        case "byte":
        case "tinyint":
          arrowType = new ArrowType.Int(8, true);
          break;
        case "short":
        case "smallint":
          arrowType = new ArrowType.Int(16, true);
          break;
        case "integer":
        case "int":
          arrowType = new ArrowType.Int(32, true);
          break;
        case "long":
        case "bigint":
          arrowType = new ArrowType.Int(64, true);
          break;
        case "float":
          arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
          break;
        case "double":
          arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
          break;
        case "binary":
          arrowType = ArrowType.Binary.INSTANCE;
          break;
        case "date":
          arrowType = new ArrowType.Date(DateUnit.DAY);
          break;
        case "timestamp":
          arrowType =
              new ArrowType.Timestamp(
                  TimeUnit.MICROSECOND,
                  type.path("timezone").isTextual()
                      ? type.path("timezone").asText()
                      : sessionTimeZone);
          break;
        case "timestamp_ntz":
          arrowType = new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
          break;
        case "array":
          arrowType = ArrowType.List.INSTANCE;
          children =
              Collections.singletonList(
                  field(
                      "element",
                      type.path("elementType"),
                      type.path("containsNull").asBoolean(true),
                      typeName(type.path("elementType")),
                      sessionTimeZone));
          break;
        case "map":
          arrowType = new ArrowType.Map(false);
          Field key =
              field(
                  "key",
                  type.path("keyType"),
                  false,
                  typeName(type.path("keyType")),
                  sessionTimeZone);
          Field value =
              field(
                  "value",
                  type.path("valueType"),
                  type.path("valueContainsNull").asBoolean(true),
                  typeName(type.path("valueType")),
                  sessionTimeZone);
          children =
              Collections.singletonList(
                  new Field(
                      "entries",
                      new FieldType(false, ArrowType.Struct.INSTANCE, null),
                      java.util.Arrays.asList(key, value)));
          break;
        case "struct":
          arrowType = ArrowType.Struct.INSTANCE;
          List<Field> structFields = new ArrayList<>();
          for (JsonNode child : type.path("fields")) {
            structFields.add(
                field(
                    child.path("name").asText(),
                    child.path("type"),
                    child.path("nullable").asBoolean(true),
                    typeName(child.path("type")),
                    sessionTimeZone));
          }
          children = structFields;
          break;
        case "void":
        case "null":
          arrowType = ArrowType.Null.INSTANCE;
          break;
        case "string":
        case "char":
        case "varchar":
        case "variant":
          arrowType = ArrowType.Utf8.INSTANCE;
          break;
        default:
          throw invalidSchema("Unsupported APC Arrow type: " + nameLower, null);
      }
    }
    return new Field(
        name,
        new FieldType(nullable, arrowType, null, Map.of(SQL_TYPE_METADATA, sqlType)),
        children);
  }

  private static DatabricksParsingException invalidSchema(String message, Exception cause) {
    DatabricksDriverErrorCode code = DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR;
    return new DatabricksParsingException(message, cause, code.name(), code.getCode());
  }
}
