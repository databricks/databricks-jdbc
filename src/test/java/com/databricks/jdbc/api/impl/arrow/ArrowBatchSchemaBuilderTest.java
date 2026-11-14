package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class ArrowBatchSchemaBuilderTest {
  @Test
  void completeApcTypeMatrixBuildsIpcSchemaFromManifest() throws Exception {
    String[][] types = {
      {"boolean", "boolean", "Bool"},
      {"tinyint", "byte", "Int"},
      {"smallint", "short", "Int"},
      {"int", "integer", "Int"},
      {"bigint", "long", "Int"},
      {"float", "float", "FloatingPoint"},
      {"double", "double", "FloatingPoint"},
      {"string", "string", "Utf8"},
      {"binary", "binary", "Binary"},
      {"date", "date", "Date"},
      {"decimal(18,2)", "decimal(18,2)", "Decimal"},
      {"timestamp", "timestamp", "Timestamp"},
      {"timestamp_ntz", "timestamp_ntz", "Timestamp"},
      {"interval year to month", "yearMonthInterval", "Interval"},
      {"interval day to second", "dayTimeInterval", "Interval"},
      {"geography", "geography", "Struct"},
      {"geometry", "geometry", "Struct"},
      {"variant", "variant", "Utf8"},
      {
        "array<int>",
        "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true}",
        "List"
      },
      {
        "map<string,int>",
        "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":true}",
        "Map"
      },
      {
        "struct<x:int>",
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"x\",\"type\":\"integer\",\"nullable\":true}]}",
        "Struct"
      }
    };
    List<ColumnInfo> columns = new ArrayList<>();
    for (int i = 0; i < types.length; i++) {
      columns.add(
          new ColumnInfo()
              .setName("c" + i)
              .setPosition((long) i)
              .setTypeText(types[i][0])
              .setTypeJson(types[i][1]));
    }
    ResultSchema manifestSchema =
        new ResultSchema().setColumnCount((long) types.length).setColumns(columns);
    Schema arrowSchema = ArrowBatchSchemaBuilder.build(manifestSchema);
    assertEquals(types.length, arrowSchema.getFields().size());
    for (int i = 0; i < types.length; i++) {
      Field field = arrowSchema.getFields().get(i);
      assertEquals(types[i][2], field.getType().getTypeID().name(), types[i][0]);
      assertEquals(types[i][0], field.getMetadata().get("Spark:DataType:SqlName"));
    }
    assertEquals(
        128, ((ArrowType.Decimal) arrowSchema.getFields().get(10).getType()).getBitWidth());
    assertEquals(
        "UTC", ((ArrowType.Timestamp) arrowSchema.getFields().get(11).getType()).getTimezone());
    assertNull(((ArrowType.Timestamp) arrowSchema.getFields().get(12).getType()).getTimezone());
    assertEquals("element", arrowSchema.getFields().get(18).getChildren().get(0).getName());
    assertEquals("entries", arrowSchema.getFields().get(19).getChildren().get(0).getName());
    assertEquals("x", arrowSchema.getFields().get(20).getChildren().get(0).getName());
    assertEquals("srid", arrowSchema.getFields().get(15).getChildren().get(0).getName());
    assertEquals("wkb", arrowSchema.getFields().get(16).getChildren().get(1).getName());

    byte[] serialized = ArrowBatchSchemaBuilder.serialize(manifestSchema);
    try (RootAllocator allocator = new RootAllocator();
        ArrowStreamReader reader =
            new ArrowStreamReader(new ByteArrayInputStream(serialized), allocator)) {
      assertEquals(arrowSchema, reader.getVectorSchemaRoot().getSchema());
    }
  }

  @Test
  void unknownTypeCannotSilentlyCoerceToString() {
    ResultSchema schema =
        new ResultSchema()
            .setColumns(List.of(new ColumnInfo().setName("c").setTypeJson("future_type")));
    DatabricksParsingException error =
        assertThrows(DatabricksParsingException.class, () -> ArrowBatchSchemaBuilder.build(schema));
    assertEquals(DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR.name(), error.getSQLState());
    assertEquals(
        DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR.getCode(), error.getErrorCode());
  }

  @Test
  void timestampNtzManifestTypeNameDeserializes() throws Exception {
    ColumnInfo info =
        new ObjectMapper()
            .readValue(
                "{\"name\":\"ts\",\"type_name\":\"TIMESTAMP_NTZ\","
                    + "\"type_text\":\"TIMESTAMP_NTZ\",\"type_json\":\"timestamp_ntz\"}",
                ColumnInfo.class);
    assertEquals(ColumnInfoTypeName.TIMESTAMP_NTZ, info.getTypeName());
    assertInstanceOf(
        ArrowType.Timestamp.class,
        ArrowBatchSchemaBuilder.build(new ResultSchema().setColumns(List.of(info)))
            .getFields()
            .get(0)
            .getType());
  }
}
