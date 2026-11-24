package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.converters.JavaTimeConverter;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TimeRoundThreeRegressionTest {
  private static final String MAP =
      "{\"a\":\"01:02:03.000001\",\"b\":\"01:02:03.0001\",\"c\":\"01:02:03.0002\"}";

  @Test
  void timePrecheckIgnoresIdentifiersAndQuotedFields() {
    for (String metadata :
        Arrays.asList(
            "STRUCT<id:INT,event_time:STRING,runtime:BIGINT,tags:ARRAY<STRING>>",
            "ARRAY<STRUCT<uptime:DOUBLE,timezone:STRING>>",
            "STRUCT<time:INT,`TIME(6)`:STRING,`a``time`:TIMESTAMP_NTZ>",
            "STRUCT<`TIME`:ARRAY<TIMESTAMP>,time : INT>",
            "TIMESTAMPTIME")) {
      assertFalse(MetadataParser.mayContainTime(metadata), metadata);
    }
    for (String metadata :
        Arrays.asList(
            "time(6)",
            "ARRAY<TIME>",
            "STRUCT<time:TIME(3)>",
            "MAP<TIME(0),STRUCT<`x``y`:ARRAY<TIME(6)>>>")) {
      assertTrue(MetadataParser.mayContainTime(metadata), metadata);
    }
  }

  @Test
  void unsupportedTimeTypeMapsAreIgnored() throws Exception {
    String json = "[\"01:02:03.123456\"]";
    DatabricksArray array =
        new ComplexDataTypeParser().parseJsonStringToDbArray(json, "ARRAY<TIME(6)>");
    DatabricksStruct struct =
        new ComplexDataTypeParser()
            .parseJsonStringToDbStruct("{\"t\":\"01:02:03.123456\"}", "STRUCT<t:TIME(6)>");
    for (Class<?> target :
        Arrays.asList(LocalDate.class, LocalDateTime.class, Instant.class, OffsetDateTime.class)) {
      Map<String, Class<?>> mapping = Collections.singletonMap("TIME", target);
      assertNull(JavaTimeConverter.findJavaTimeMapping(mapping, "TIME"));
      assertNull(JavaTimeConverter.getJavaTimeMapping(mapping, "TIME(6)"));
      assertSame(array.getArray(), array.getArray(mapping));
      assertArrayEquals(struct.getAttributes(), struct.getAttributes(mapping));
      try (DatabricksResultSet result =
          TimeRoundTwoRegressionTest.inlineResult("TIME(6)", "01:02:03.123456", false, false)) {
        assertTrue(result.next());
        assertNull(result.getObject(1, mapping), target.getName());
      }
    }
    assertEquals(
        LocalTime.parse("01:02:03.123456"),
        ((Object[]) array.getArray(Collections.singletonMap("TIME", LocalTime.class)))[0]);
  }

  @Test
  void malformedPrecisionUsesDefault() {
    for (String type : Arrays.asList("TIME(6", "TIME()", "TIME(x)", "TIME(-1)", "TIME(99)")) {
      assertArrayEquals(new int[] {15, 6}, DatabricksTypeUtil.getTimePrecisionAndScale(type), type);
    }
  }

  @Test
  void numericTimeRequiresPhysicalUnit() {
    ComplexDataTypeParser parser = new ComplexDataTypeParser();
    for (boolean complex : new boolean[] {false, true}) {
      DatabricksParsingException error =
          assertThrows(
              DatabricksParsingException.class,
              () -> {
                if (complex) parser.parseJsonStringToDbArray("[86399999999]", "ARRAY<TIME(6)>");
                else
                  parser.formatComplexTypeStringForResult(
                      "[86399999999]", "ARRAY", "ARRAY<TIME(6)>");
              });
      assertEquals("Invalid TIME value", error.getMessage());
      assertInstanceOf(DateTimeException.class, error.getCause());
    }
  }

  @Test
  void publicFormatterNeedsNoCheckedException() {
    assertEquals(
        "[1]", new ComplexDataTypeParser().formatComplexTypeString("[1]", "ARRAY", "ARRAY<INT>"));
  }

  @Test
  void emptyStructAlongsideTimeKeepsFormatterFallback() throws Exception {
    String json = "{\"a\":{},\"t\":\"01:02:03\"}";
    String metadata = "STRUCT<a:STRUCT<>,t:TIME(0)>";
    assertEquals(
        json, new ComplexDataTypeParser().formatComplexTypeString(json, "STRUCT", metadata));
    try (DatabricksResultSet result =
        TimeRoundTwoRegressionTest.inlineResult(metadata, json, false, false)) {
      assertTrue(result.next());
      assertEquals(json, result.getString(1));
    }
  }

  @Test
  void malformedTimeJsonKeepsJsonParsingError() {
    DatabricksParsingException exception =
        assertThrows(
            DatabricksParsingException.class,
            () ->
                new ComplexDataTypeParser().parseJsonStringToDbArray("[broken", "ARRAY<TIME(6)>"));
    assertEquals(DatabricksDriverErrorCode.JSON_PARSING_ERROR.name(), exception.getSQLState());
    assertEquals(DatabricksDriverErrorCode.JSON_PARSING_ERROR.getCode(), exception.getErrorCode());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nestedMapsKeepExactTimeValues(boolean complex) throws Exception {
    String[] types = {
      "ARRAY<MAP<STRING,TIME(6)>>",
      "STRUCT<m:MAP<STRING,TIME(6)>>",
      "MAP<STRING,MAP<STRING,TIME(6)>>",
      "STRUCT<a:STRUCT<b:ARRAY<MAP<STRING,TIME(6)>>>>"
    };
    String[] values = {
      "[" + MAP + "]", "{\"m\":" + MAP + "}", "{\"m\":" + MAP + "}", "{\"a\":{\"b\":[" + MAP + "]}}"
    };
    for (int index = 0; index < types.length; index++) {
      try (DatabricksResultSet result =
          TimeRoundTwoRegressionTest.inlineResult(types[index], values[index], false, complex)) {
        assertTrue(result.next());
        assertEquals(values[index], result.getString(1), types[index]);
        assertEquals(result.getString(1), result.getObject(1).toString(), types[index]);
      }
      ComplexDataTypeParser parser = new ComplexDataTypeParser();
      Object typed;
      if (types[index].startsWith("ARRAY")) {
        typed = parser.parseJsonStringToDbArray(values[index], types[index]);
      } else if (types[index].startsWith("MAP")) {
        typed = parser.parseJsonStringToDbMap(values[index], types[index]);
      } else {
        typed = parser.parseJsonStringToDbStruct(values[index], types[index]);
      }
      assertEquals(values[index], typed.toString(), types[index]);
    }
  }
}
