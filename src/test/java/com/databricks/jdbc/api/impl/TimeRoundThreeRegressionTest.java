package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TimeRoundThreeRegressionTest {
  private static final String MAP =
      "{\"a\":\"01:02:03.000001\",\"b\":\"01:02:03.0001\",\"c\":\"01:02:03.0002\"}";

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
