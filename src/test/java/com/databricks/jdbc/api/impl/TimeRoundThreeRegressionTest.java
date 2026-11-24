package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TimeRoundThreeRegressionTest {
  private static final String MAP =
      "{\"a\":\"01:02:03.000001\",\"b\":\"01:02:03.0001\",\"c\":\"01:02:03.0002\"}";

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
