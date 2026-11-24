package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.Time;
import java.time.LocalTime;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TimeBotReviewRegressionTest {
  @Test
  void arrayResultSetStringsKeepExactTimeAndFollowMutation() throws Exception {
    DatabricksArray array =
        new ComplexDataTypeParser()
            .parseJsonStringToDbArray(
                "[\"09:15:00.123456\",null,\"00:00:00\",\"23:59:59.999999\"]", "ARRAY<TIME(6)>");
    try (ResultSet result = array.getResultSet()) {
      assertTrue(result.next());
      assertEquals("09:15:00.123456", result.getString(2));
      assertEquals(result.getString(2), result.getString("VALUE"));
      assertFalse(result.wasNull());
      assertInstanceOf(Time.class, result.getObject(2));
      assertEquals(123, Math.floorMod(result.getTime(2).getTime(), 1000L));
      ((Time) ((Object[]) array.getArray())[0]).setTime(Time.valueOf("12:34:56").getTime());
      assertEquals("12:34:56", result.getString(2));
      assertEquals(LocalTime.of(12, 34, 56), result.getObject(2, LocalTime.class));
      assertTrue(result.next());
      assertNull(result.getString(2));
      assertTrue(result.wasNull());
      assertTrue(result.next());
      assertEquals("00:00:00", result.getString(2));
      assertFalse(result.wasNull());
      assertTrue(result.next());
      assertEquals("23:59:59.999999", result.getString(2));
      assertFalse(result.next());
    }
    try (ResultSet result =
        array.getResultSet(4, 1, Collections.singletonMap("TIME", LocalTime.class))) {
      assertTrue(result.next());
      assertEquals(4L, result.getLong(1));
      assertEquals("23:59:59.999999", result.getString(2));
      assertEquals(LocalTime.parse("23:59:59.999999"), result.getObject(2));
    }
  }

  @Test
  void supportedTimeTypeMapsConvertNonNullValues() throws Exception {
    for (boolean complex : new boolean[] {false, true}) {
      try (DatabricksResultSet result =
          TimeRoundTwoRegressionTest.inlineResult("TIME(6)", "09:15:00.123456", false, complex)) {
        assertTrue(result.next());
        Map<String, Class<?>> timeMap = Collections.singletonMap("TIME", Time.class);
        Object mapped = result.getObject(1, timeMap);
        assertEquals(result.getTime(1), mapped);
        assertEquals(123, Math.floorMod(((Time) mapped).getTime(), 1000L));
        assertEquals(
            "09:15:00.123456", result.getObject(1, Collections.singletonMap("TIME", String.class)));
        assertEquals(
            LocalTime.parse("09:15:00.123456"),
            result.getObject(1, Collections.singletonMap("TIME", LocalTime.class)));
        assertFalse(result.wasNull());
      }
      try (DatabricksResultSet result =
          TimeRoundTwoRegressionTest.inlineResult("TIME(6)", null, false, complex)) {
        assertTrue(result.next());
        assertNull(result.getObject(1, Collections.singletonMap("TIME", Time.class)));
        assertTrue(result.wasNull());
        assertNull(result.getObject(1, Collections.singletonMap("TIME", String.class)));
        assertTrue(result.wasNull());
      }
    }
  }
}
