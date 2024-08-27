package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StringUtilTest {
  private static StringUtil stringUtil = new StringUtil();

  @Test
  public void testDateEscapeSequence() {
    String sqlWithDate = "SELECT * FROM table WHERE date_column = {d '2023-01-01'}";
    String expected = "SELECT * FROM table WHERE date_column = DATE '2023-01-01'";
    assertEquals(expected, stringUtil.getProcessedEscapeSequence(sqlWithDate));
  }

  @Test
  public void testTimeEscapeSequence() {
    String sqlWithTime = "SELECT * FROM table WHERE time_column = {t '23:59:59'}";
    String expected = "SELECT * FROM table WHERE time_column = TIME '23:59:59'";
    assertEquals(expected, stringUtil.getProcessedEscapeSequence(sqlWithTime));
  }

  @Test
  public void testTimestampEscapeSequence() {
    String sqlWithTimestamp =
        "SELECT * FROM table WHERE timestamp_column = {ts '2023-01-01 23:59:59.123'}";
    String expected =
        "SELECT * FROM table WHERE timestamp_column = TIMESTAMP '2023-01-01 23:59:59.123'";
    assertEquals(expected, stringUtil.getProcessedEscapeSequence(sqlWithTimestamp));
  }

  @Test
  public void testFunctionEscapeSequence() {
    String sqlWithFunction = "SELECT {fn UCASE('name')} FROM table";
    String expected = "SELECT UCASE('name') FROM table";
    assertEquals(expected, stringUtil.getProcessedEscapeSequence(sqlWithFunction));
  }

  @Test
  public void testNoEscapeSequence() {
    String sqlWithoutEscape = "SELECT * FROM table WHERE column = 'value'";
    assertEquals(sqlWithoutEscape, stringUtil.getProcessedEscapeSequence(sqlWithoutEscape));
  }
}
