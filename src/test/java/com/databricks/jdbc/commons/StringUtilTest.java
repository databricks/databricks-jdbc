package com.databricks.jdbc.commons;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.commons.util.StringUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StringUtilTest {
  @Test
  public void testDateEscapeSequence() {
    String sqlWithDate = "SELECT * FROM table WHERE date_column = {d '2023-01-01'}";
    String expected = "SELECT * FROM table WHERE date_column = DATE '2023-01-01'";
    assertEquals(expected, StringUtil.getProcessedEscapeSequence(sqlWithDate));
  }

  @Test
  public void testTimeEscapeSequence() {
    String sqlWithTime = "SELECT * FROM table WHERE time_column = {t '23:59:59'}";
    String expected = "SELECT * FROM table WHERE time_column = TIME '23:59:59'";
    assertEquals(expected, StringUtil.getProcessedEscapeSequence(sqlWithTime));
  }

  @Test
  public void testTimestampEscapeSequence() {
    String sqlWithTimestamp =
        "SELECT * FROM table WHERE timestamp_column = {ts '2023-01-01 23:59:59.123'}";
    String expected =
        "SELECT * FROM table WHERE timestamp_column = TIMESTAMP '2023-01-01 23:59:59.123'";
    assertEquals(expected, StringUtil.getProcessedEscapeSequence(sqlWithTimestamp));
  }

  @Test
  public void testFunctionEscapeSequence() {
    String sqlWithFunction = "SELECT {fn UCASE('name')} FROM table";
    String expected = "SELECT UCASE('name') FROM table";
    assertEquals(expected, StringUtil.getProcessedEscapeSequence(sqlWithFunction));
  }

  @Test
  public void testNoEscapeSequence() {
    String sqlWithoutEscape = "SELECT * FROM table WHERE column = 'value'";
    assertEquals(sqlWithoutEscape, StringUtil.getProcessedEscapeSequence(sqlWithoutEscape));
  }
}
