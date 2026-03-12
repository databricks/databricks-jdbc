package com.databricks.jdbc.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class Slf4jFormatterTest {

  private Slf4jFormatter formatter;

  @BeforeEach
  public void setUp() {
    formatter = new Slf4jFormatter();
  }

  @Test
  public void testFormat() {
    LogRecord record = new LogRecord(Level.INFO, "Test message");
    record.setSourceClassName("TestClass");
    record.setSourceMethodName("testMethod");

    Instant instant = Instant.parse("2021-07-01T00:00:00Z");
    record.setInstant(instant);

    String formattedLog = formatter.format(record);

    // Use system default timezone (matches formatter)
    TimeZone formatterZone = TimeZone.getDefault();

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormat.setTimeZone(formatterZone);

    String expectedTimestamp = dateFormat.format(Date.from(instant));

    // Use SHORT display name instead of ID (Asia/Kolkata → IST)
    String expectedZone = formatterZone.getDisplayName(false, TimeZone.SHORT);

    String expected =
        String.format(
            "%s %s INFO TestClass#testMethod - Test message%n", expectedTimestamp, expectedZone);

    assertEquals(expected, formattedLog);
  }

  @Test
  public void testFormatWithDifferentLevels() {
    LogRecord infoRecord = new LogRecord(Level.INFO, "Info message");
    LogRecord warningRecord = new LogRecord(Level.WARNING, "Warning message");
    LogRecord severeRecord = new LogRecord(Level.SEVERE, "Severe message");

    assertTrue(formatter.format(infoRecord).contains("INFO"));
    assertTrue(formatter.format(warningRecord).contains("WARNING"));
    assertTrue(formatter.format(severeRecord).contains("SEVERE"));
  }

  @Test
  public void testFormatWithNullValues() {
    LogRecord record = new LogRecord(Level.INFO, null);
    record.setSourceClassName(null);
    record.setSourceMethodName(null);

    String formattedLog = formatter.format(record);

    assertTrue(formattedLog.contains("INFO"));
    assertTrue(formattedLog.contains("null#null"));
    assertTrue(formattedLog.endsWith("null" + System.lineSeparator()));
  }
}
