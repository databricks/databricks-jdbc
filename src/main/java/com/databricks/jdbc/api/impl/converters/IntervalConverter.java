package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.time.Duration;
import java.time.Period;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts a java.time.Period or java.time.Duration into the exact ANSI‐style interval literals
 * that Databricks prints.
 */
public class IntervalConverter {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(IntervalConverter.class);

  // Arrow stores day‐time intervals in microseconds. Converting to nanoseconds to align with Thrift
  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final long NANOS_PER_MINUTE = NANOS_PER_SECOND * 60;
  private static final long NANOS_PER_HOUR = NANOS_PER_MINUTE * 60;
  private static final long NANOS_PER_DAY = NANOS_PER_HOUR * 24;

  private static final Pattern INTERVAL_PATTERN =
      Pattern.compile("INTERVAL\\s+(\\w+)(?:\\s+TO\\s+(\\w+))?", Pattern.CASE_INSENSITIVE);

  /** The supported fields in the SQL syntax. */
  public enum Field {
    YEAR,
    MONTH,
    DAY,
    HOUR,
    MINUTE,
    SECOND
  }

  private final boolean isYearMonth;

  /**
   * @param arrowMetadata e.g. "INTERVAL YEAR TO MONTH" or "INTERVAL HOUR TO SECOND"
   */
  public IntervalConverter(String arrowMetadata) {
    Matcher m = INTERVAL_PATTERN.matcher(arrowMetadata.trim());
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid interval metadata: " + arrowMetadata);
    }
    Field start = Field.valueOf(m.group(1).toUpperCase());
    // YEAR or MONTH qualifiers → Period; otherwise → Duration
    this.isYearMonth = (start.equals(Field.YEAR) || start.equals(Field.MONTH));
  }

  /**
   * Turn a Period (YEAR/MONTH intervals) or Duration (DAY–TIME intervals) into exactly the string
   * Databricks will print.
   */
  public String toLiteral(Object obj) {
    String body;
    if (isYearMonth) {
      if (!(obj instanceof Period)) {
        throw new IllegalArgumentException("Expected Period, got " + obj.getClass());
      }
      body = formatYearMonth((Period) obj);
    } else {
      if (!(obj instanceof Duration)) {
        throw new IllegalArgumentException("Expected Duration, got " + obj.getClass());
      }
      body = formatFullDayTime((Duration) obj);
    }
    return body;
  }

  // --- YEAR–MONTH formatting ---
  private String formatYearMonth(Period p) {
    long totalMonths = p.getYears() * 12L + p.getMonths();
    boolean neg = totalMonths < 0;
    long absMonths = Math.abs(totalMonths);
    long years = absMonths / 12;
    long months = absMonths % 12;
    // Databricks shows "Y-M" with no zero‐padding
    String body = years + "-" + months;
    return (neg ? "-" : "") + body;
  }

  // DAY–TIME always prints all subfields in D HH:MM:SS.NNNNNNNNN
  // max day to second supported is 106751 23:47:16.854775. Beyond that Duration Object rolls over
  // to LONG_MIN and loses info.
  private String formatFullDayTime(Duration d) {
    long nanos = d.toNanos();
    if (nanos == Long.MIN_VALUE) {
      nanos += 1; // -abs(LONG.MAX_VALUE)
    }
    boolean neg = nanos < 0;
    if (neg) nanos = -nanos;

    if (nanos == Long.MAX_VALUE) {
      LOGGER.warn(
          "Duration value at Long.MAX_VALUE detected - interval representation may be incorrect due to overflow");
    }

    long days = nanos / NANOS_PER_DAY;
    nanos %= NANOS_PER_DAY;
    long hours = nanos / NANOS_PER_HOUR;
    nanos %= NANOS_PER_HOUR;
    long minutes = nanos / NANOS_PER_MINUTE;
    nanos %= NANOS_PER_MINUTE;
    long seconds = nanos / NANOS_PER_SECOND;
    long frac = nanos % NANOS_PER_SECOND;

    // "%02d" for HH,MM,SS and "%09d" for nanosecond fraction
    return String.format(
        "%s%d %02d:%02d:%02d.%09d", neg ? "-" : "", days, hours, minutes, seconds, frac);
  }
}
