package com.databricks.jdbc.api.impl.converters;

import java.time.Duration;
import java.time.Period;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts a java.time.Period or java.time.Duration into the exact ANSI‐style interval literals
 * that Databricks prints.
 */
public class IntervalConverter {

  // Arrow stores day‐time intervals in microseconds
  private static final long MICROS_PER_SECOND = 1_000_000L;
  private static final long MICROS_PER_MINUTE = MICROS_PER_SECOND * 60;
  private static final long MICROS_PER_HOUR = MICROS_PER_MINUTE * 60;
  private static final long MICROS_PER_DAY = MICROS_PER_HOUR * 24;

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

  private final Field start, end;
  private final boolean isYearMonth;

  /**
   * @param arrowMetadata e.g. "INTERVAL YEAR TO MONTH" or "INTERVAL HOUR TO SECOND"
   */
  public IntervalConverter(String arrowMetadata) {
    Matcher m = INTERVAL_PATTERN.matcher(arrowMetadata.trim());
    if (!m.matches()) {
      throw new IllegalArgumentException("Invalid interval metadata: " + arrowMetadata);
    }
    this.start = Field.valueOf(m.group(1).toUpperCase());
    this.end = (m.group(2) != null) ? Field.valueOf(m.group(2).toUpperCase()) : this.start;
    // YEAR or MONTH qualifiers → Period; otherwise → Duration
    this.isYearMonth = (start == Field.YEAR || start == Field.MONTH);
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
      body = formatDayTime((Duration) obj);
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

  // --- DAY–TIME formatting ---
  private String formatDayTime(Duration d) {
    long micros = d.toNanos() / 1_000; // to μs
    boolean neg = micros < 0;
    if (neg) micros = -micros;

    switch (start) {
      case DAY:
        return formatDay(micros, neg);
      case HOUR:
        return formatHour(micros, neg);
      case MINUTE:
        return formatMinute(micros, neg);
      case SECOND:
        return formatSecond(micros, neg);
      default:
        throw new IllegalStateException("Unexpected start field: " + start);
    }
  }

  private String formatDay(long micros, boolean neg) {
    long days = micros / MICROS_PER_DAY;
    micros %= MICROS_PER_DAY;
    long hours = micros / MICROS_PER_HOUR;
    micros %= MICROS_PER_HOUR;
    long minutes = micros / MICROS_PER_MINUTE;
    micros %= MICROS_PER_MINUTE;
    long seconds = micros / MICROS_PER_SECOND;
    long frac = micros % MICROS_PER_SECOND;

    StringBuilder sb = new StringBuilder();
    sb.append(neg ? "-" : "").append(days);

    if (start != end) {
      if (end.ordinal() >= Field.HOUR.ordinal()) {
        sb.append(" ").append(pad2(hours));
      }
      if (end.ordinal() >= Field.MINUTE.ordinal()) {
        sb.append(":").append(pad2(minutes));
      }
      if (end.ordinal() >= Field.SECOND.ordinal()) {
        sb.append(":").append(formatSecFrac(seconds, frac));
      }
    }
    return sb.toString();
  }

  private String formatHour(long micros, boolean neg) {
    long hours = micros / MICROS_PER_HOUR;
    micros %= MICROS_PER_HOUR;
    long minutes = micros / MICROS_PER_MINUTE;
    micros %= MICROS_PER_MINUTE;
    long seconds = micros / MICROS_PER_SECOND;
    long frac = micros % MICROS_PER_SECOND;

    StringBuilder sb = new StringBuilder();
    sb.append(neg ? "-" : "").append(hours);

    if (start != end) {
      if (end.ordinal() >= Field.MINUTE.ordinal()) {
        sb.append(":").append(pad2(minutes));
      }
      if (end.ordinal() >= Field.SECOND.ordinal()) {
        sb.append(":").append(formatSecFrac(seconds, frac));
      }
    }
    return sb.toString();
  }

  private String formatMinute(long micros, boolean neg) {
    long minutes = micros / MICROS_PER_MINUTE;
    micros %= MICROS_PER_MINUTE;
    long seconds = micros / MICROS_PER_SECOND;
    long frac = micros % MICROS_PER_SECOND;

    StringBuilder sb = new StringBuilder();
    sb.append(neg ? "-" : "").append(minutes);

    if (start != end && end == Field.SECOND) {
      sb.append(":").append(formatSecFrac(seconds, frac));
    }
    return sb.toString();
  }

  private String formatSecond(long micros, boolean neg) {
    long seconds = micros / MICROS_PER_SECOND;
    long frac = micros % MICROS_PER_SECOND;
    StringBuilder sb = new StringBuilder();
    sb.append(neg ? "-" : "").append(seconds);
    if (frac > 0) {
      sb.append(".")
          .append(
              String.format("%06d", frac) // pad to 6 digits
                  .replaceAll("0+$", "") // drop trailing zeros
              );
    }
    return sb.toString();
  }

  /** two-digit zero-pad for subfields */
  private static String pad2(long v) {
    return String.format("%02d", v);
  }

  /** seconds + optional fraction, with sub-second trimmed */
  private static String formatSecFrac(long sec, long frac) {
    String s = pad2(sec);
    if (frac == 0) return s;
    String f = String.format("%06d", frac).replaceAll("0+$", "");
    return s + "." + f;
  }
}
