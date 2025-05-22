package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@DisplayName("IntervalConverter")
class IntervalConverterTest {

  @Nested
  @DisplayName("Year–Month intervals")
  class YearMonth {

    @ParameterizedTest(name = "[{index}] {1} of {0} months ⇒ “{2}”")
    @CsvSource({
      "1200, INTERVAL YEAR,          100-0",
      "1200, INTERVAL MONTH,         100-0",
      "1200, INTERVAL YEAR TO MONTH, 100-0",
      "14,   INTERVAL YEAR TO MONTH, 1-2",
      "-14,  INTERVAL YEAR TO MONTH, -1-2",
      "0,    INTERVAL YEAR,          0-0",
      "0,    INTERVAL MONTH,         0-0"
    })
    void testNormalization(long totalMonths, String meta, String expected) {
      Period p = Period.ofMonths((int) totalMonths);
      IntervalConverter ic = new IntervalConverter(meta);
      assertEquals(expected, ic.toLiteral(p));
    }

    @Test
    @DisplayName("Huge period still normalizes correctly")
    void testHugePeriod() {
      Period p = Period.ofYears(37212).plusMonths(11);
      IntervalConverter ic = new IntervalConverter("INTERVAL YEAR TO MONTH");
      assertEquals("37212-11", ic.toLiteral(p));
    }

    @Test
    @DisplayName("Passing Duration to YEAR qualifier throws")
    void testTypeMismatch() {
      IntervalConverter ic = new IntervalConverter("INTERVAL YEAR");
      assertThrows(IllegalArgumentException.class, () -> ic.toLiteral(Duration.ofHours(5)));
    }
  }

  @Nested
  @DisplayName("Day–Time intervals (full D HH:MM:SS.NNNNNNNNN form)")
  class DayTime {

    @ParameterizedTest(name = "[{index}] {0} ⇒ “{3}”")
    @CsvSource({
      // desc                   , metadata              , ISO8601           , expected
      "zero                   , INTERVAL DAY TO SECOND, PT0S               , 0 00:00:00.000000000",
      "only days              , INTERVAL DAY,          P3D                 , 3 00:00:00.000000000",
      "days+hours             , INTERVAL DAY TO HOUR,  P2DT5H              , 2 05:00:00.000000000",
      "days+minutes           , INTERVAL DAY TO MINUTE,P1DT2H30M           , 1 02:30:00.000000000",
      "days+seconds frac      , INTERVAL DAY TO SECOND,P0DT0H0M4.005S      , 0 00:00:04.005000000",
      "only hours             , INTERVAL HOUR,         PT27H               , 1 03:00:00.000000000",
      "hour+minute            , INTERVAL HOUR TO MINUTE,PT2H5M             , 0 02:05:00.000000000",
      "hour+second frac       , INTERVAL HOUR TO SECOND,PT1H0M0.123S       , 0 01:00:00.123000000",
      "only minutes           , INTERVAL MINUTE,       PT125M              , 0 02:05:00.000000000",
      "minute+second frac     , INTERVAL MINUTE TO SECOND,PT1M30.5S        , 0 00:01:30.500000000",
      "only seconds frac      , INTERVAL SECOND,       PT45.789S           , 0 00:00:45.789000000",
      "large nano fraction    , INTERVAL SECOND,       PT0.000000123S      , 0 00:00:00.000000123"
    })
    void testCanonicalForm(String desc, String meta, String iso, String expected) {
      Duration d = Duration.parse(iso);
      IntervalConverter ic = new IntervalConverter(meta);
      assertEquals(expected, ic.toLiteral(d), () -> desc + " failed");
    }

    @Test
    @DisplayName("Negative duration rollover across days")
    void testNegativeAndRollover() {
      // -(49h + 10m + 5s + 123ms)
      Duration d =
          Duration.ofHours(-49)
              .plusMinutes(-10)
              .plusSeconds(-5)
              .plusMillis(-123)
              .truncatedTo(ChronoUnit.MILLIS);
      IntervalConverter ic = new IntervalConverter("INTERVAL DAY TO SECOND");
      // |d| = 177005.123s → 2 days + 4205.123s → 2 days, 1h 10m 5.123s
      assertEquals("-2 01:10:05.123000000", ic.toLiteral(d));
    }

    @Test
    @DisplayName("Edge: just under one day")
    void testMaxNanosUnderDay() {
      long justUnder = Duration.ofDays(1).toNanos() - 1;
      Duration d = Duration.ofNanos(justUnder);
      IntervalConverter ic = new IntervalConverter("INTERVAL DAY TO SECOND");
      assertEquals("0 23:59:59.999999999", ic.toLiteral(d));
    }

    @Test
    @DisplayName("Very large duration formatting")
    void testVeryLargeDuration() {
      // 1000d + 12h + 34m + 56s + 789ns
      Duration d =
          Duration.ofDays(1000).plusHours(12).plusMinutes(34).plusSeconds(56).plusNanos(789);
      IntervalConverter ic = new IntervalConverter("INTERVAL DAY TO SECOND");
      assertEquals("1000 12:34:56.000000789", ic.toLiteral(d));
    }

    @Test
    @DisplayName("LONG_MIN duration handling")
    void testLongExtremesDuration() {
      // LONG_MIN is -abs(LONG_MAX) + 1
      Duration d = Duration.ofNanos(Long.MIN_VALUE);
      IntervalConverter ic = new IntervalConverter("INTERVAL DAY TO SECOND");
      assertEquals("-106751 23:47:16.854775807", ic.toLiteral(d));

      // LONG_MAX
      d = Duration.ofNanos(Long.MAX_VALUE);
      ic = new IntervalConverter("INTERVAL DAY TO SECOND");
      assertEquals("106751 23:47:16.854775807", ic.toLiteral(d));
    }

    @Test
    @DisplayName("Passing Period to DAY qualifier throws")
    void testDayMismatch() {
      IntervalConverter ic = new IntervalConverter("INTERVAL HOUR TO MINUTE");
      assertThrows(IllegalArgumentException.class, () -> ic.toLiteral(Period.ofDays(1)));
    }
  }

  @Nested
  @DisplayName("Error conditions")
  class Errors {
    @Test
    @DisplayName("Invalid metadata string")
    void testInvalidMetadata() {
      assertThrows(IllegalArgumentException.class, () -> new IntervalConverter("NOT AN INTERVAL"));
    }
  }
}
