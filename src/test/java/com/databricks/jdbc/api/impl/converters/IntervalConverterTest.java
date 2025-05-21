package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Period;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class IntervalConverterTest {

  @ParameterizedTest(name = "[{index}] {0} ⇒ “{1}”")
  @CsvSource({
    // YEAR, MONTH, and YEAR TO MONTH
    "INTERVAL YEAR,        P1200M,     100-0",
    "INTERVAL MONTH,       P1200M,     100-0",
    "INTERVAL YEAR TO MONTH,P1200M,    100-0",

    // negative year‐month
    "INTERVAL YEAR TO MONTH,-P1200M,   -100-0",

    // DAY alone
    "INTERVAL DAY,         P3D,        3",
    "INTERVAL DAY,        -P3D,       -3",

    // DAY TO HOUR
    "INTERVAL DAY TO HOUR, P3DT4H,     3 04",
    "INTERVAL DAY TO HOUR, P0DT0H,     0 00",

    // DAY TO MINUTE
    "INTERVAL DAY TO MINUTE, P1DT2H30M,1 02:30",
    "INTERVAL DAY TO MINUTE,-P1DT2H,   -1 02:00",

    // DAY TO SECOND with fractions
    "INTERVAL DAY TO SECOND, P1DT2H3M4.005S,1 02:03:04.005",
    "INTERVAL DAY TO SECOND,-P0DT0H0M0.500S,-0 00:00:00.5",

    // HOUR alone
    "INTERVAL HOUR,        PT27H,       27",
    "INTERVAL HOUR,       -PT27H,      -27",

    // HOUR TO MINUTE
    "INTERVAL HOUR TO MINUTE, PT2H5M,   2:05",
    "INTERVAL HOUR TO MINUTE,-PT2H,      -2:00",

    // HOUR TO SECOND
    "INTERVAL HOUR TO SECOND, PT2H3M4.006S,2:03:04.006",
    "INTERVAL HOUR TO SECOND,-PT2H,      -2:00:00",

    // MINUTE TO SECOND
    "INTERVAL MINUTE TO SECOND, PT90S,   1:30",
    "INTERVAL MINUTE TO SECOND,-PT90.5S, -1:30.5",

    // SECOND alone (with and without fractional)
    "INTERVAL SECOND,     PT45S,       45",
    "INTERVAL SECOND,     PT0.500S,    0.5"
  })
  void testAllQualifiers(String metadata, String isoPeriodOrDuration, String expectedBody) {
    IntervalConverter ic = new IntervalConverter(metadata);
    Object obj;
    if (metadata.toUpperCase().contains("YEAR") || metadata.toUpperCase().contains("MONTH")) {
      obj = Period.parse(isoPeriodOrDuration);
    } else {
      obj = Duration.parse(isoPeriodOrDuration);
    }
    String literal = ic.toLiteral(obj);
    assertEquals(
        expectedBody, literal, () -> "body for " + metadata + " of " + isoPeriodOrDuration);
  }
}
