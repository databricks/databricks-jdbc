package com.databricks.jdbc.api.impl.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimestampConverterTest {
  private Timestamp TIMESTAMP =
      Timestamp.from(
          LocalDateTime.of(2023, Month.SEPTEMBER, 10, 20, 45).atZone(ZoneId.of("UTC")).toInstant());

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(TIMESTAMP).convertToLong(), 1694378700000L);
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(TIMESTAMP).convertToString(), "2023-09-10T20:45:00Z");
  }

  @Test
  public void testConvertFromString() throws DatabricksSQLException {
    assertEquals(new TimestampConverter("2023-09-10T20:45:00Z").convertToTimestamp(), TIMESTAMP);
    Assertions.assertDoesNotThrow(
        () -> new TimestampConverter("2023-09-10 20:45:00").convertToString());
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(TIMESTAMP).convertToDate().toString(), "2023-09-10");
  }

  @Test
  public void testConvertToBigInteger() throws DatabricksSQLException {
    assertEquals(
        new TimestampConverter(TIMESTAMP).convertToBigInteger(),
        BigInteger.valueOf(1694378700000L));
  }
}
