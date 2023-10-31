package com.databricks.jdbc.core.converters;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.core.DatabricksSQLException;
import java.sql.Date;
import java.sql.Timestamp;
import org.junit.jupiter.api.Test;

public class TimestampConverterTest {

  private Timestamp TIMESTAMP = Timestamp.valueOf("2023-09-10 20:45:00");

  @Test
  public void testConvertToLong() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(TIMESTAMP).convertToLong(), 1694358900000L);
  }

  @Test
  public void testConvertToString() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(TIMESTAMP).convertToString(), "2023-09-10 20:45:00.0");
  }

  @Test
  public void testConvertToDate() throws DatabricksSQLException {
    assertEquals(new TimestampConverter(TIMESTAMP).convertToDate(), Date.valueOf("2023-09-10"));
  }
}
