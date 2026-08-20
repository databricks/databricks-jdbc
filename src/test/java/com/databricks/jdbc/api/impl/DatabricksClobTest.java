package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;

class DatabricksClobTest {

  @Test
  void supportsStringMutationAndReads() throws Exception {
    DatabricksClob clob = new DatabricksClob();

    assertEquals(0, clob.length());
    assertEquals(5, clob.setString(1, "hello"));
    assertEquals(2, clob.setString(2, "XY"));
    assertEquals("hXYlo", clob.getSubString(1, 5));
    assertEquals(2, clob.position("XY", 1));

    clob.truncate(3);
    assertEquals("hXY", clob.getSubString(1, 3));
  }

  @Test
  void supportsAsciiAndCharacterStreams() throws Exception {
    DatabricksClob clob = new DatabricksClob();
    clob.setString(1, "prefix");

    OutputStream output = clob.setAsciiStream(1);
    output.write("ascii".getBytes(StandardCharsets.US_ASCII));
    output.flush();
    assertEquals("asciix", clob.getSubString(1, 6));
    output.close();
    assertThrows(IOException.class, () -> output.write('x'));

    Writer writer = clob.setCharacterStream(6);
    writer.write("-chars");
    writer.flush();
    assertEquals("ascii-chars", clob.getSubString(1, 11));
    writer.close();
    assertThrows(IOException.class, () -> writer.write('x'));

    try (InputStream input = clob.getAsciiStream();
        Reader reader = clob.getCharacterStream(6, 6)) {
      assertEquals("ascii-chars", new String(input.readAllBytes(), StandardCharsets.US_ASCII));
      assertEquals("-chars", readAll(reader));
    }
  }

  @Test
  void rejectsInvalidRanges() throws Exception {
    DatabricksClob clob = new DatabricksClob();
    clob.setString(1, "value");

    DatabricksSQLException invalidPosition =
        assertThrows(DatabricksSQLException.class, () -> clob.getSubString(0, 1));
    assertEquals(
        DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR.name(), invalidPosition.getSQLState());
    assertEquals(
        DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR.getCode(), invalidPosition.getErrorCode());
    assertThrows(SQLException.class, () -> clob.getSubString(6, 0));
    assertThrows(SQLException.class, () -> clob.getSubString(1, 6));
    assertThrows(SQLException.class, () -> clob.setString(7, "x"));
    assertThrows(SQLException.class, () -> clob.truncate(6));
  }

  @Test
  void freeInvalidatesTheClob() throws Exception {
    DatabricksClob clob = new DatabricksClob();
    clob.setString(1, "value");

    clob.free();

    DatabricksSQLException exception = assertThrows(DatabricksSQLException.class, clob::length);
    assertEquals(DatabricksDriverErrorCode.INVALID_STATE.name(), exception.getSQLState());
    assertEquals(DatabricksDriverErrorCode.INVALID_STATE.getCode(), exception.getErrorCode());
    assertThrows(SQLException.class, clob::getCharacterStream);
    assertThrows(SQLException.class, () -> clob.setString(1, "other"));
  }

  private static String readAll(Reader reader) throws Exception {
    StringBuilder result = new StringBuilder();
    char[] buffer = new char[32];
    int count;
    while ((count = reader.read(buffer)) != -1) {
      result.append(buffer, 0, count);
    }
    return result.toString();
  }
}
