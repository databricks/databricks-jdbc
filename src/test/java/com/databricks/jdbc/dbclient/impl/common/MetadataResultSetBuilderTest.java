package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class MetadataResultSetBuilderTest {

  @Test
  void testGetCode() {
    assert MetadataResultSetBuilder.getCode("STRING") == 12;
    assert MetadataResultSetBuilder.getCode("INT") == 4;
    assert MetadataResultSetBuilder.getCode("DOUBLE") == 8;
    assert MetadataResultSetBuilder.getCode("FLOAT") == 6;
    assert MetadataResultSetBuilder.getCode("BOOLEAN") == 16;
    assert MetadataResultSetBuilder.getCode("DATE") == 91;
    assert MetadataResultSetBuilder.getCode("TIMESTAMP") == 93;
    assert MetadataResultSetBuilder.getCode("DECIMAL") == 3;
    assert MetadataResultSetBuilder.getCode("BINARY") == -2;
    assert MetadataResultSetBuilder.getCode("ARRAY") == 2003;
    assert MetadataResultSetBuilder.getCode("MAP") == 2002;
    assert MetadataResultSetBuilder.getCode("STRUCT") == 2002;
    assert MetadataResultSetBuilder.getCode("UNIONTYPE") == 2002;
    assert MetadataResultSetBuilder.getCode("BYTE") == -6;
    assert MetadataResultSetBuilder.getCode("SHORT") == 5;
    assert MetadataResultSetBuilder.getCode("LONG") == -5;
    assert MetadataResultSetBuilder.getCode("NULL") == 0;
    assert MetadataResultSetBuilder.getCode("VOID") == 0;
    assert MetadataResultSetBuilder.getCode("CHAR") == 1;
    assert MetadataResultSetBuilder.getCode("VARCHAR") == 12;
    assert MetadataResultSetBuilder.getCode("CHARACTER") == 1;
    assert MetadataResultSetBuilder.getCode("BIGINT") == -5;
    assert MetadataResultSetBuilder.getCode("TINYINT") == -6;
    assert MetadataResultSetBuilder.getCode("SMALLINT") == 5;
    assert MetadataResultSetBuilder.getCode("INTEGER") == 4;
  }

  private static Stream<Arguments> charOctetArguments() {
    return Stream.of(
        Arguments.of("VARCHAR(100)", 100),
        Arguments.of("VARCHAR", 255),
        Arguments.of("CHAR(255)", 255),
        Arguments.of("CHAR", 255),
        Arguments.of("CHAR(123)", 123),
        Arguments.of("TEXT", 255),
        Arguments.of("VARCHAR(", 0),
        Arguments.of("VARCHAR(100,200)", 100),
        Arguments.of("VARCHAR(50,30)", 50),
        Arguments.of("INT", 0),
        Arguments.of("VARCHAR()", 0),
        Arguments.of("VARCHAR(abc)", 0));
  }

  private static Stream<Arguments> stripTypeNameArguments() {
    return Stream.of(
        Arguments.of("VARCHAR(100)", "VARCHAR"),
        Arguments.of("VARCHAR", "VARCHAR"),
        Arguments.of("CHAR(255)", "CHAR"),
        Arguments.of("TEXT", "TEXT"),
        Arguments.of("VARCHAR(", "VARCHAR"),
        Arguments.of("VARCHAR(100,200)", "VARCHAR"),
        Arguments.of("CHAR(123)", "CHAR"),
        Arguments.of(null, null),
        Arguments.of("", ""),
        Arguments.of("INTEGER(10,5)", "INTEGER"));
  }

  private static Stream<Arguments> getBufferLengthArguments() {
    return Stream.of(
        // Null or empty typeVal
        Arguments.of(null, 10, 0),
        Arguments.of("", 10, 0),

        // Simple types without length specification
        Arguments.of("DATE", 10, 6),
        Arguments.of("TIMESTAMP", 10, 16),
        Arguments.of("BINARY", 10, 32767),
        Arguments.of("STRING", 10, 255),
        Arguments.of("INT", 4, 4),

        // Types with length specification
        Arguments.of("CHAR(10)", 10, 10),
        Arguments.of("VARCHAR(50)", 10, 50),
        Arguments.of("DECIMAL(10,2)", 10, 40), // DECIMAL gets multiplied by 4
        Arguments.of("NUMERIC(20)", 10, 80), // NUMERIC gets multiplied by 4

        // Type with invalid length specification
        Arguments.of("VARCHAR(abc)", 10, 0),
        Arguments.of("VARCHAR()", 10, 0),
        Arguments.of("VARCHAR(100,200)", 10, 100),

        // Types without length but still valid strings
        Arguments.of("CHAR", 10, 255),
        Arguments.of("VARCHAR", 10, 255),
        Arguments.of("TEXT", 10, 255));
  }

  @ParameterizedTest
  @MethodSource("getBufferLengthArguments")
  public void testGetBufferLength(String typeVal, int columnSize, int expected) {
    int actual = MetadataResultSetBuilder.getBufferLength(typeVal, columnSize);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("stripTypeNameArguments")
  public void testStripTypeName(String input, String expected) {
    String actual = MetadataResultSetBuilder.stripTypeName(input);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("charOctetArguments")
  public void testGetCharOctetLength(String typeVal, int expected) {
    System.out.println("Incorrect getCharOctet for typeVal : " + typeVal);
    int actual = MetadataResultSetBuilder.getCharOctetLength(typeVal);
    assertEquals(expected, actual);
  }
}
