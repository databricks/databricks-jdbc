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
        Arguments.of("VARCHAR", 0),
        Arguments.of("CHAR(255)", 255),
        Arguments.of("CHAR", 0),
        Arguments.of("CHAR(123)", 123),
        Arguments.of("TEXT", 0),
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
