package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EmptyResultSetMetaDataTest {

  private EmptyResultSetMetaData metaData;

  @BeforeEach
  void setUp() {
    metaData = new EmptyResultSetMetaData();
  }

  @Test
  void testGetColumnCount() throws SQLException {
    assertEquals(0, metaData.getColumnCount(), "Column count should be 0");
  }

  @Test
  void testIsAutoIncrement() throws SQLException {
    assertFalse(metaData.isAutoIncrement(1), "AutoIncrement should be false");
  }

  @Test
  void testIsCaseSensitive() throws SQLException {
    assertFalse(metaData.isCaseSensitive(1), "CaseSensitive should be false");
  }

  @Test
  void testIsSearchable() throws SQLException {
    assertFalse(metaData.isSearchable(1), "Searchable should be false");
  }

  @Test
  void testIsCurrency() throws SQLException {
    assertFalse(metaData.isCurrency(1), "Currency should be false");
  }

  @Test
  void testIsNullable() throws SQLException {
    assertEquals(
        ResultSetMetaData.columnNullable,
        metaData.isNullable(1),
        "Nullable should be columnNullable");
  }

  @Test
  void testIsSigned() throws SQLException {
    assertFalse(metaData.isSigned(1), "Signed should be false");
  }

  @Test
  void testGetColumnDisplaySize() throws SQLException {
    assertEquals(0, metaData.getColumnDisplaySize(1), "Column display size should be 0");
  }

  @Test
  void testGetColumnLabel() throws SQLException {
    assertEquals("", metaData.getColumnLabel(1), "Column label should be empty");
  }

  @Test
  void testGetColumnName() throws SQLException {
    assertEquals("", metaData.getColumnName(1), "Column name should be empty");
  }

  @Test
  void testGetSchemaName() throws SQLException {
    assertEquals("", metaData.getSchemaName(1), "Schema name should be empty");
  }

  @Test
  void testGetPrecision() throws SQLException {
    assertEquals(0, metaData.getPrecision(1), "Precision should be 0");
  }

  @Test
  void testGetScale() throws SQLException {
    assertEquals(0, metaData.getScale(1), "Scale should be 0");
  }

  @Test
  void testGetTableName() throws SQLException {
    assertEquals("", metaData.getTableName(1), "Table name should be empty");
  }

  @Test
  void testGetCatalogName() throws SQLException {
    assertEquals("", metaData.getCatalogName(1), "Catalog name should be empty");
  }

  @Test
  void testGetColumnType() throws SQLException {
    assertEquals(Types.VARCHAR, metaData.getColumnType(1), "Column type should be VARCHAR");
  }

  @Test
  void testGetColumnTypeName() throws SQLException {
    assertEquals("", metaData.getColumnTypeName(1), "Column type name should be empty");
  }

  @Test
  void testIsReadOnly() throws SQLException {
    assertTrue(metaData.isReadOnly(1), "ReadOnly should be true");
  }

  @Test
  void testIsWritable() throws SQLException {
    assertFalse(metaData.isWritable(1), "Writable should be false");
  }

  @Test
  void testIsDefinitelyWritable() throws SQLException {
    assertFalse(metaData.isDefinitelyWritable(1), "DefinitelyWritable should be false");
  }

  @Test
  void testGetColumnClassName() throws SQLException {
    assertEquals("", metaData.getColumnClassName(1), "Column class name should be empty");
  }

  @Test
  void testUnwrap() throws SQLException {
    assertEquals(
        metaData,
        metaData.unwrap(EmptyResultSetMetaData.class),
        "Unwrap should return the same instance");
  }

  @Test
  void testIsWrapperFor() throws SQLException {
    assertTrue(
        metaData.isWrapperFor(EmptyResultSetMetaData.class),
        "Should be a wrapper for EmptyResultSetMetaData");
    assertFalse(
        metaData.isWrapperFor(DatabricksResultSetMetaData.class),
        "Should not be a wrapper for ResultSetMetaData");
  }

  @Test
  void testEquals() {
    assertEquals(new EmptyResultSetMetaData(), metaData, "Equal objects should be equal");
    assertNotEquals(null, metaData, "Should not be equal to null");
    assertNotEquals("String", metaData, "Should not be equal to a different type");
  }
}
