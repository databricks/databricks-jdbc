package com.databricks.jdbc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Tests for {@link MetadataOperationType} enum. */
public class MetadataOperationTypeTest {

  @Test
  void testAllEnumValuesExist() {
    assertEquals(9, MetadataOperationType.values().length);
  }

  @ParameterizedTest
  @CsvSource({
    "GET_CATALOGS, GetCatalogs, true",
    "GET_SCHEMAS, GetSchemas, true",
    "GET_TABLES, GetTables, true",
    "GET_COLUMNS, GetColumns, true",
    "GET_FUNCTIONS, GetFunctions, true",
    "GET_PRIMARY_KEYS, GetPrimaryKeys, true",
    "GET_CROSS_REFERENCE, GetCrossReference, true",
    "GET_PROCEDURES, GetProcedures, false",
    "GET_PROCEDURE_COLUMNS, GetProcedureColumns, false"
  })
  void testHeaderAndThriftNativeSupport(
      String enumName, String expectedHeaderValue, boolean thriftNativeSupported) {
    MetadataOperationType operationType = MetadataOperationType.valueOf(enumName);
    assertEquals(expectedHeaderValue, operationType.getHeaderValue());
    assertEquals(thriftNativeSupported, operationType.isThriftNativeSupported());
  }
}
