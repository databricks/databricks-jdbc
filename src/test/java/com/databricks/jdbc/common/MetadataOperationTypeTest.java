package com.databricks.jdbc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Tests for {@link MetadataOperationType} enum. */
public class MetadataOperationTypeTest {

  @Test
  void testAllEnumValuesExist() {
    // Verify all expected enum values exist
    assertEquals(9, MetadataOperationType.values().length);
    assertNotNull(MetadataOperationType.GET_CATALOGS);
    assertNotNull(MetadataOperationType.GET_SCHEMAS);
    assertNotNull(MetadataOperationType.GET_TABLES);
    assertNotNull(MetadataOperationType.GET_COLUMNS);
    assertNotNull(MetadataOperationType.GET_FUNCTIONS);
    assertNotNull(MetadataOperationType.GET_PRIMARY_KEYS);
    assertNotNull(MetadataOperationType.GET_CROSS_REFERENCE);
    assertNotNull(MetadataOperationType.GET_PROCEDURES);
    assertNotNull(MetadataOperationType.GET_PROCEDURE_COLUMNS);
  }

  @ParameterizedTest
  @CsvSource({
    "GET_CATALOGS, GetCatalogs",
    "GET_SCHEMAS, GetSchemas",
    "GET_TABLES, GetTables",
    "GET_COLUMNS, GetColumns",
    "GET_FUNCTIONS, GetFunctions",
    "GET_PRIMARY_KEYS, GetPrimaryKeys",
    "GET_CROSS_REFERENCE, GetCrossReference",
    "GET_PROCEDURES, GetProcedures",
    "GET_PROCEDURE_COLUMNS, GetProcedureColumns"
  })
  void testHeaderValues(String enumName, String expectedHeaderValue) {
    MetadataOperationType operationType = MetadataOperationType.valueOf(enumName);
    assertEquals(expectedHeaderValue, operationType.getHeaderValue());
  }

  @Test
  void testGetCatalogsHeaderValue() {
    assertEquals("GetCatalogs", MetadataOperationType.GET_CATALOGS.getHeaderValue());
  }

  @Test
  void testGetSchemasHeaderValue() {
    assertEquals("GetSchemas", MetadataOperationType.GET_SCHEMAS.getHeaderValue());
  }

  @Test
  void testGetTablesHeaderValue() {
    assertEquals("GetTables", MetadataOperationType.GET_TABLES.getHeaderValue());
  }

  @Test
  void testGetColumnsHeaderValue() {
    assertEquals("GetColumns", MetadataOperationType.GET_COLUMNS.getHeaderValue());
  }

  @Test
  void testGetFunctionsHeaderValue() {
    assertEquals("GetFunctions", MetadataOperationType.GET_FUNCTIONS.getHeaderValue());
  }

  @Test
  void testGetPrimaryKeysHeaderValue() {
    assertEquals("GetPrimaryKeys", MetadataOperationType.GET_PRIMARY_KEYS.getHeaderValue());
  }

  @Test
  void testGetCrossReferenceHeaderValue() {
    assertEquals("GetCrossReference", MetadataOperationType.GET_CROSS_REFERENCE.getHeaderValue());
  }

  @Test
  void testGetProceduresHeaderValue() {
    assertEquals("GetProcedures", MetadataOperationType.GET_PROCEDURES.getHeaderValue());
  }

  @Test
  void testGetProcedureColumnsHeaderValue() {
    assertEquals(
        "GetProcedureColumns", MetadataOperationType.GET_PROCEDURE_COLUMNS.getHeaderValue());
  }
}
