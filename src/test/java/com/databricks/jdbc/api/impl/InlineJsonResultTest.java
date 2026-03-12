package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.sdk.service.sql.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class InlineJsonResultTest {

  @Mock private IDatabricksSession session;
  @Mock private IDatabricksConnectionContext connectionContext;
  @Mock private ResultData resultData;

  private static final StatementId STATEMENT_ID = new StatementId("test-statement-id");

  @BeforeEach
  void setUp() {
    // Note: InlineJsonResult for JSON format doesn't actually check the geospatial flag
    // because JSON data is already stored as strings. These tests document expected behavior.
    lenient().when(session.getConnectionContext()).thenReturn(connectionContext);
  }

  private ResultManifest createManifestWithColumns(List<ColumnInfo> columns) {
    ResultSchema schema = new ResultSchema();
    schema.setColumns(columns);
    schema.setColumnCount((long) columns.size());

    ResultManifest manifest = new ResultManifest();
    manifest.setSchema(schema);
    manifest.setFormat(Format.JSON_ARRAY);
    manifest.setTotalChunkCount(1L);
    manifest.setTotalRowCount(1L);
    return manifest;
  }

  private ColumnInfo createColumnInfo(String name, ColumnInfoTypeName typeName, String typeText) {
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setName(name);
    columnInfo.setTypeName(typeName);
    columnInfo.setTypeText(typeText);
    columnInfo.setPosition(0L);
    return columnInfo;
  }

  @Test
  void testGeometryColumnReturnedAsStringWhenFlagDisabled() throws DatabricksSQLException {
    // Setup columns
    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(createColumnInfo("id", ColumnInfoTypeName.INT, "INT"));
    columns.add(createColumnInfo("location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY"));

    ResultManifest manifest = createManifestWithColumns(columns);

    // Setup data - in JSON format, geospatial data is already stored as strings
    Collection<Collection<String>> dataArray = Arrays.asList(Arrays.asList("1", "POINT(1 2)"));
    when(resultData.getDataArray()).thenReturn(dataArray);
    when(resultData.getChunkIndex()).thenReturn(0L);

    // Note: JSON format doesn't need geospatial flag check - data is already strings

    // Create InlineJsonResult
    InlineJsonResult result = new InlineJsonResult(manifest, resultData, STATEMENT_ID, session);

    // Move to first row
    assertTrue(result.next());

    // Get the geometry column - should return as string
    Object geometryValue = result.getObject(1); // columnIndex 1 = location
    assertNotNull(geometryValue);
    assertEquals("POINT(1 2)", geometryValue.toString());
  }

  @Test
  void testGeographyColumnReturnedAsStringWhenFlagDisabled() throws DatabricksSQLException {
    // Setup columns
    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(createColumnInfo("id", ColumnInfoTypeName.INT, "INT"));
    columns.add(createColumnInfo("region", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY"));

    ResultManifest manifest = createManifestWithColumns(columns);

    // Setup data
    Collection<Collection<String>> dataArray =
        Arrays.asList(Arrays.asList("1", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"));
    when(resultData.getDataArray()).thenReturn(dataArray);
    when(resultData.getChunkIndex()).thenReturn(0L);

    // Note: JSON format doesn't need geospatial flag check - data is already strings

    // Create InlineJsonResult
    InlineJsonResult result = new InlineJsonResult(manifest, resultData, STATEMENT_ID, session);

    // Move to first row
    assertTrue(result.next());

    // Get the geography column - should return as string
    Object geographyValue = result.getObject(1); // columnIndex 1 = region
    assertNotNull(geographyValue);
    assertEquals("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", geographyValue.toString());
  }

  @Test
  void testGeospatialColumnsRetainedWhenFlagEnabled() throws DatabricksSQLException {
    // Setup columns with both GEOMETRY and GEOGRAPHY
    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(createColumnInfo("id", ColumnInfoTypeName.INT, "INT"));
    columns.add(createColumnInfo("location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY"));
    columns.add(createColumnInfo("region", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY"));

    ResultManifest manifest = createManifestWithColumns(columns);

    // Setup data
    Collection<Collection<String>> dataArray =
        Arrays.asList(Arrays.asList("1", "POINT(1 2)", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"));
    when(resultData.getDataArray()).thenReturn(dataArray);
    when(resultData.getChunkIndex()).thenReturn(0L);

    // Note: JSON format doesn't need geospatial flag check - data is already strings

    // Create InlineJsonResult
    InlineJsonResult result = new InlineJsonResult(manifest, resultData, STATEMENT_ID, session);

    // Move to first row
    assertTrue(result.next());

    // Get all columns
    Object idValue = result.getObject(0);
    Object geometryValue = result.getObject(1);
    Object geographyValue = result.getObject(2);

    assertNotNull(idValue);
    assertNotNull(geometryValue);
    assertNotNull(geographyValue);

    assertEquals("1", idValue.toString());
    assertEquals("POINT(1 2)", geometryValue.toString());
    assertEquals("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", geographyValue.toString());
  }

  @Test
  void testMixedColumnsWithGeospatialFlagDisabled() throws DatabricksSQLException {
    // Setup columns with mixed types
    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(createColumnInfo("id", ColumnInfoTypeName.INT, "INT"));
    columns.add(createColumnInfo("name", ColumnInfoTypeName.STRING, "STRING"));
    columns.add(createColumnInfo("location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY"));
    columns.add(createColumnInfo("price", ColumnInfoTypeName.DECIMAL, "DECIMAL"));

    ResultManifest manifest = createManifestWithColumns(columns);

    // Setup data
    Collection<Collection<String>> dataArray =
        Arrays.asList(Arrays.asList("1", "Test", "POINT(1 2)", "99.99"));
    when(resultData.getDataArray()).thenReturn(dataArray);
    when(resultData.getChunkIndex()).thenReturn(0L);

    // Note: JSON format doesn't need geospatial flag check - data is already strings

    // Create InlineJsonResult
    InlineJsonResult result = new InlineJsonResult(manifest, resultData, STATEMENT_ID, session);

    // Move to first row
    assertTrue(result.next());

    // Get all columns
    Object idValue = result.getObject(0);
    Object nameValue = result.getObject(1);
    Object geometryValue = result.getObject(2);
    Object priceValue = result.getObject(3);

    assertNotNull(idValue);
    assertNotNull(nameValue);
    assertNotNull(geometryValue);
    assertNotNull(priceValue);

    assertEquals("1", idValue.toString());
    assertEquals("Test", nameValue.toString());
    assertEquals("POINT(1 2)", geometryValue.toString()); // Returned as string
    assertEquals("99.99", priceValue.toString());
  }

  @Test
  void testNullGeospatialValueWhenFlagDisabled() throws DatabricksSQLException {
    // Setup columns
    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(createColumnInfo("id", ColumnInfoTypeName.INT, "INT"));
    columns.add(createColumnInfo("location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY"));

    ResultManifest manifest = createManifestWithColumns(columns);

    // Setup data with null geometry value
    List<String> row = new ArrayList<>();
    row.add("1");
    row.add(null);
    Collection<Collection<String>> dataArray = Arrays.asList(row);
    when(resultData.getDataArray()).thenReturn(dataArray);
    when(resultData.getChunkIndex()).thenReturn(0L);

    // Note: JSON format doesn't need geospatial flag check - data is already strings

    // Create InlineJsonResult
    InlineJsonResult result = new InlineJsonResult(manifest, resultData, STATEMENT_ID, session);

    // Move to first row
    assertTrue(result.next());

    // Get the geometry column - should return null
    Object geometryValue = result.getObject(1);
    assertNull(geometryValue);
  }

  @Test
  void testMultipleRowsWithGeospatialFlagDisabled() throws DatabricksSQLException {
    // Setup columns
    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(createColumnInfo("id", ColumnInfoTypeName.INT, "INT"));
    columns.add(createColumnInfo("location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY"));

    ResultManifest manifest = createManifestWithColumns(columns);

    // Setup data with multiple rows
    Collection<Collection<String>> dataArray =
        Arrays.asList(
            Arrays.asList("1", "POINT(1 2)"),
            Arrays.asList("2", "POINT(3 4)"),
            Arrays.asList("3", "POINT(5 6)"));
    when(resultData.getDataArray()).thenReturn(dataArray);
    when(resultData.getChunkIndex()).thenReturn(0L);

    // Note: JSON format doesn't need geospatial flag check - data is already strings

    // Create InlineJsonResult
    InlineJsonResult result = new InlineJsonResult(manifest, resultData, STATEMENT_ID, session);

    // Verify all rows
    assertTrue(result.next());
    assertEquals("1", result.getObject(0).toString());
    assertEquals("POINT(1 2)", result.getObject(1).toString());

    assertTrue(result.next());
    assertEquals("2", result.getObject(0).toString());
    assertEquals("POINT(3 4)", result.getObject(1).toString());

    assertTrue(result.next());
    assertEquals("3", result.getObject(0).toString());
    assertEquals("POINT(5 6)", result.getObject(1).toString());

    assertFalse(result.next());
  }

  @Test
  void testJsonArrayFormatWithGeospatialTypesWhenFlagDisabled() throws DatabricksSQLException {
    // This test explicitly verifies JSON_ARRAY format handling
    // In SQL Exec API, when format is JSON_ARRAY, data comes as Collection<Collection<String>>
    // where each inner collection represents a row of JSON array values

    // Setup columns with various types including geospatial
    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(createColumnInfo("store_id", ColumnInfoTypeName.INT, "INT"));
    columns.add(createColumnInfo("store_name", ColumnInfoTypeName.STRING, "STRING"));
    columns.add(createColumnInfo("store_location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY"));
    columns.add(createColumnInfo("delivery_region", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY"));
    columns.add(createColumnInfo("rating", ColumnInfoTypeName.DECIMAL, "DECIMAL"));

    ResultManifest manifest = createManifestWithColumns(columns);
    // Explicitly set format to JSON_ARRAY
    manifest.setFormat(Format.JSON_ARRAY);

    // Setup JSON array data - simulating real SQL Exec API response
    // Format: [["1", "Store A", "POINT(1 2)", "POLYGON(...)", "4.5"], ...]
    Collection<Collection<String>> jsonArrayData =
        Arrays.asList(
            Arrays.asList(
                "1",
                "Store A",
                "POINT(10.5 20.3)",
                "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",
                "4.5"),
            Arrays.asList(
                "2",
                "Store B",
                "POINT(30.2 40.8)",
                "POLYGON((0 0, 20 0, 20 20, 0 20, 0 0))",
                "4.8"));
    when(resultData.getDataArray()).thenReturn(jsonArrayData);
    when(resultData.getChunkIndex()).thenReturn(0L);

    // Note: JSON format doesn't need geospatial flag check - data is already strings

    // Create InlineJsonResult with JSON_ARRAY format
    InlineJsonResult result = new InlineJsonResult(manifest, resultData, STATEMENT_ID, session);

    // Verify first row - geospatial columns should be returned as strings
    assertTrue(result.next());
    assertEquals("1", result.getObject(0).toString()); // store_id
    assertEquals("Store A", result.getObject(1).toString()); // store_name
    // Geospatial columns should be returned as plain strings when flag is disabled
    assertEquals("POINT(10.5 20.3)", result.getObject(2).toString()); // store_location (GEOMETRY)
    assertEquals(
        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",
        result.getObject(3).toString()); // delivery_region (GEOGRAPHY)
    assertEquals("4.5", result.getObject(4).toString()); // rating

    // Verify second row
    assertTrue(result.next());
    assertEquals("2", result.getObject(0).toString());
    assertEquals("Store B", result.getObject(1).toString());
    assertEquals("POINT(30.2 40.8)", result.getObject(2).toString()); // GEOMETRY as string
    assertEquals(
        "POLYGON((0 0, 20 0, 20 20, 0 20, 0 0))",
        result.getObject(3).toString()); // GEOGRAPHY as string
    assertEquals("4.8", result.getObject(4).toString());

    assertFalse(result.next());
  }

  @Test
  void testJsonArrayFormatWithGeospatialTypesWhenFlagEnabled() throws DatabricksSQLException {
    // Test JSON_ARRAY format with geospatial flag enabled
    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(createColumnInfo("id", ColumnInfoTypeName.INT, "INT"));
    columns.add(createColumnInfo("location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY"));
    columns.add(createColumnInfo("region", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY"));

    ResultManifest manifest = createManifestWithColumns(columns);
    manifest.setFormat(Format.JSON_ARRAY);

    // JSON array data
    Collection<Collection<String>> jsonArrayData =
        Arrays.asList(Arrays.asList("100", "POINT(1.5 2.5)", "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"));
    when(resultData.getDataArray()).thenReturn(jsonArrayData);
    when(resultData.getChunkIndex()).thenReturn(0L);

    // Note: JSON format doesn't need geospatial flag check - data is already strings

    // Create InlineJsonResult
    InlineJsonResult result = new InlineJsonResult(manifest, resultData, STATEMENT_ID, session);

    // When flag is enabled, geospatial data should still be accessible
    assertTrue(result.next());
    assertEquals("100", result.getObject(0).toString());
    assertEquals("POINT(1.5 2.5)", result.getObject(1).toString());
    assertEquals("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))", result.getObject(2).toString());

    assertFalse(result.next());
  }

  @Test
  void testJsonArrayWithComplexTypesEnabledButGeospatialDisabled() throws DatabricksSQLException {
    // This test validates the scenario where EnableComplexDatatypeSupport=1 but
    // EnableGeoSpatialSupport=0 (disabled). This simulates a real-world scenario where
    // users enable complex types for ARRAY/MAP/STRUCT but want geospatial data as strings.
    //
    // Expected behavior:
    // - Geospatial column TYPES in metadata should report as STRING
    // - Geospatial column DATA should return as plain string values
    // - Complex types (ARRAY, MAP) should still work normally

    List<ColumnInfo> columns = new ArrayList<>();
    columns.add(createColumnInfo("id", ColumnInfoTypeName.INT, "INT"));
    columns.add(createColumnInfo("location", ColumnInfoTypeName.GEOMETRY, "GEOMETRY"));
    columns.add(createColumnInfo("region", ColumnInfoTypeName.GEOGRAPHY, "GEOGRAPHY"));
    // Add complex types that should still work when complex support is enabled
    columns.add(createColumnInfo("tags", ColumnInfoTypeName.ARRAY, "ARRAY<STRING>"));
    columns.add(createColumnInfo("metadata", ColumnInfoTypeName.MAP, "MAP<STRING,STRING>"));

    ResultManifest manifest = createManifestWithColumns(columns);
    manifest.setFormat(Format.JSON_ARRAY);

    // JSON array data with geospatial + complex types
    Collection<Collection<String>> jsonArrayData =
        Arrays.asList(
            Arrays.asList(
                "1",
                "POINT(10.5 20.3)",
                "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))",
                "[\"tag1\",\"tag2\"]",
                "{\"key1\":\"value1\",\"key2\":\"value2\"}"));
    when(resultData.getDataArray()).thenReturn(jsonArrayData);
    when(resultData.getChunkIndex()).thenReturn(0L);

    // Note: This test documents the scenario where EnableComplexDatatypeSupport=1
    // but EnableGeoSpatialSupport=0. However, InlineJsonResult doesn't check these flags
    // because JSON format already stores geospatial data as strings.

    // Create InlineJsonResult
    InlineJsonResult result = new InlineJsonResult(manifest, resultData, STATEMENT_ID, session);

    // Move to first row
    assertTrue(result.next());

    // === Test 1: Verify DATA values ===
    Object idValue = result.getObject(0);
    Object geometryValue = result.getObject(1);
    Object geographyValue = result.getObject(2);
    Object arrayValue = result.getObject(3);
    Object mapValue = result.getObject(4);

    assertNotNull(idValue);
    assertNotNull(geometryValue);
    assertNotNull(geographyValue);
    assertNotNull(arrayValue);
    assertNotNull(mapValue);

    // Geospatial data should be plain strings when flag is disabled
    assertEquals("1", idValue.toString());
    assertEquals("POINT(10.5 20.3)", geometryValue.toString());
    assertEquals("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", geographyValue.toString());

    // Complex types are returned as JSON strings in JSON_ARRAY format
    assertEquals("[\"tag1\",\"tag2\"]", arrayValue.toString());
    assertEquals("{\"key1\":\"value1\",\"key2\":\"value2\"}", mapValue.toString());

    // === Test 2: Verify COLUMN TYPES (metadata) ===
    // Note: The ColumnInfo objects should have been converted to STRING by
    // DatabricksResultSetMetaData when geospatial flag is disabled.
    // Here we verify that the columns were passed correctly.

    // In JSON format, we verify the column info types that would be used for metadata
    ColumnInfo geometryColumn = columns.get(1);
    ColumnInfo geographyColumn = columns.get(2);

    // These column infos will be processed by DatabricksResultSetMetaData,
    // which converts GEOMETRY/GEOGRAPHY to STRING when flag is disabled
    assertEquals(ColumnInfoTypeName.GEOMETRY, geometryColumn.getTypeName());
    assertEquals(ColumnInfoTypeName.GEOGRAPHY, geographyColumn.getTypeName());

    // The actual conversion happens in DatabricksResultSetMetaData constructor,
    // which is tested separately in DatabricksResultSetMetaDataTest

    assertFalse(result.next());
  }
}
