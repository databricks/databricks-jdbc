package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import java.io.ByteArrayInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** Unit tests for ArrowUtil. */
public class ArrowUtilTest {

  @Test
  void testGetSerializedSchemaWithPreSerializedArrow() throws DatabricksParsingException {
    byte[] preSerializedSchema = new byte[] {1, 2, 3, 4, 5};

    TGetResultSetMetadataResp metadata = new TGetResultSetMetadataResp();
    metadata.setArrowSchema(preSerializedSchema);

    byte[] result = ArrowUtil.getSerializedSchema(metadata);

    assertArrayEquals(preSerializedSchema, result);
  }

  @Test
  void testGetSerializedSchemaWithNullMetadata() throws DatabricksParsingException {
    byte[] result = ArrowUtil.getSerializedSchema(null);

    assertNotNull(result);
    assertEquals(0, result.length);
  }

  @Test
  void testGetSerializedSchemaWithHiveSchemaFallback() throws DatabricksParsingException {
    // Create a Hive schema with a simple string column
    TTableSchema hiveSchema = createHiveSchema("test_column", TTypeId.STRING_TYPE);

    TGetResultSetMetadataResp metadata = new TGetResultSetMetadataResp();
    metadata.setSchema(hiveSchema);
    // Don't set arrowSchema - should trigger fallback

    byte[] result = ArrowUtil.getSerializedSchema(metadata);

    assertNotNull(result);
    assertTrue(result.length > 0, "Serialized schema should not be empty");
  }

  @Test
  void testHiveSchemaToArrowSchemaWithMultipleColumns() throws DatabricksParsingException {
    // Create a Hive schema with multiple columns of different types
    TTableSchema hiveSchema = new TTableSchema();
    List<TColumnDesc> columns = new ArrayList<>();

    columns.add(createColumnDesc("string_col", TTypeId.STRING_TYPE));
    columns.add(createColumnDesc("int_col", TTypeId.INT_TYPE));
    columns.add(createColumnDesc("boolean_col", TTypeId.BOOLEAN_TYPE));

    hiveSchema.setColumns(columns);

    Schema arrowSchema = ArrowUtil.hiveSchemaToArrowSchema(hiveSchema);

    assertNotNull(arrowSchema);
    assertEquals(3, arrowSchema.getFields().size());
    assertEquals("string_col", arrowSchema.getFields().get(0).getName());
    assertEquals("int_col", arrowSchema.getFields().get(1).getName());
    assertEquals("boolean_col", arrowSchema.getFields().get(2).getName());
  }

  @Test
  void testHiveSchemaToArrowSchemaWithNullSchema() throws DatabricksParsingException {
    Schema arrowSchema = ArrowUtil.hiveSchemaToArrowSchema(null);

    assertNotNull(arrowSchema);
    assertEquals(0, arrowSchema.getFields().size());
  }

  @Test
  void testColumnDescToArrowField() throws SQLException {
    TColumnDesc columnDesc = createColumnDesc("my_column", TTypeId.STRING_TYPE);

    Field field = ArrowUtil.columnDescToArrowField(columnDesc);

    assertNotNull(field);
    assertEquals("my_column", field.getName());
    assertTrue(field.isNullable());
    assertNotNull(field.getType());
  }

  @Test
  void testCreateArrowByteStream() throws DatabricksParsingException {
    byte[] schema = new byte[] {1, 2, 3};

    TFetchResultsResp response = new TFetchResultsResp();
    TRowSet rowSet = new TRowSet();
    rowSet.setArrowBatches(Collections.emptyList());
    response.setResults(rowSet);

    // Set up metadata with no compression
    TGetResultSetMetadataResp metadata = new TGetResultSetMetadataResp();
    response.setResultSetMetadata(metadata);

    ByteArrayInputStream result = ArrowUtil.createArrowByteStream(schema, response, getClass());

    assertNotNull(result);
    // Should at least contain the schema bytes
    assertTrue(result.available() >= schema.length);
  }

  @Test
  void testGetTotalRowsInResponse() {
    TFetchResultsResp response = new TFetchResultsResp();
    TRowSet rowSet = new TRowSet();

    List<TSparkArrowBatch> batches = new ArrayList<>();
    TSparkArrowBatch batch1 = new TSparkArrowBatch();
    batch1.setRowCount(100);
    batch1.setBatch(new byte[0]);
    batches.add(batch1);

    TSparkArrowBatch batch2 = new TSparkArrowBatch();
    batch2.setRowCount(50);
    batch2.setBatch(new byte[0]);
    batches.add(batch2);

    rowSet.setArrowBatches(batches);
    response.setResults(rowSet);

    long totalRows = ArrowUtil.getTotalRowsInResponse(response);

    assertEquals(150, totalRows);
  }

  @Test
  void testGetTotalRowsInResponseWithNullResults() {
    TFetchResultsResp response = new TFetchResultsResp();
    // Don't set results

    long totalRows = ArrowUtil.getTotalRowsInResponse(response);

    assertEquals(0, totalRows);
  }

  // ==================== Helper Methods ====================

  private TTableSchema createHiveSchema(String columnName, TTypeId typeId) {
    TTableSchema schema = new TTableSchema();
    List<TColumnDesc> columns = new ArrayList<>();
    columns.add(createColumnDesc(columnName, typeId));
    schema.setColumns(columns);
    return schema;
  }

  private TColumnDesc createColumnDesc(String name, TTypeId typeId) {
    TColumnDesc columnDesc = new TColumnDesc();
    columnDesc.setColumnName(name);

    TTypeDesc typeDesc = new TTypeDesc();
    List<TTypeEntry> typeEntries = new ArrayList<>();

    TTypeEntry typeEntry = new TTypeEntry();
    TPrimitiveTypeEntry primitiveType = new TPrimitiveTypeEntry();
    primitiveType.setType(typeId);
    typeEntry.setPrimitiveEntry(primitiveType);
    typeEntries.add(typeEntry);

    typeDesc.setTypes(typeEntries);
    columnDesc.setTypeDesc(typeDesc);

    return columnDesc;
  }
}
