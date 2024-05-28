package com.databricks.jdbc.core;

import com.databricks.jdbc.client.impl.thrift.generated.TColumnDesc;
import com.databricks.jdbc.client.impl.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.client.impl.thrift.generated.TTableSchema;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.client.sqlexec.VolumeOperationInfo;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.ResultSchema;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DatabricksResultSetMetaDataTest {
  private static final String STATEMENT_ID = "statementId";

  public ColumnInfo getColumn(String name, ColumnInfoTypeName typeName, String typeText) {
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setName(name);
    columnInfo.setTypeName(typeName);
    columnInfo.setTypeText(typeText);
    return columnInfo;
  }

  public ResultManifest getResultManifest() {
    ResultManifest manifest = new ResultManifest();
    manifest.setTotalRowCount(10L);
    ResultSchema schema = new ResultSchema();
    schema.setColumnCount(3L);
    ColumnInfo col1 = getColumn("col1", ColumnInfoTypeName.INT, "int");
    ColumnInfo col2 = getColumn("col2", ColumnInfoTypeName.STRING, "string");
    ColumnInfo col2dup = getColumn("col2", ColumnInfoTypeName.DOUBLE, "double");
    ColumnInfo col3 = getColumn("col5", null, "double");
    schema.setColumns(List.of(col1, col2, col2dup, col3));
    manifest.setSchema(schema);
    return manifest;
  }

  public TGetResultSetMetadataResp getThriftResultManifest() {
    TGetResultSetMetadataResp resultSetMetadataResp = new TGetResultSetMetadataResp();
    TColumnDesc columnDesc = new TColumnDesc().setColumnName("testCol");
    TTableSchema schema = new TTableSchema().setColumns(Collections.singletonList(columnDesc));
    resultSetMetadataResp.setSchema(schema);
    return resultSetMetadataResp;
  }

  @Test
  public void testColumnsWithSameNameAndNullTypeName() throws SQLException {
    ResultManifest resultManifest = getResultManifest();
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, new ResultData());
    Assertions.assertEquals(4, metaData.getColumnCount());
    Assertions.assertEquals("col1", metaData.getColumnName(1));
    Assertions.assertEquals("col2", metaData.getColumnName(2));
    Assertions.assertEquals("col2", metaData.getColumnName(3));
    Assertions.assertEquals("col5", metaData.getColumnName(4));
    Assertions.assertEquals(10, metaData.getTotalRows());
    Assertions.assertEquals(2, metaData.getColumnNameIndex("col2"));

    metaData =
        new DatabricksResultSetMetaData(
            STATEMENT_ID,
            List.of("col1", "col2", "col2"),
            List.of("int", "string", "double"),
            List.of(4, 12, 8),
            List.of(0, 0, 0),
            10);
    Assertions.assertEquals(3, metaData.getColumnCount());
    Assertions.assertEquals("col1", metaData.getColumnName(1));
    Assertions.assertEquals("col2", metaData.getColumnName(2));
    Assertions.assertEquals("col2", metaData.getColumnName(3));
    Assertions.assertEquals(10, metaData.getTotalRows());
    Assertions.assertEquals(2, metaData.getColumnNameIndex("col2"));
  }

  @Test
  public void testColumnsForVolumeOperation() throws SQLException {
    ResultManifest resultManifest = getResultManifest();
    ResultData resultData =
        new ResultData()
            .setVolumeOperationInfo(
                new VolumeOperationInfo()
                    .setExternalLinks(List.of(new ExternalLink().setExternalLink("link"))));
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest, resultData);
    Assertions.assertEquals(1, metaData.getColumnCount());
    Assertions.assertEquals(
        DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME, metaData.getColumnName(1));
    Assertions.assertEquals(10, metaData.getTotalRows());
    Assertions.assertEquals(
        1,
        metaData.getColumnNameIndex(DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME));
  }

  @Test
  public void testThriftColumns() throws SQLException {
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, getThriftResultManifest(), 10, 1);
    Assertions.assertEquals(10, metaData.getTotalRows());
    Assertions.assertEquals(1, metaData.getColumnCount());
    Assertions.assertEquals("testCol", metaData.getColumnName(1));
  }

  @Test
  public void testEmptyAndNullThriftColumns() throws SQLException {
    TGetResultSetMetadataResp resultSetMetadataResp = new TGetResultSetMetadataResp();
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultSetMetadataResp, 0, 1);
    Assertions.assertEquals(0, metaData.getColumnCount());

    resultSetMetadataResp.setSchema(new TTableSchema());
    Assertions.assertEquals(0, metaData.getColumnCount());
  }
}
