package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.ResultSchema;
import java.sql.SQLException;
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
    schema.setColumns(List.of(col1, col2, col2dup));
    manifest.setSchema(schema);
    return manifest;
  }

  @Test
  public void testColumnsWithSameName() throws SQLException {
    ResultManifest resultManifest = getResultManifest();
    DatabricksResultSetMetaData metaData =
        new DatabricksResultSetMetaData(STATEMENT_ID, resultManifest);
    Assertions.assertEquals(3, metaData.getColumnCount());
    Assertions.assertEquals("col1", metaData.getColumnName(1));
    Assertions.assertEquals("col2", metaData.getColumnName(2));
    Assertions.assertEquals("col2", metaData.getColumnName(3));
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
}
