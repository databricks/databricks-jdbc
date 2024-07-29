package com.databricks.jdbc;

import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.core.ImmutableSessionInfo;
import com.databricks.jdbc.core.types.AllPurposeCluster;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.core.types.Warehouse;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/*All the common test constants should be placed here */
public class TestConstants {
  public static final String WAREHOUSE_ID = "warehouse_id";
  public static final String SESSION_ID = "12345678";
  private static final String CATALOG = "field_demos";
  private static final String SCHEMA = "ossjdbc";
  public static final Warehouse WAREHOUSE_COMPUTE = new Warehouse(WAREHOUSE_ID);
  public static final ComputeResource CLUSTER_COMPUTE =
      new AllPurposeCluster("6051921418418893", "1115-130834-ms4m0yv");
  public static final String TEST_SCHEMA = "testSchema";
  public static final String TEST_TABLE = "testTable";
  public static final Map<String, String> EMPTY_MAP = Collections.emptyMap();
  public static final String TEST_COLUMN = "testColumn";
  public static final String TEST_CATALOG = "catalog1";
  public static final String TEST_FUNCTION_PATTERN = "functionPattern";
  public static final String TEST_STRING = "test";
  public static final String TEST_STATEMENT_ID = "testStatementId";
  public static final String UC_VOLUME_CATALOG = "uc_volume_test_catalog";
  public static final String UC_VOLUME_SCHEMA = "uc_volume_test_schema";

  public static final TSessionHandle SESSION_HANDLE =
      new TSessionHandle().setSessionId(new THandleIdentifier().setGuid(SESSION_ID.getBytes()));
  public static final ImmutableSessionInfo SESSION_INFO =
      ImmutableSessionInfo.builder()
          .sessionHandle(SESSION_HANDLE)
          .sessionId(SESSION_ID)
          .computeResource(CLUSTER_COMPUTE)
          .build();
  public static final String WAREHOUSE_JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/warehouse_id;";

  public static final String WAREHOUSE_JDBC_URL_WITH_THRIFT =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/warehouse_id;UseThriftClient=1;";

  public static final TRowSet binaryRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.binaryVal(
                      new TBinaryColumn()
                          .setValues(
                              Collections.singletonList(
                                  ByteBuffer.wrap(TEST_STRING.getBytes()))))));
  public static final TRowSet boolRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.boolVal(
                      new TBoolColumn().setValues(Arrays.asList(false, true, false, true)))));
  public static final TRowSet byteRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.byteVal(new TByteColumn().setValues(Arrays.asList((byte) 5)))));
  public static final TRowSet doubleRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.doubleVal(
                      new TDoubleColumn().setValues(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)))));
  public static final TRowSet i16RowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.i16Val(new TI16Column().setValues(Arrays.asList((short) 1)))));
  public static final TRowSet i32RowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.i32Val(new TI32Column().setValues(Arrays.asList(1)))));
  public static final TRowSet i64RowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.i64Val(new TI64Column().setValues(Arrays.asList(1L, 5L)))));

  public static final TRowSet stringRowSet =
      new TRowSet()
          .setColumns(
              Collections.singletonList(
                  TColumn.stringVal(
                      new TStringColumn().setValues(Arrays.asList(TEST_STRING, TEST_STRING)))));

  private static final TColumnDesc TEST_COLUMN_DESCRIPTION =
      new TColumnDesc().setColumnName("testCol");
  public static final TTableSchema TEST_TABLE_SCHEMA =
      new TTableSchema().setColumns(Collections.singletonList(TEST_COLUMN_DESCRIPTION));
  public static final byte[] TEST_BYTES =
      ByteBuffer.allocate(Long.BYTES).putLong(123456789L).array();
}
