package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_RESULT_ROW_LIMIT;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.checkDirectResultsForErrorStatus;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.StatementState;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksThriftUtilTest {

  @Mock TFetchResultsResp fetchResultsResp;
  @Mock IDatabricksStatementInternal parentStatement;
  @Mock IDatabricksSession session;

  @Test
  void testByteBufferToString() {
    DatabricksThriftUtil helper = new DatabricksThriftUtil(); // cover the constructors too
    long expectedLong = 123456789L;
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(expectedLong);
    buffer.flip();
    String result = helper.byteBufferToString(buffer);
    String expectedUUID = new UUID(expectedLong, expectedLong).toString();
    assertEquals(expectedUUID, result);
  }

  @Test
  void testVerifySuccessStatus() {
    assertDoesNotThrow(
        () ->
            DatabricksThriftUtil.verifySuccessStatus(
                new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS), "test"));
    assertDoesNotThrow(
        () ->
            DatabricksThriftUtil.verifySuccessStatus(
                new TStatus().setStatusCode(TStatusCode.SUCCESS_WITH_INFO_STATUS), "test"));

    DatabricksSQLException exception =
        assertThrows(
            DatabricksHttpException.class,
            () ->
                DatabricksThriftUtil.verifySuccessStatus(
                    new TStatus().setStatusCode(TStatusCode.ERROR_STATUS).setSqlState(null),
                    "test"));
    assertNull(exception.getSQLState());

    exception =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                DatabricksThriftUtil.verifySuccessStatus(
                    new TStatus().setStatusCode(TStatusCode.ERROR_STATUS).setSqlState("42S02"),
                    "test"));

    assertEquals("42S02", exception.getSQLState());
  }

  private static Stream<Arguments> resultDataTypes() {
    // Create a test row set with chunk links
    TSparkArrowResultLink sampleResultLink = new TSparkArrowResultLink().setRowCount(10);
    sampleResultLink.setRowCountIsSet(true);
    TRowSet resultChunkRowSet =
        new TRowSet().setResultLinks(Collections.singletonList(sampleResultLink));
    resultChunkRowSet.setResultLinksIsSet(true);

    return Stream.of(
        Arguments.of(new TRowSet(), 0),
        Arguments.of(new TRowSet().setColumns(Collections.emptyList()), 0),
        Arguments.of(resultChunkRowSet, 10),
        Arguments.of(BOOL_ROW_SET, BOOL_ROW_SET_VALUES.size()),
        Arguments.of(BYTE_ROW_SET, BYTE_ROW_SET_VALUES.size()),
        Arguments.of(DOUBLE_ROW_SET, DOUBLE_ROW_SET_VALUES.size()),
        Arguments.of(I16_ROW_SET, SHORT_ROW_SET_VALUES.size()),
        Arguments.of(I32_ROW_SET, INT_ROW_SET_VALUES.size()),
        Arguments.of(I64_ROW_SET, LONG_ROW_SET_VALUES.size()),
        Arguments.of(STRING_ROW_SET, STRING_ROW_SET_VALUES.size()),
        Arguments.of(BINARY_ROW_SET, BINARY_ROW_SET_VALUES.size()),
        Arguments.of(MIXED_ROW_SET, MIXED_ROW_SET_COUNT));
  }

  private static Stream<Arguments> thriftDirectResultSets() {
    return Stream.of(
        Arguments.of(
            new TSparkDirectResults()
                .setResultSet(
                    new TFetchResultsResp()
                        .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS)))),
        Arguments.of(
            new TSparkDirectResults()
                .setCloseOperation(
                    new TCloseOperationResp()
                        .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS)))),
        Arguments.of(
            new TSparkDirectResults()
                .setOperationStatus(
                    new TGetOperationStatusResp()
                        .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS)))),
        Arguments.of(
            new TSparkDirectResults()
                .setResultSet(
                    new TFetchResultsResp()
                        .setStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS)))));
  }

  private static Stream<Arguments> typeIdAndColumnInfoType() {
    return Stream.of(
        Arguments.of(TTypeId.BOOLEAN_TYPE, ColumnInfoTypeName.BOOLEAN),
        Arguments.of(TTypeId.TINYINT_TYPE, ColumnInfoTypeName.BYTE),
        Arguments.of(TTypeId.SMALLINT_TYPE, ColumnInfoTypeName.SHORT),
        Arguments.of(TTypeId.INT_TYPE, ColumnInfoTypeName.INT),
        Arguments.of(TTypeId.BIGINT_TYPE, ColumnInfoTypeName.LONG),
        Arguments.of(TTypeId.FLOAT_TYPE, ColumnInfoTypeName.FLOAT),
        Arguments.of(TTypeId.VARCHAR_TYPE, ColumnInfoTypeName.STRING),
        Arguments.of(TTypeId.STRING_TYPE, ColumnInfoTypeName.STRING),
        Arguments.of(TTypeId.TIMESTAMP_TYPE, ColumnInfoTypeName.TIMESTAMP),
        Arguments.of(TTypeId.BINARY_TYPE, ColumnInfoTypeName.BINARY),
        Arguments.of(TTypeId.DECIMAL_TYPE, ColumnInfoTypeName.DECIMAL),
        Arguments.of(TTypeId.NULL_TYPE, ColumnInfoTypeName.STRING),
        Arguments.of(TTypeId.DATE_TYPE, ColumnInfoTypeName.DATE),
        Arguments.of(TTypeId.CHAR_TYPE, ColumnInfoTypeName.CHAR),
        Arguments.of(TTypeId.INTERVAL_YEAR_MONTH_TYPE, ColumnInfoTypeName.INTERVAL),
        Arguments.of(TTypeId.INTERVAL_DAY_TIME_TYPE, ColumnInfoTypeName.INTERVAL),
        Arguments.of(TTypeId.DOUBLE_TYPE, ColumnInfoTypeName.DOUBLE),
        Arguments.of(TTypeId.MAP_TYPE, ColumnInfoTypeName.MAP),
        Arguments.of(TTypeId.ARRAY_TYPE, ColumnInfoTypeName.ARRAY),
        Arguments.of(TTypeId.STRUCT_TYPE, ColumnInfoTypeName.STRUCT));
  }

  private static Stream<Arguments> typeIdColumnTypeText() {
    return Stream.of(
        Arguments.of(TTypeId.BOOLEAN_TYPE, "BOOLEAN"),
        Arguments.of(TTypeId.TINYINT_TYPE, "TINYINT"),
        Arguments.of(TTypeId.SMALLINT_TYPE, "SMALLINT"),
        Arguments.of(TTypeId.INT_TYPE, "INT"),
        Arguments.of(TTypeId.BIGINT_TYPE, "BIGINT"),
        Arguments.of(TTypeId.FLOAT_TYPE, "FLOAT"),
        Arguments.of(TTypeId.DOUBLE_TYPE, "DOUBLE"),
        Arguments.of(TTypeId.TIMESTAMP_TYPE, "TIMESTAMP"),
        Arguments.of(TTypeId.BINARY_TYPE, "BINARY"),
        Arguments.of(TTypeId.DECIMAL_TYPE, "DECIMAL"),
        Arguments.of(TTypeId.DATE_TYPE, "DATE"),
        Arguments.of(TTypeId.CHAR_TYPE, "CHAR"),
        Arguments.of(TTypeId.STRING_TYPE, "STRING"),
        Arguments.of(TTypeId.VARCHAR_TYPE, "VARCHAR"));
  }

  private static List<List<Object>> getExpectedResults(int rowCount, List<?>... typeValues) {
    List<List<Object>> rows = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      List<Object> row = new ArrayList<>();
      for (List<?> typeValue : typeValues) {
        row.add(typeValue.get(i));
      }
      rows.add(row);
    }
    return rows;
  }

  private static List<List<Object>> getExpectedResults(List<?>... typeValues) {
    return getExpectedResults(typeValues[0].size(), typeValues);
  }

  private static Stream<Arguments> resultDataTypesForGetColumnValue() {
    return Stream.of(
        Arguments.of(new TRowSet(), List.of()),
        Arguments.of(new TRowSet().setColumns(Collections.emptyList()), List.of()),
        Arguments.of(BOOL_ROW_SET, getExpectedResults(BOOL_ROW_SET_VALUES)),
        Arguments.of(BYTE_ROW_SET, getExpectedResults(BYTE_ROW_SET_VALUES)),
        Arguments.of(DOUBLE_ROW_SET, getExpectedResults(DOUBLE_ROW_SET_VALUES)),
        Arguments.of(I16_ROW_SET, getExpectedResults(SHORT_ROW_SET_VALUES)),
        Arguments.of(I32_ROW_SET, getExpectedResults(INT_ROW_SET_VALUES)),
        Arguments.of(I64_ROW_SET, getExpectedResults(LONG_ROW_SET_VALUES)),
        Arguments.of(STRING_ROW_SET, getExpectedResults(STRING_ROW_SET_VALUES)),
        Arguments.of(BINARY_ROW_SET, getExpectedResults(BINARY_ROW_SET_VALUES)),
        Arguments.of(
            MIXED_ROW_SET,
            getExpectedResults(
                MIXED_ROW_SET_COUNT,
                BYTE_ROW_SET_VALUES,
                DOUBLE_ROW_SET_VALUES,
                STRING_ROW_SET_VALUES)));
  }

  @ParameterizedTest
  @MethodSource("resultDataTypes")
  public void testRowCount(TRowSet resultData, int expectedRowCount) throws DatabricksSQLException {
    assertEquals(expectedRowCount, DatabricksThriftUtil.getRowCount(resultData));
  }

  @ParameterizedTest
  @MethodSource("resultDataTypesForGetColumnValue")
  public void testColumnCount(TRowSet resultData, List<List<Object>> expectedValues)
      throws DatabricksSQLException {
    assertEquals(expectedValues, DatabricksThriftUtil.extractRowsFromColumnar(resultData));
  }

  @Test
  public void testUnknownColumnType() {
    TRowSet rowSetWithNoColumnType =
        new TRowSet().setColumns(Collections.singletonList(new TColumn()));
    assertThrows(
        DatabricksSQLException.class,
        () -> DatabricksThriftUtil.extractRowsFromColumnar(rowSetWithNoColumnType));
  }

  private static Stream<Arguments> manifestTypes() {
    return Stream.of(
        Arguments.of(null, 0),
        Arguments.of(new TGetResultSetMetadataResp(), 0),
        Arguments.of(
            new TGetResultSetMetadataResp()
                .setSchema(
                    new TTableSchema().setColumns(Collections.singletonList(new TColumnDesc()))),
            1));
  }

  @ParameterizedTest
  @MethodSource("manifestTypes")
  public void testColumnCount(TGetResultSetMetadataResp resultManifest, int expectedColumnCount) {
    assertEquals(expectedColumnCount, DatabricksThriftUtil.getColumnCount(resultManifest));
  }

  @Test
  public void testConvertColumnarToRowBased() throws DatabricksSQLException {
    when(fetchResultsResp.getResults()).thenReturn(BOOL_ROW_SET);
    when(parentStatement.getMaxRows()).thenReturn(DEFAULT_RESULT_ROW_LIMIT);
    List<List<Object>> rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(fetchResultsResp, parentStatement, session);
    assertEquals(4, rowBasedData.size());

    when(fetchResultsResp.getResults()).thenReturn(null);
    rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(fetchResultsResp, parentStatement, session);
    assertEquals(0, rowBasedData.size());

    when(fetchResultsResp.getResults())
        .thenReturn(new TRowSet().setColumns(Collections.emptyList()));
    rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(fetchResultsResp, parentStatement, session);
    assertEquals(0, rowBasedData.size());
  }

  private static TTypeDesc createTypeDesc(TTypeId type) {
    TPrimitiveTypeEntry primitiveType = new TPrimitiveTypeEntry().setType(type);
    TTypeEntry typeEntry = new TTypeEntry();
    typeEntry.setPrimitiveEntry(primitiveType);
    return new TTypeDesc().setTypes(Collections.singletonList(typeEntry));
  }

  @Test
  public void testMaxRowsInStatement() throws DatabricksSQLException {
    when(fetchResultsResp.getResults()).thenReturn(BOOL_ROW_SET);
    int maxRows = 2;
    when(parentStatement.getMaxRows()).thenReturn(maxRows);
    List<List<Object>> rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(fetchResultsResp, parentStatement, session);
    assertEquals(maxRows, rowBasedData.size());

    maxRows = 0;
    when(parentStatement.getMaxRows()).thenReturn(maxRows);
    rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(fetchResultsResp, parentStatement, session);
    assertTrue(rowBasedData.isEmpty());

    maxRows = 5;
    when(parentStatement.getMaxRows()).thenReturn(maxRows);
    rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(fetchResultsResp, parentStatement, session);
    assertEquals(4, rowBasedData.size());
  }

  @ParameterizedTest
  @MethodSource("typeIdAndColumnInfoType")
  public void testGetTypeFromTypeDesc(TTypeId type, ColumnInfoTypeName typeName) {
    TTypeDesc typeDesc = createTypeDesc(type);
    assertEquals(DatabricksThriftUtil.getTypeFromTypeDesc(typeDesc), typeName);
  }

  @ParameterizedTest
  @MethodSource("typeIdColumnTypeText")
  public void testGetTypeTextFromTypeDesc(TTypeId type, String expectedColumnTypeText) {
    TTypeDesc typeDesc = createTypeDesc(type);
    assertEquals(DatabricksThriftUtil.getTypeTextFromTypeDesc(typeDesc), expectedColumnTypeText);
  }

  @ParameterizedTest
  @MethodSource("thriftDirectResultSets")
  public void testCheckDirectResultsForErrorStatus(TSparkDirectResults response) {
    assertDoesNotThrow(() -> checkDirectResultsForErrorStatus(response, TEST_STRING));
  }

  @Test
  public void testGetStatementStatus() throws Exception {
    assertEquals(
        StatementState.PENDING,
        DatabricksThriftUtil.getStatementStatus(
                new TGetOperationStatusResp().setOperationState(TOperationState.INITIALIZED_STATE))
            .getState());
    assertEquals(
        StatementState.PENDING,
        DatabricksThriftUtil.getStatementStatus(
                new TGetOperationStatusResp().setOperationState(TOperationState.PENDING_STATE))
            .getState());
    assertEquals(
        StatementState.SUCCEEDED,
        DatabricksThriftUtil.getStatementStatus(
                new TGetOperationStatusResp().setOperationState(TOperationState.FINISHED_STATE))
            .getState());
    assertEquals(
        StatementState.RUNNING,
        DatabricksThriftUtil.getStatementStatus(
                new TGetOperationStatusResp().setOperationState(TOperationState.RUNNING_STATE))
            .getState());
    assertEquals(
        StatementState.FAILED,
        DatabricksThriftUtil.getStatementStatus(
                new TGetOperationStatusResp().setOperationState(TOperationState.ERROR_STATE))
            .getState());
    assertEquals(
        StatementState.FAILED,
        DatabricksThriftUtil.getStatementStatus(
                new TGetOperationStatusResp().setOperationState(TOperationState.TIMEDOUT_STATE))
            .getState());
    assertEquals(
        StatementState.FAILED,
        DatabricksThriftUtil.getStatementStatus(
                new TGetOperationStatusResp().setOperationState(TOperationState.UKNOWN_STATE))
            .getState());
    assertEquals(
        StatementState.CLOSED,
        DatabricksThriftUtil.getStatementStatus(
                new TGetOperationStatusResp().setOperationState(TOperationState.CLOSED_STATE))
            .getState());
    assertEquals(
        StatementState.CANCELED,
        DatabricksThriftUtil.getStatementStatus(
                new TGetOperationStatusResp().setOperationState(TOperationState.CANCELED_STATE))
            .getState());
  }

  @Test
  public void testGetStatementStatusForAsync() throws Exception {
    assertEquals(
        StatementState.RUNNING,
        DatabricksThriftUtil.getAsyncStatus(new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS))
            .getState());
    assertEquals(
        StatementState.RUNNING,
        DatabricksThriftUtil.getAsyncStatus(
                new TStatus().setStatusCode(TStatusCode.SUCCESS_WITH_INFO_STATUS))
            .getState());
    assertEquals(
        StatementState.RUNNING,
        DatabricksThriftUtil.getAsyncStatus(
                new TStatus().setStatusCode(TStatusCode.STILL_EXECUTING_STATUS))
            .getState());
    assertEquals(
        StatementState.FAILED,
        DatabricksThriftUtil.getAsyncStatus(
                new TStatus().setStatusCode(TStatusCode.INVALID_HANDLE_STATUS))
            .getState());
    assertEquals(
        StatementState.FAILED,
        DatabricksThriftUtil.getAsyncStatus(new TStatus().setStatusCode(TStatusCode.ERROR_STATUS))
            .getState());
  }

  @Test
  public void testExtractRowsWithNullsForAllTypes() throws DatabricksSQLException {
    // Create a TRowSet with three columns: STRING, DOUBLE, and BOOLEAN.
    TRowSet rowSet = new TRowSet();
    List<TColumn> columns = new ArrayList<>();

    // STRING column: two rows. First value is non-null, second is marked null.
    TStringColumn stringColumn = new TStringColumn();
    stringColumn.setValues(Arrays.asList("value1", "value2"));
    // Bit mask: 2 (binary 00000010): row0 = 0 (non-null), row1 = 1 (null)
    stringColumn.setNulls(new byte[] {2});
    TColumn stringTColumn = new TColumn();
    stringTColumn.setStringVal(stringColumn);
    columns.add(stringTColumn);

    // DOUBLE column: two rows. First value is marked null, second is non-null.
    TDoubleColumn doubleColumn = new TDoubleColumn();
    doubleColumn.setValues(Arrays.asList(1.1, 2.2));
    // Bit mask: 1 (binary 00000001): row0 = 1 (null), row1 = 0 (non-null)
    doubleColumn.setNulls(new byte[] {1});
    TColumn doubleTColumn = new TColumn();
    doubleTColumn.setDoubleVal(doubleColumn);
    columns.add(doubleTColumn);

    // BOOLEAN column: two rows with no nulls.
    TBoolColumn boolColumn = new TBoolColumn();
    boolColumn.setValues(Arrays.asList(true, false));
    // Bit mask: 0 (binary 00000000): no nulls
    boolColumn.setNulls(new byte[] {0});
    TColumn boolTColumn = new TColumn();
    boolTColumn.setBoolVal(boolColumn);
    columns.add(boolTColumn);

    rowSet.setColumns(columns);

    // Expected rows:
    // Row 1: [ "value1", null, true ]
    // Row 2: [ null, 2.2, false ]
    List<List<Object>> expected = new ArrayList<>();
    expected.add(Arrays.asList("value1", null, true));
    expected.add(Arrays.asList(null, 2.2, false));

    List<List<Object>> actual = DatabricksThriftUtil.extractRowsFromColumnar(rowSet);
    assertEquals(expected.size(), actual.size(), "Expected row count of 2");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i), "Mismatch in row " + i);
    }
  }

  @Test
  public void testExtractRowsWhenNullsArrayIsNull() throws DatabricksSQLException {
    // Create a TRowSet with two columns: STRING and DOUBLE, where the nulls arrays are null.
    TRowSet rowSet = new TRowSet();
    List<TColumn> columns = new ArrayList<>();

    // STRING column: two rows, no null indicators.
    TStringColumn stringColumn = new TStringColumn();
    stringColumn.setValues(Arrays.asList("a", "b"));
    stringColumn.setNulls((byte[]) null);
    // No nulls array provided.
    TColumn stringTColumn = new TColumn();
    stringTColumn.setStringVal(stringColumn);
    columns.add(stringTColumn);

    // DOUBLE column: two rows, no null indicators.
    TDoubleColumn doubleColumn = new TDoubleColumn();
    doubleColumn.setValues(Arrays.asList(10.0, 20.0));
    stringColumn.setNulls((byte[]) null);
    TColumn doubleTColumn = new TColumn();
    doubleTColumn.setDoubleVal(doubleColumn);
    columns.add(doubleTColumn);

    rowSet.setColumns(columns);

    // Expected rows:
    // Row 1: [ "a", 10.0 ]
    // Row 2: [ "b", 20.0 ]
    List<List<Object>> expected = new ArrayList<>();
    expected.add(Arrays.asList("a", 10.0));
    expected.add(Arrays.asList("b", 20.0));

    List<List<Object>> actual = DatabricksThriftUtil.extractRowsFromColumnar(rowSet);
    assertEquals(expected.size(), actual.size(), "Expected row count of 2");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i), "Mismatch in row " + i);
    }
  }
}
