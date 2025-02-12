package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.MetadataResultConstants.NULL_STRING;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.checkDirectResultsForErrorStatus;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.StatementState;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
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
        Arguments.of(boolRowSet, 4),
        Arguments.of(byteRowSet, 1),
        Arguments.of(doubleRowSet, 6),
        Arguments.of(i16RowSet, 1),
        Arguments.of(i32RowSet, 1),
        Arguments.of(i64RowSet, 2),
        Arguments.of(stringRowSet, 2),
        Arguments.of(binaryRowSet, 1));
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

  private static Stream<Arguments> resultDataTypesForGetColumnValue() {
    return Stream.of(
        Arguments.of(new TRowSet(), null),
        Arguments.of(
            new TRowSet().setColumns(Collections.emptyList()),
            Collections.singletonList(Collections.emptyList())),
        Arguments.of(
            new TRowSet().setColumns(Collections.singletonList(new TColumn())),
            Collections.singletonList(Collections.singletonList(NULL_STRING))),
        Arguments.of(boolRowSet, Collections.singletonList(List.of(false))),
        Arguments.of(byteRowSet, Collections.singletonList(List.of((byte) 5))),
        Arguments.of(doubleRowSet, Collections.singletonList(List.of(1.0))),
        Arguments.of(i16RowSet, Collections.singletonList(List.of((short) 1))),
        Arguments.of(i32RowSet, Collections.singletonList(List.of(1))),
        Arguments.of(i64RowSet, Collections.singletonList(List.of(1L))),
        Arguments.of(stringRowSet, Collections.singletonList(List.of(TEST_STRING))),
        Arguments.of(
            binaryRowSet,
            Collections.singletonList(List.of(ByteBuffer.wrap(TEST_STRING.getBytes())))));
  }

  @ParameterizedTest
  @MethodSource("resultDataTypes")
  public void testRowCount(TRowSet resultData, int expectedRowCount) {
    assertEquals(expectedRowCount, DatabricksThriftUtil.getRowCount(resultData));
  }

  @ParameterizedTest
  @MethodSource("resultDataTypesForGetColumnValue")
  public void testColumnCount(TRowSet resultData, List<List<Object>> expectedValues) {
    assertEquals(expectedValues, DatabricksThriftUtil.extractValues(resultData.getColumns()));
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
    when(fetchResultsResp.getResults()).thenReturn(boolRowSet);
    List<List<Object>> rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(fetchResultsResp, parentStatement, session);
    assertEquals(rowBasedData.size(), 4);

    when(fetchResultsResp.getResults()).thenReturn(null);
    rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(fetchResultsResp, parentStatement, session);
    assertEquals(rowBasedData.size(), 0);

    when(fetchResultsResp.getResults())
        .thenReturn(new TRowSet().setColumns(Collections.emptyList()));
    rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(fetchResultsResp, parentStatement, session);
    assertEquals(rowBasedData.size(), 0);
  }

  private static TTypeDesc createTypeDesc(TTypeId type) {
    TPrimitiveTypeEntry primitiveType = new TPrimitiveTypeEntry().setType(type);
    TTypeEntry typeEntry = new TTypeEntry();
    typeEntry.setPrimitiveEntry(primitiveType);
    return new TTypeDesc().setTypes(Collections.singletonList(typeEntry));
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
}
