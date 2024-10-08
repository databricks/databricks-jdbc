package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.common.MetadataResultConstants.NULL_STRING;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.checkDirectResultsForErrorStatus;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DatabricksThriftUtilTest {
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
        () -> DatabricksThriftUtil.verifySuccessStatus(TStatusCode.SUCCESS_STATUS, "test"));
    assertDoesNotThrow(
        () ->
            DatabricksThriftUtil.verifySuccessStatus(TStatusCode.SUCCESS_WITH_INFO_STATUS, "test"));
    assertThrows(
        DatabricksHttpException.class,
        () -> DatabricksThriftUtil.verifySuccessStatus(TStatusCode.ERROR_STATUS, "test"));
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
        Arguments.of(TTypeId.MAP_TYPE, ColumnInfoTypeName.STRING));
  }

  private static Stream<Arguments> resultDataTypesForGetColumnValue() {
    return Stream.of(
        Arguments.of(new TRowSet(), Collections.singletonList(Collections.emptyList())),
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
  public void testConvertColumnarToRowBased() {
    List<List<Object>> rowBasedData = DatabricksThriftUtil.convertColumnarToRowBased(boolRowSet);
    assertEquals(rowBasedData.size(), 4);

    rowBasedData = DatabricksThriftUtil.convertColumnarToRowBased(null);
    assertEquals(rowBasedData.size(), 0);

    rowBasedData =
        DatabricksThriftUtil.convertColumnarToRowBased(
            new TRowSet().setColumns(Collections.emptyList()));
    assertEquals(rowBasedData.size(), 0);
  }

  @ParameterizedTest
  @MethodSource("typeIdAndColumnInfoType")
  public void testGetTypeFromTypeDesc(TTypeId type, ColumnInfoTypeName typeName) {
    TPrimitiveTypeEntry primitiveType = new TPrimitiveTypeEntry().setType(type);
    TTypeEntry typeEntry = new TTypeEntry();
    typeEntry.setPrimitiveEntry(primitiveType);
    TTypeDesc typeDesc = new TTypeDesc().setTypes(Collections.singletonList(typeEntry));
    assertEquals(DatabricksThriftUtil.getTypeFromTypeDesc(typeDesc), typeName);
  }

  @ParameterizedTest
  @MethodSource("thriftDirectResultSets")
  public void testCheckDirectResultsForErrorStatus(TSparkDirectResults response) {
    assertDoesNotThrow(() -> checkDirectResultsForErrorStatus(response, TEST_STRING));
  }
}
