package com.databricks.jdbc.client.impl.thrift.commons;

import static com.databricks.jdbc.TestConstants.*;
import static com.databricks.jdbc.client.impl.helper.MetadataResultConstants.NULL_STRING;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DatabricksThriftHelperTest {
  @Test
  void testByteBufferToString() {
    DatabricksThriftHelper helper = new DatabricksThriftHelper(); // cover the constructors too
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
        () -> DatabricksThriftHelper.verifySuccessStatus(TStatusCode.SUCCESS_STATUS, "test"));
    assertDoesNotThrow(
        () ->
            DatabricksThriftHelper.verifySuccessStatus(
                TStatusCode.SUCCESS_WITH_INFO_STATUS, "test"));
    assertThrows(
        DatabricksHttpException.class,
        () -> DatabricksThriftHelper.verifySuccessStatus(TStatusCode.ERROR_STATUS, "test"));
  }

  private static Stream<Arguments> resultDataTypes() {
    return Stream.of(
        Arguments.of(new TRowSet(), 0),
        Arguments.of(new TRowSet().setColumns(Collections.emptyList()), 0),
        Arguments.of(boolRowSet, 4),
        Arguments.of(byteRowSet, 1),
        Arguments.of(doubleRowSet, 6),
        Arguments.of(i16RowSet, 1),
        Arguments.of(i32RowSet, 1),
        Arguments.of(i64RowSet, 2),
        Arguments.of(stringRowSet, 2),
        Arguments.of(binaryRowSet, 1));
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
    assertEquals(expectedRowCount, DatabricksThriftHelper.getRowCount(resultData));
  }

  @ParameterizedTest
  @MethodSource("resultDataTypesForGetColumnValue")
  public void testColumnCount(TRowSet resultData, List<List<Object>> expectedValues) {
    assertEquals(expectedValues, DatabricksThriftHelper.extractValues(resultData.getColumns()));
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
    assertEquals(expectedColumnCount, DatabricksThriftHelper.getColumnCount(resultManifest));
  }
}
