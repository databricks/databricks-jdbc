package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.api.impl.DatabricksResultSet.AFFECTED_ROWS_COUNT;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.VARIANT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.impl.volume.VolumeOperationResult;
import com.databricks.jdbc.api.internal.IDatabricksResultSetInternal;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.service.sql.StatementState;
import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.*;
import java.util.*;
import org.apache.http.entity.InputStreamEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksResultSetTest {

  private static final StatementId STATEMENT_ID = new StatementId("statementId");
  private static final StatementId THRIFT_STATEMENT_ID =
      StatementId.deserialize(
          "01efc77c-7c8b-1a8e-9ecb-a9a6e6aa050a|338d529d-8272-46eb-8482-cb419466839d");

  @Mock InlineJsonResult mockedExecutionResult;
  @Mock TFetchResultsResp fetchResultsResp;
  @Mock VolumeOperationResult mockedVolumeResult;
  @Mock DatabricksResultSetMetaData mockedResultSetMetadata;
  @Mock IDatabricksSession session;
  @Mock DatabricksStatement mockedDatabricksStatement;

  @Mock DatabricksConnectionContext databricksConnectionContext;
  @Mock Statement mockedStatement;

  private DatabricksResultSet getResultSet(
      StatementState statementState, IDatabricksStatementInternal statement) {
    return new DatabricksResultSet(
        new StatementStatus().setState(statementState),
        STATEMENT_ID,
        StatementType.METADATA,
        statement,
        mockedExecutionResult,
        mockedResultSetMetadata,
        false);
  }

  private DatabricksResultSet getThriftResultSetMetadata() throws SQLException {
    TColumnValue columnValue = new TColumnValue();
    columnValue.setStringVal(new TStringValue().setValue("testString"));
    TRow row = new TRow().setColVals(Collections.singletonList(columnValue));
    TRowSet rowSet = new TRowSet().setRows(Collections.singletonList(row));
    TGetResultSetMetadataResp metadataResp =
        new TGetResultSetMetadataResp().setResultFormat(TSparkRowSetType.COLUMN_BASED_SET);
    TColumnDesc columnDesc = new TColumnDesc().setColumnName("testCol");
    TTableSchema schema = new TTableSchema().setColumns(Collections.singletonList(columnDesc));
    metadataResp.setSchema(schema);
    when(fetchResultsResp.getResults()).thenReturn(rowSet);
    when(fetchResultsResp.getResultSetMetadata()).thenReturn(metadataResp);
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        THRIFT_STATEMENT_ID,
        fetchResultsResp,
        StatementType.METADATA,
        mockedDatabricksStatement,
        session);
  }

  @Test
  void testNext() throws SQLException {
    when(mockedExecutionResult.next()).thenReturn(true);
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    assertTrue(resultSet.next());
  }

  @Test
  void testAbsolute() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.absolute(0));

    when(mockedExecutionResult.getCurrentRow()).thenReturn(0L, 1L, 2L);
    doReturn(true).doReturn(true).doReturn(false).when(mockedExecutionResult).next();

    assertTrue(resultSet.absolute(3));
    // throws exception for backward exception
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.absolute(1));

    assertFalse(resultSet.absolute(4));
  }

  @Test
  void testThriftResultSet() throws SQLException {
    when(session.getConnectionContext()).thenReturn(databricksConnectionContext);
    DatabricksThreadContextHolder.setConnectionContext(databricksConnectionContext);
    when(databricksConnectionContext.isComplexDatatypeSupportEnabled()).thenReturn(false);
    DatabricksResultSet resultSet = getThriftResultSetMetadata();
    DatabricksThreadContextHolder.clearAllContext();
    assertFalse(resultSet.next());
  }

  @Test
  void testGetStatementStatus() {
    DatabricksResultSet resultSet = getResultSet(StatementState.PENDING, null);
    assertEquals(StatementState.PENDING, resultSet.getStatementStatus().getState());
  }

  @Test
  void testGetStatement() throws SQLException {
    when(mockedDatabricksStatement.getStatement()).thenReturn(mockedStatement);
    DatabricksResultSet resultSet = getResultSet(StatementState.PENDING, mockedDatabricksStatement);
    assertEquals(mockedStatement, resultSet.getStatement());
  }

  @Test
  void testGetStringAndWasNull() throws SQLException {
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    DatabricksResultSet resultSet = getResultSet(StatementState.PENDING, null);
    assertNull(resultSet.getString(1));
    assertTrue(resultSet.wasNull());
    when(mockedExecutionResult.getObject(0)).thenReturn("test");
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);
    assertEquals("test", resultSet.getString(1));
    assertFalse(resultSet.wasNull());
    // Test with invalid label
    assertThrows(DatabricksSQLException.class, () -> resultSet.getString(0));
    assertThrows(DatabricksSQLException.class, () -> resultSet.getString(-1));
    // Test with column label
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals("test", resultSet.getString("columnLabel"));
  }

  @Test
  void testGetStringWithWhiteSpaces() throws SQLException {
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    DatabricksResultSet resultSet = getResultSet(StatementState.PENDING, null);
    assertNull(resultSet.getString(1));
    assertTrue(resultSet.wasNull());
    when(mockedExecutionResult.getObject(0)).thenReturn("     test     ");
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);
    assertEquals("     test     ", resultSet.getString(1));
    assertFalse(resultSet.wasNull());
    // Test with column label
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals("     test     ", resultSet.getString("columnLabel"));
  }

  @Test
  void testGetStringWithQuotes() throws SQLException {
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    DatabricksResultSet resultSet = getResultSet(StatementState.PENDING, null);
    assertNull(resultSet.getString(1));
    assertTrue(resultSet.wasNull());
    when(mockedExecutionResult.getObject(0)).thenReturn("\"test\"");
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);
    assertEquals("\"test\"", resultSet.getString(1));
    assertFalse(resultSet.wasNull());
    // Test with column label
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals("\"test\"", resultSet.getString("columnLabel"));
  }

  @Test
  void testGetBoolean() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(0)).thenReturn(true);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.BOOLEAN);
    assertTrue(resultSet.getBoolean(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertFalse(resultSet.getBoolean(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn(false);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertFalse(resultSet.getBoolean("columnLabel"));
  }

  @Test
  void testGetByte() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(0)).thenReturn((byte) 100);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.TINYINT);
    assertEquals((byte) 100, resultSet.getByte(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(0, resultSet.getByte(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn((byte) 100);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals((byte) 100, resultSet.getByte("columnLabel"));
  }

  @Test
  void testGetShort() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(0)).thenReturn((short) 100);
    when(mockedResultSetMetadata.getColumnTypeName(anyInt())).thenReturn("");
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.SMALLINT);
    assertEquals((short) 100, resultSet.getShort(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(0, resultSet.getShort(1));
    assertNull(resultSet.getObject(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn((short) 100);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals((short) 100, resultSet.getShort("columnLabel"));
    assertEquals((short) 100, resultSet.getObject("columnLabel"));
  }

  @Test
  void testGetInt() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(0)).thenReturn(100);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);
    assertEquals(100, resultSet.getInt(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(0, resultSet.getInt(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn(100);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals(100, resultSet.getInt("columnLabel"));
  }

  @Test
  void testGetLong() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(0)).thenReturn((long) 100);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.BIGINT);
    assertEquals(100, resultSet.getLong(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(0, resultSet.getLong(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn((long) 100);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals(100, resultSet.getLong("columnLabel"));
  }

  @Test
  void testGetFloat() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(0)).thenReturn((float) 100.43);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.FLOAT);
    assertEquals(100.43f, resultSet.getFloat(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(0, resultSet.getFloat(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn((float) 100.43);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals(100.43f, resultSet.getFloat("columnLabel"));
  }

  @Test
  void testGetUnicode() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    String testString = "test";
    when(mockedExecutionResult.getObject(0)).thenReturn(testString);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);
    assertNotNull(resultSet.getUnicodeStream(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertNull(resultSet.getUnicodeStream(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn(testString);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertNotNull(resultSet.getUnicodeStream("columnLabel"));
  }

  @Test
  void testGetDouble() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(0)).thenReturn((double) 100);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.DOUBLE);
    assertEquals(100f, resultSet.getDouble(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(0, resultSet.getDouble(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn((double) 100);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals(100f, resultSet.getDouble("columnLabel"));
  }

  @Test
  void testGetBigDecimal() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    // Test with float type column
    when(mockedExecutionResult.getObject(1)).thenReturn(123.423123f);
    when(mockedResultSetMetadata.getColumnType(2)).thenReturn(Types.FLOAT);
    when(mockedResultSetMetadata.getScale(2)).thenReturn(0);
    assertEquals(new BigDecimal("123.42313"), resultSet.getBigDecimal(2));
    // Test with double type column
    when(mockedExecutionResult.getObject(1)).thenReturn(123.423123d);
    when(mockedResultSetMetadata.getColumnType(2)).thenReturn(Types.DOUBLE);
    when(mockedResultSetMetadata.getScale(2)).thenReturn(0);
    assertEquals(new BigDecimal("123.423123"), resultSet.getBigDecimal(2));
    // Test with decimal type column
    when(mockedExecutionResult.getObject(1)).thenReturn(new BigDecimal("123.423123"));
    when(mockedResultSetMetadata.getColumnType(2)).thenReturn(Types.DECIMAL);
    when(mockedResultSetMetadata.getScale(2)).thenReturn(6);
    assertEquals(new BigDecimal("123.423123"), resultSet.getBigDecimal(2));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertNull(resultSet.getBigDecimal(1));
    // Test with column label
    when(mockedExecutionResult.getObject(1)).thenReturn(new BigDecimal("123.423123"));
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(2);
    assertEquals(new BigDecimal("123.423123"), resultSet.getBigDecimal("columnLabel"));
  }

  @Test
  void testGetDate() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);

    int epochDay = 19722;
    Date expected = Date.valueOf(LocalDate.ofEpochDay(epochDay)); // 2023-12-31
    int columnIndex = 2;
    when(mockedExecutionResult.getObject(columnIndex - 1)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnType(columnIndex)).thenReturn(Types.DATE);
    assertEquals(expected, resultSet.getDate(columnIndex));
    // null object
    when(mockedExecutionResult.getObject(columnIndex - 1)).thenReturn(null);
    assertNull(resultSet.getDate(columnIndex));
    // Test with column label
    when(mockedExecutionResult.getObject(columnIndex - 1)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(columnIndex);
    assertEquals(expected, resultSet.getDate("columnLabel"));

    // Test with Calendar argument
    Calendar calendar = Calendar.getInstance();
    Date actualDate = resultSet.getDate(columnIndex, calendar);
    assertEquals(expected, actualDate);

    // Test with Calendar argument in different TZ
    ZoneId inputZoneId = ZoneId.of("America/New_York");
    calendar = Calendar.getInstance(TimeZone.getTimeZone(inputZoneId));
    actualDate = resultSet.getDate(columnIndex, calendar);
    assertEquals(expected.toLocalDate(), actualDate.toLocalDate());

    // Test with null Calendar argument
    assertEquals(expected, resultSet.getDate(columnIndex, null));
  }

  @Test
  void testGetBinaryStream() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    int columnIndex = 1;
    byte[] testBytes = {0x00, 0x01, 0x02};
    when(resultSet.getObject(columnIndex)).thenReturn(testBytes);
    when(mockedResultSetMetadata.getColumnType(columnIndex)).thenReturn(java.sql.Types.BINARY);
    assertNotNull(resultSet.getBinaryStream(columnIndex));
    // null object
    when(mockedExecutionResult.getObject(2)).thenReturn(null);
    assertNull(resultSet.getBinaryStream(3));
    // Test with column label
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(columnIndex);
    assertNotNull(resultSet.getBinaryStream("columnLabel"));
  }

  @Test
  void testGetAsciiStream() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    int columnIndex = 5;
    when(resultSet.getObject(columnIndex)).thenReturn("Test ASCII Stream");
    when(mockedResultSetMetadata.getColumnType(columnIndex)).thenReturn(java.sql.Types.VARCHAR);
    assertNotNull(resultSet.getAsciiStream(columnIndex));
    // null object
    when(mockedExecutionResult.getObject(2)).thenReturn(null);
    assertNull(resultSet.getAsciiStream(3));
    // Test with column label
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(columnIndex);
    assertNotNull(resultSet.getAsciiStream("columnLabel"));
  }

  @Test
  void testGetTime() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    int columnIndex = 5;
    String expectedTimestampString = "2023-01-01 12:30:00";
    // Test using timestamp object
    Timestamp expectedTimestamp = Timestamp.valueOf(expectedTimestampString);
    when(resultSet.getObject(columnIndex)).thenReturn(expectedTimestamp);
    when(mockedResultSetMetadata.getColumnType(columnIndex)).thenReturn(java.sql.Types.TIMESTAMP);
    assertEquals(Time.valueOf("12:30:00"), resultSet.getTime(columnIndex));

    // null object
    when(mockedExecutionResult.getObject(2)).thenReturn(null);
    assertNull(resultSet.getTime(3));

    // Test with column label
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(columnIndex);
    assertEquals(Time.valueOf("12:30:00"), resultSet.getTime("columnLabel"));

    // Test with Calendar argument
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
    Time actualTime = resultSet.getTime(columnIndex, calendar);
    assertEquals(
        new Time(OffsetTime.parse("12:30:00+09:00").toEpochSecond(LocalDate.EPOCH) * 1000),
        actualTime);

    // Test with null Calendar argument
    assertEquals(Time.valueOf("12:30:00"), resultSet.getTime(columnIndex, null));
  }

  @Test
  void testGetTimestamp() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    int columnIndex = 5;
    String expectedTimestampString = "2023-01-01 12:30:00";
    // Test using timestamp object
    Timestamp expectedTimestamp = Timestamp.valueOf(expectedTimestampString);
    when(resultSet.getObject(columnIndex)).thenReturn(expectedTimestamp);
    when(mockedResultSetMetadata.getColumnType(columnIndex)).thenReturn(java.sql.Types.TIMESTAMP);
    assertEquals(expectedTimestamp, resultSet.getTimestamp(columnIndex));
    // null object
    when(mockedExecutionResult.getObject(2)).thenReturn(null);
    assertNull(resultSet.getTimestamp(3));
    // Test with column label
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(columnIndex);
    assertEquals(expectedTimestamp, resultSet.getTimestamp("columnLabel"));

    // Test with Calendar argument
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
    // Asia/Tokyo is GMT+9
    assertEquals(
        new Timestamp(OffsetDateTime.parse("2023-01-01T12:30:00+09:00").toEpochSecond() * 1000),
        resultSet.getTimestamp(columnIndex, calendar));

    // Test with null Calendar argument
    assertEquals(expectedTimestamp, resultSet.getTimestamp(columnIndex, null));
  }

  private static Timestamp getTimestampAdjustedToTimeZone(long timestamp, String timeZone) {
    Instant instant = Instant.ofEpochMilli(timestamp);
    LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of(timeZone));
    return Timestamp.valueOf(localDateTime);
  }

  @Test
  void testGetBytes() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    byte[] expected = new byte[] {1, 2, 3};
    when(mockedExecutionResult.getObject(2)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnType(3)).thenReturn(Types.BINARY);
    assertEquals(expected, resultSet.getBytes(3));
    // null object
    when(mockedExecutionResult.getObject(2)).thenReturn(null);
    assertNull(resultSet.getBytes(3));
    // Test with column label
    when(mockedExecutionResult.getObject(2)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(3);
    assertEquals(expected, resultSet.getBytes("columnLabel"));
  }

  @Test
  void testGetStruct() throws SQLException {
    // Define expected attributes
    Object[] structAttributes = {1, "Alice"};

    // Mock execution result
    when(mockedExecutionResult.getObject(2))
        .thenReturn(
            new DatabricksStruct(
                Map.of("id", 1, "name", "Alice"), "STRUCT<id: INT, name: STRING>"));
    when(mockedResultSetMetadata.getColumnNameIndex("user_struct")).thenReturn(3);

    // Instantiate result set
    DatabricksResultSet resultSet =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            STATEMENT_ID,
            StatementType.METADATA,
            null,
            mockedExecutionResult,
            mockedResultSetMetadata,
            true);

    // Retrieve struct by index
    Struct retrievedStruct = resultSet.getStruct(3);
    assertNotNull(retrievedStruct, "Retrieved struct should not be null");
    assertArrayEquals(
        structAttributes, retrievedStruct.getAttributes(), "Struct attributes should match");

    // Retrieve struct by label
    Struct retrievedStructByLabel = resultSet.getStruct("user_struct");
    assertNotNull(retrievedStructByLabel, "Retrieved struct by label should not be null");
    assertArrayEquals(
        structAttributes, retrievedStructByLabel.getAttributes(), "Struct attributes should match");
  }

  @Test
  void testGetArray() throws SQLException {
    // Define expected array elements
    Object[] arrayElements = {"elem1", "elem2", "elem3"};

    // Mock execution result
    when(mockedExecutionResult.getObject(3))
        .thenReturn(new DatabricksArray(List.of(arrayElements), "ARRAY<STRING>"));
    when(mockedResultSetMetadata.getColumnNameIndex("string_array")).thenReturn(4);

    // Instantiate result set
    DatabricksResultSet resultSet =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            STATEMENT_ID,
            StatementType.METADATA,
            null,
            mockedExecutionResult,
            mockedResultSetMetadata,
            true);

    // Retrieve array by index
    Array retrievedArray = resultSet.getArray(4);
    assertNotNull(retrievedArray, "Retrieved array should not be null");
    assertTrue(
        Arrays.equals(arrayElements, (Object[]) retrievedArray.getArray()),
        "Array elements should match");

    // Retrieve array by label
    Array retrievedArrayByLabel = resultSet.getArray("string_array");
    assertNotNull(retrievedArrayByLabel, "Retrieved array by label should not be null");
    assertTrue(
        Arrays.equals(arrayElements, (Object[]) retrievedArrayByLabel.getArray()),
        "Array elements should match");
  }

  @Test
  void testGetMap() throws SQLException {
    // Define expected map entries
    Object[] mapEntries = {"key1", 100, "key2", 200};

    // Mock DatabricksMap
    DatabricksMap<String, Integer> mockMap = mock(DatabricksMap.class);

    // Mock execution result
    when(mockedExecutionResult.getObject(4)).thenReturn(Map.of("key1", 100, "key2", 200));
    when(mockedResultSetMetadata.getColumnNameIndex("int_map")).thenReturn(5);

    // Instantiate result set
    DatabricksResultSet resultSet =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            STATEMENT_ID,
            StatementType.METADATA,
            null,
            mockedExecutionResult,
            mockedResultSetMetadata,
            true);

    // Retrieve map by index
    Map<String, Integer> retrievedMap = resultSet.getMap(5);
    assertNotNull(retrievedMap, "Retrieved map should not be null");

    // Retrieve map by label
    Map<String, Integer> retrievedMapByLabel = resultSet.getMap("int_map");
    assertNotNull(retrievedMapByLabel, "Retrieved map by label should not be null");
  }

  @Test
  void testComplexTypes_Exceptions() throws SQLException {
    DatabricksResultSetMetaData mockedMeta = mock(DatabricksResultSetMetaData.class);
    InlineJsonResult mockedExecResult = mock(InlineJsonResult.class);
    DatabricksResultSet resultSet =
        new DatabricksResultSet(
            new StatementStatus(),
            new StatementId("fake_statement"),
            StatementType.QUERY,
            null,
            mockedExecResult,
            mockedMeta,
            true);

    // Mock closed result set
    resultSet.close();
    assertThrows(
        SQLException.class,
        () -> resultSet.getArray(1),
        "Expected getArray to fail on closed result set");
    assertThrows(
        SQLException.class,
        () -> resultSet.getStruct(1),
        "Expected getStruct to fail on closed result set");
    assertThrows(
        SQLException.class,
        () -> resultSet.getMap(1),
        "Expected getMap to fail on closed result set");

    // Mock invalid column type
    assertThrows(
        DatabricksSQLException.class,
        () -> resultSet.getArray(1),
        "Expected getArray to fail for non-ARRAY column");
    assertThrows(
        DatabricksSQLException.class,
        () -> resultSet.getStruct(1),
        "Expected getStruct to fail for non-STRUCT column");
    assertThrows(
        DatabricksSQLException.class,
        () -> resultSet.getMap(1),
        "Expected getMap to fail for non-MAP column");

    // Mock invalid parsing exception
    assertThrows(
        SQLException.class,
        () -> resultSet.getArray(1),
        "Expected getArray to fail on invalid data parse");

    assertThrows(
        SQLException.class,
        () -> resultSet.getStruct(1),
        "Expected getStruct to fail on invalid data parse");

    assertThrows(
        SQLException.class,
        () -> resultSet.getMap(1),
        "Expected getMap to fail on invalid data parse");
  }

  @Test
  void testGetObjectWithVariant() throws Exception {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(2)).thenReturn("testObject");
    when(mockedResultSetMetadata.getColumnTypeName(anyInt())).thenReturn(VARIANT);
    assertEquals("testObject", resultSet.getObject(3));

    // null object
    when(mockedExecutionResult.getObject(2)).thenReturn(null);
    assertNull(resultSet.getObject(3));

    // non-string type
    when(mockedExecutionResult.getObject(2)).thenReturn(100);
    assertThrows(
        DatabricksSQLException.class,
        () -> resultSet.getObject(3),
        "Expected getObject to fail for non-string VARIANT type");

    // Test with column label
    when(mockedExecutionResult.getObject(2)).thenReturn("testObject");
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(3);
    assertEquals("testObject", resultSet.getObject("columnLabel"));
  }

  @Test
  void testGetObject() throws SQLException {
    String expected = "testObject";
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(2)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnTypeName(anyInt())).thenReturn("");
    assertEquals(expected, resultSet.getObject(3));
    // null object
    when(mockedExecutionResult.getObject(2)).thenReturn(null);
    assertNull(resultSet.getObject(3));
    // Test with column label
    when(mockedExecutionResult.getObject(2)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(3);
    assertEquals(expected, resultSet.getObject("columnLabel"));
  }

  @Test
  void testUpdateFunctionsThrowsError() {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateNString(1, "test"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob(1, (NClob) null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNCharacterStream(1, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(1, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(1, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream(1, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateBlob(1, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateClob(1, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateNClob(1, null, 1));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::insertRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::updateRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::deleteRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::refreshRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::cancelRowUpdates);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::moveToInsertRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::moveToCurrentRow);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNString("column", "test"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob("column", (NClob) null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNCharacterStream("column", null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("column", null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("column", null, 1));

    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("column", new ByteArrayInputStream(new byte[0]), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("column", null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBlob("column", null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateClob("column", null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob("column", null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNCharacterStream(1, null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(1, null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(1, null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream(1, null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBlob(1, InputStream.nullInputStream()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateClob(1, Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob(1, Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNCharacterStream("column", null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("column", null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("column", null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("column", null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBlob("column", InputStream.nullInputStream()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateClob("column", Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob("column", Reader.nullReader()));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateInt(1, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateInt("column", 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateLong(1, 1L));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateLong("column", 1L));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateFloat(1, 1.0f));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateFloat("column", 1.0f));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateDouble(1, 1.0));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateDouble("column", 1.0));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBigDecimal(1, BigDecimal.ONE));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBigDecimal("column", BigDecimal.ONE));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateString(1, "test"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateString("column", "test"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBytes(1, new byte[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBytes("column", new byte[0]));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateDate(1, new Date(0)));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateDate("column", new Date(0)));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTime(1, new Time(0)));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTime("column", new Time(0)));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTimestamp(1, new Timestamp(0)));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTimestamp("column", new Timestamp(0)));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(1, InputStream.nullInputStream(), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("column", InputStream.nullInputStream(), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(1, InputStream.nullInputStream(), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("column", InputStream.nullInputStream(), 1));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::rowUpdated);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::rowInserted);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::rowDeleted);
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateNull("column"));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateNull(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean(1, true));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBoolean("column", true));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateByte(1, (byte) 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateByte("column", (byte) 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream(1, Reader.nullReader(), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("column", Reader.nullReader(), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML(1, null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateSQLXML("column", null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject(1, new Object(), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(1, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("column", null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("column", new ByteArrayInputStream(new byte[0]), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream(1, new CharArrayReader(new char[0]), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("column", new CharArrayReader(new char[0]), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject("column", new Object(), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject(1, new Object()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject("column", new Object()));

    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::first);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::last);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::beforeFirst);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::afterLast);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.relative(1));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::previous);

    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.setFetchDirection(ResultSet.FETCH_REVERSE));
  }

  @Test
  void testUnsupportedOperationsThrowDatabricksSQLFeatureNotSupportedException() throws Exception {
    when(mockedResultSetMetadata.getColumnNameIndex("column")).thenReturn(1);
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getBlob(1));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getRef(1));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getRef("column"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getBlob("column"));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getClob(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getClob("column"));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getURL(1));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getURL("column"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateRef(1, null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateRef("column", null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBlob(1, (Blob) null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBlob("column", (Blob) null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateClob(1, (Clob) null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateClob("column", (Clob) null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateArray(1, null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateArray("column", null));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getRowId(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getRowId("column"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateRowId(1, null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateRowId("column", null));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getNClob(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getNClob("column"));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getSQLXML(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getSQLXML("column"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getNString("column"));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getNString(2));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.getNCharacterStream("column"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getNCharacterStream(2));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(2, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("column", null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(2, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("column", null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream(2, null, 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("column", null, 1));
    assertNotNull(resultSet.unwrap(IDatabricksResultSet.class));
    assertTrue(resultSet.isWrapperFor(IDatabricksResultSet.class));
    assertTrue(resultSet.isWrapperFor(IDatabricksResultSetInternal.class));
  }

  @Test
  void testFindColumnSuccessful() throws SQLException {
    // Setup
    when(mockedResultSetMetadata.getColumnNameIndex("existingColumn")).thenReturn(3);
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);

    // Verify that findColumn returns the correct index
    assertEquals(3, resultSet.findColumn("existingColumn"));
  }

  @Test
  void testFindColumnNotFound() throws SQLException {
    // Setup
    when(mockedResultSetMetadata.getColumnNameIndex("nonExistentColumn")).thenReturn(-1);
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);

    // Verify that findColumn throws SQLException for non-existent column
    SQLException exception =
        assertThrows(DatabricksSQLException.class, () -> resultSet.findColumn("nonExistentColumn"));

    // Verify the exception message
    assertTrue(exception.getMessage().contains("Column not found"));
  }

  @Test
  void testFindColumnClosedResultSet() throws SQLException {
    // Setup
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    resultSet.close();

    // Verify that findColumn throws SQLException when result set is closed
    SQLException exception =
        assertThrows(DatabricksSQLException.class, () -> resultSet.findColumn("anyColumn"));

    // Verify the exception message
    assertTrue(exception.getMessage().contains("ResultSet is closed"));
  }

  @Test
  void testClose() throws SQLException {
    // Test null parent statement
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    assertFalse(resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
    assertThrows(DatabricksSQLException.class, resultSet::next);

    // Test non null parent statement
    resultSet = getResultSet(StatementState.SUCCEEDED, mockedDatabricksStatement);
    assertFalse(resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
  }

  @Test
  void testVolumeOperationInputStream() throws Exception {
    DatabricksResultSet resultSet =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            STATEMENT_ID,
            StatementType.METADATA,
            mockedDatabricksStatement,
            mockedVolumeResult,
            mockedResultSetMetadata,
            false);
    String fakeInput = "Hello World\n42\n";

    when(mockedVolumeResult.getVolumeOperationInputStream())
        .thenReturn(new InputStreamEntity(new ByteArrayInputStream(fakeInput.getBytes()), 10L));
    assertNotNull(resultSet.getVolumeOperationInputStream());
    assertEquals(10L, resultSet.getVolumeOperationInputStream().getContentLength());
  }

  @Test
  void testGetUpdateCountForMetadataStatement() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    assertEquals(0, resultSet.getUpdateCount());
  }

  @Test
  void testGetUpdateCountForQueryStatement() throws SQLException {
    DatabricksResultSet resultSet =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            STATEMENT_ID,
            StatementType.QUERY,
            null,
            mockedExecutionResult,
            mockedResultSetMetadata,
            false);

    assertEquals(0, resultSet.getUpdateCount());
  }

  @Test
  void testGetUpdateCountForUpdateStatement() throws SQLException {
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.BIGINT);
    when(mockedResultSetMetadata.getColumnNameIndex(AFFECTED_ROWS_COUNT)).thenReturn(1);
    when(mockedExecutionResult.next()).thenReturn(true).thenReturn(false);
    when(mockedExecutionResult.getObject(0)).thenReturn(5L);

    DatabricksResultSet resultSet =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            STATEMENT_ID,
            StatementType.UPDATE,
            null,
            mockedExecutionResult,
            mockedResultSetMetadata,
            false);

    assertEquals(5L, resultSet.getUpdateCount());
  }

  @Test
  void testGetUpdateCountForUpdateStatementMultipleRows() throws SQLException {
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.BIGINT);
    when(mockedResultSetMetadata.getColumnNameIndex("num_affected_rows")).thenReturn(1);
    when(mockedExecutionResult.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(mockedExecutionResult.getObject(0)).thenReturn(3L).thenReturn(2L);

    DatabricksResultSet resultSet =
        new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            STATEMENT_ID,
            StatementType.UPDATE,
            null,
            mockedExecutionResult,
            mockedResultSetMetadata,
            false);

    assertEquals(5L, resultSet.getUpdateCount());
  }

  @Test
  void testGetUpdateCountForClosedResultSet() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    resultSet.close();
    assertThrows(DatabricksSQLException.class, resultSet::getUpdateCount);
  }

  @Test
  void testDefaultValuesForNullFields() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(anyInt())).thenReturn(null);

    // Object types should return null
    assertNull(resultSet.getString(1));
    assertNull(resultSet.getBigDecimal(1));
    assertNull(resultSet.getDate(1));
    assertNull(resultSet.getTime(1));
    assertNull(resultSet.getTimestamp(1));
    assertNull(resultSet.getBytes(1));
    assertNull(resultSet.getAsciiStream(1));
    assertNull(resultSet.getUnicodeStream(1));
    assertNull(resultSet.getBinaryStream(1));

    // Primitive types should return their default values
    assertEquals(false, resultSet.getBoolean(1));
    assertEquals((byte) 0, resultSet.getByte(1));
    assertEquals((short) 0, resultSet.getShort(1));
    assertEquals(0, resultSet.getInt(1));
    assertEquals(0L, resultSet.getLong(1));
    assertEquals(0.0f, resultSet.getFloat(1));
    assertEquals(0.0d, resultSet.getDouble(1));

    // Make sure wasNull returns true after getting a null value
    resultSet.getString(1);
    assertTrue(resultSet.wasNull());
  }
}
