package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.api.impl.DatabricksResultSet.AFFECTED_ROWS_COUNT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.inline.InlineJsonResult;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.apache.http.HttpEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksResultSetTest {
  @Mock InlineJsonResult mockedExecutionResult;
  @Mock DatabricksResultSetMetaData mockedResultSetMetadata;
  @Mock IDatabricksSession session;

  @Mock DatabricksStatement mockedDatabricksStatement;
  @Mock Statement mockedStatement;

  private DatabricksResultSet getResultSet(
      StatementState statementState, IDatabricksStatement statement) {
    return new DatabricksResultSet(
        new StatementStatus().setState(statementState),
        "test-statementID",
        StatementType.METADATA,
        statement,
        mockedExecutionResult,
        mockedResultSetMetadata);
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
    return new DatabricksResultSet(
        new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS),
        "test-statementID",
        rowSet,
        metadataResp,
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
  void testThriftResultSet() throws SQLException {
    DatabricksResultSet resultSet = getThriftResultSetMetadata();
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
    when(mockedExecutionResult.getObject(0)).thenReturn((int) 100);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.INTEGER);
    assertEquals((int) 100, resultSet.getInt(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(0, resultSet.getInt(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn((int) 100);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals((int) 100, resultSet.getInt("columnLabel"));
  }

  @Test
  void testGetLong() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(0)).thenReturn((long) 100);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.BIGINT);
    assertEquals((long) 100, resultSet.getLong(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(0, resultSet.getLong(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn((long) 100);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals((long) 100, resultSet.getLong("columnLabel"));
  }

  @Test
  void testGetFloat() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(0)).thenReturn((float) 100);
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.FLOAT);
    assertEquals(100f, resultSet.getFloat(1));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(0, resultSet.getFloat(1));
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn((float) 100);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals(100f, resultSet.getFloat("columnLabel"));
  }

  @Test
  void testGetUnicode() throws SQLException, UnsupportedEncodingException {
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
    BigDecimal expected = new BigDecimal("123.45");
    when(mockedExecutionResult.getObject(1)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnType(2)).thenReturn(Types.DECIMAL);
    assertEquals(expected, resultSet.getBigDecimal(2));
    // null object
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    assertEquals(BigDecimal.ZERO, resultSet.getBigDecimal(1));
    // Test with column label
    when(mockedExecutionResult.getObject(1)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(2);
    assertEquals(expected, resultSet.getBigDecimal("columnLabel"));
  }

  @Test
  void testGetDate() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    Date expected = Date.valueOf("2023-01-01");
    when(mockedExecutionResult.getObject(2)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnType(3)).thenReturn(Types.DATE);
    assertEquals(expected, resultSet.getDate(3));
    // null object
    when(mockedExecutionResult.getObject(2)).thenReturn(null);
    assertEquals(null, resultSet.getDate(3));
    // Test with column label
    when(mockedExecutionResult.getObject(2)).thenReturn(expected);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(3);
    assertEquals(expected, resultSet.getDate("columnLabel"));
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
  void testGetTimestamp() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    int columnIndex = 5;
    Timestamp expectedTimestamp = new Timestamp(System.currentTimeMillis());
    when(resultSet.getObject(columnIndex)).thenReturn(expectedTimestamp);
    when(mockedResultSetMetadata.getColumnType(columnIndex)).thenReturn(java.sql.Types.TIMESTAMP);
    assertEquals(expectedTimestamp, resultSet.getTimestamp(columnIndex));
    // null object
    when(mockedExecutionResult.getObject(2)).thenReturn(null);
    assertNull(resultSet.getTimestamp(3));
    // Test with column label
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(columnIndex);
    assertEquals(expectedTimestamp, resultSet.getTimestamp("columnLabel"));
  }

  @Test
  void testGetTimestampWithCalendar() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    int columnIndex = 5;
    String columnLabel = "columnLabel";
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
    Timestamp expectedTimestamp = new Timestamp(System.currentTimeMillis());

    // Mocking for columnIndex
    when(resultSet.getObject(columnIndex)).thenReturn(expectedTimestamp);
    when(mockedResultSetMetadata.getColumnType(columnIndex)).thenReturn(java.sql.Types.TIMESTAMP);
    assertEquals(expectedTimestamp, resultSet.getTimestamp(columnIndex, cal));

    // Mocking for columnLabel
    when(mockedResultSetMetadata.getColumnNameIndex(columnLabel)).thenReturn(columnIndex);
    assertEquals(expectedTimestamp, resultSet.getTimestamp(columnLabel, cal));
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
  void testGetObject() throws SQLException {
    String expected = "testObject";
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    when(mockedExecutionResult.getObject(2)).thenReturn(expected);
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
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.insertRow());
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateRow());
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.deleteRow());
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.refreshRow());
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.cancelRowUpdates());
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.moveToInsertRow());
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.moveToCurrentRow());
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
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.last());
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.beforeFirst());
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.afterLast());
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.absolute(1));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.relative(1));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.previous());

    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.setFetchDirection(ResultSet.FETCH_REVERSE));
  }

  @Test
  void testUnsupportedOperationsThrowDatabricksSQLFeatureNotSupportedException() {
    when(mockedResultSetMetadata.getColumnNameIndex("column")).thenReturn(1);
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::getHoldability);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getBlob(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getBlob("column"));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getClob(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getClob("column"));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getArray(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getArray("column"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.getDate(1, new GregorianCalendar()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.getDate("column", new GregorianCalendar()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.getTime(1, new GregorianCalendar()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.getTime("column", new GregorianCalendar()));
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getTime(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.getTime("column"));
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
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.unwrap(null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.isWrapperFor(null));
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
        getResultSet(StatementState.SUCCEEDED, mockedDatabricksStatement);
    HttpEntity mockEntity = mock(HttpEntity.class);
    when(mockEntity.getContentLength()).thenReturn(10L);
    resultSet.setVolumeOperationEntityStream(mockEntity);
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
            "test-statementID",
            StatementType.QUERY,
            null,
            mockedExecutionResult,
            mockedResultSetMetadata);

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
            "test-statementID",
            StatementType.UPDATE,
            null,
            mockedExecutionResult,
            mockedResultSetMetadata);

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
            "test-statementID",
            StatementType.UPDATE,
            null,
            mockedExecutionResult,
            mockedResultSetMetadata);

    assertEquals(5L, resultSet.getUpdateCount());
  }

  @Test
  void testGetUpdateCountForClosedResultSet() throws SQLException {
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    resultSet.close();
    assertThrows(DatabricksSQLException.class, resultSet::getUpdateCount);
  }
}
