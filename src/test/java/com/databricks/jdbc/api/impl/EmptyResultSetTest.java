package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.service.sql.StatementState;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EmptyResultSetTest {
  private EmptyResultSet resultSet;

  @BeforeEach
  public void setup() {
    resultSet = new EmptyResultSet();
  }

  @Test
  public void testWasNull() throws SQLException {
    assertFalse(resultSet.wasNull());
  }

  @Test
  public void testNext() throws SQLException {
    assertFalse(resultSet.next());
  }

  @Test
  public void testClose() throws SQLException {
    resultSet.close();
    assertTrue(resultSet.isClosed());
  }

  @Test
  public void testGetBoolean() throws SQLException {
    assertFalse(resultSet.getBoolean(1));
  }

  @Test
  public void testGetInt() throws SQLException {
    assertEquals(0, resultSet.getInt(1));
  }

  @Test
  public void testGetLong() throws SQLException {
    assertEquals(0L, resultSet.getLong(1));
  }

  @Test
  public void testGetFloat() throws SQLException {
    assertEquals(0.0f, resultSet.getFloat(1));
  }

  @Test
  public void testGetDouble() throws SQLException {
    assertEquals(0.0, resultSet.getDouble(1));
  }

  @Test
  public void testGetShort() throws SQLException {
    assertEquals((short) 0, resultSet.getShort(1));
  }

  @Test
  public void testGetByte() throws SQLException {
    assertEquals((byte) 0, resultSet.getByte(1));
  }

  @Test
  public void testGetString() throws SQLException {
    assertEquals("", resultSet.getString(1));
  }

  @Test
  public void testGetBytes() throws SQLException {
    assertArrayEquals(new byte[0], resultSet.getBytes(1));
  }

  @Test
  public void testGetDate() throws SQLException {
    assertNull(resultSet.getDate(1));
  }

  @Test
  public void testGetTime_returnsNull() throws SQLException {
    assertNull(resultSet.getTime(1));
  }

  @Test
  public void testGetTimestamp_returnsNull() throws SQLException {
    assertNull(resultSet.getTimestamp(1));
  }

  @Test
  public void testGetAsciiStream() throws SQLException {
    assertNull(resultSet.getAsciiStream(1));
  }

  @Test
  public void testGetUnicodeStream_returnsNull() throws SQLException {
    assertNull(resultSet.getUnicodeStream(1));
  }

  @Test
  public void testGetBinaryStream() throws SQLException {
    assertNull(resultSet.getBinaryStream(1));
  }

  @Test
  public void testGetWarnings() throws SQLException {
    assertNull(resultSet.getWarnings());
  }

  @Test
  public void testGetCursorName_returnsNull() throws SQLException {
    assertNull(resultSet.getCursorName());
  }

  @Test
  public void testGetMetaData() throws SQLException {
    assertEquals(resultSet.getMetaData(), new EmptyResultSetMetaData());
  }

  @Test
  public void testGetObject() throws SQLException {
    assertNull(resultSet.getObject(1));
  }

  @Test
  public void testFindColumn() throws SQLException {
    assertEquals(resultSet.findColumn("column"), 0);
  }

  @Test
  public void testGetCharacterStream() throws SQLException {
    assertNull(resultSet.getCharacterStream(1));
  }

  @Test
  public void testUnwrap_returnsNull() throws SQLException {
    assertNull(resultSet.unwrap(Object.class));
  }

  @Test
  public void testIsWrapperFor_returnFalse() throws SQLException {
    assertFalse(resultSet.isWrapperFor(Object.class));
  }

  @Test
  public void testGetCharacterStreamByLabel() throws SQLException {
    assertNull(resultSet.getCharacterStream("column"));
  }

  @Test
  public void testGetHoldability() throws SQLException {
    assertEquals(0, resultSet.getHoldability());
  }

  @Test
  public void testIsClosed() throws SQLException {
    resultSet.close();
    assertTrue(resultSet.isClosed());
  }

  @Test
  public void testGetStatement() throws SQLException {
    assertNull(resultSet.getStatement());
  }

  @Test
  public void testGetConcurrency() throws SQLException {
    assertEquals(0, resultSet.getConcurrency());
  }

  @Test
  public void testGetType() throws SQLException {
    assertEquals(0, resultSet.getType());
  }

  @Test
  public void testGetFetchSize() throws SQLException {
    assertEquals(0, resultSet.getFetchSize());
  }

  @Test
  public void testGetFetchDirection() throws SQLException {
    assertEquals(0, resultSet.getFetchDirection());
  }

  @Test
  public void testIsLast() throws SQLException {
    assertFalse(resultSet.isLast());
  }

  @Test
  public void testIsFirst() throws SQLException {
    assertFalse(resultSet.isFirst());
  }

  @Test
  public void testIsAfterLast() throws SQLException {
    assertFalse(resultSet.isAfterLast());
  }

  @Test
  public void testIsBeforeFirst() throws SQLException {
    assertFalse(resultSet.isBeforeFirst());
  }

  @Test
  public void testGetObjectWithMap() throws SQLException {
    assertNull(resultSet.getObject(1, Map.of()));
  }

  @Test
  public void testGetRef() throws SQLException {
    assertNull(resultSet.getRef(1));
  }

  @Test
  public void testGetBlob() throws SQLException {
    assertNull(resultSet.getBlob(1));
  }

  @Test
  public void testGetClob() throws SQLException {
    assertNull(resultSet.getClob(1));
  }

  @Test
  public void testGetArray() throws SQLException {
    assertNull(resultSet.getArray(1));
  }

  @Test
  public void testGetObjectWithMapAndLabel() throws SQLException {
    assertNull(resultSet.getObject("column", Map.of()));
  }

  @Test
  public void testGetRefWithLabel() throws SQLException {
    assertNull(resultSet.getRef("column"));
  }

  @Test
  public void testGetBlobWithLabel() throws SQLException {
    assertNull(resultSet.getBlob("column"));
  }

  @Test
  public void testGetClobWithLabel() throws SQLException {
    assertNull(resultSet.getClob("column"));
  }

  @Test
  public void testGetArrayWithLabel() throws SQLException {
    assertNull(resultSet.getArray("column"));
  }

  @Test
  public void testGetDateWithCalendar() throws SQLException {
    assertNull(resultSet.getDate(1, Calendar.getInstance()));
  }

  @Test
  public void testGetDateWithCalendarAndLabel() throws SQLException {
    assertNull(resultSet.getDate("column", Calendar.getInstance()));
  }

  @Test
  public void testGetTimeWithCalendar() throws SQLException {
    assertNull(resultSet.getTime(1, Calendar.getInstance()));
  }

  @Test
  public void testGetTimeWithCalendarAndLabel() throws SQLException {
    assertNull(resultSet.getTime("column", Calendar.getInstance()));
  }

  @Test
  public void testGetTimestampWithCalendar() throws SQLException {
    assertNull(resultSet.getTimestamp(1, Calendar.getInstance()));
  }

  @Test
  public void testGetTimestampWithCalendarAndLabel() throws SQLException {
    assertNull(resultSet.getTimestamp("column", Calendar.getInstance()));
  }

  @Test
  public void testGetURL() throws SQLException {
    assertNull(resultSet.getURL(1));
  }

  @Test
  public void testGetURLWithLabel() throws SQLException {
    assertNull(resultSet.getURL("column"));
  }

  @Test
  public void testGetRowId() throws SQLException {
    assertNull(resultSet.getRowId(1));
  }

  @Test
  public void testGetRowIdWithLabel() throws SQLException {
    assertNull(resultSet.getRowId("column"));
  }

  @Test
  public void testGetNClob() throws SQLException {
    assertNull(resultSet.getNClob(1));
  }

  @Test
  public void testGetNClobWithLabel() throws SQLException {
    assertNull(resultSet.getNClob("column"));
  }

  @Test
  public void testGetSQLXML() throws SQLException {
    assertNull(resultSet.getSQLXML(1));
  }

  @Test
  public void testGetSQLXMLWithLabel() throws SQLException {
    assertNull(resultSet.getSQLXML("column"));
  }

  @Test
  public void testGetNString() throws SQLException {
    assertNull(resultSet.getNString(1));
  }

  @Test
  public void testGetNStringWithLabel() throws SQLException {
    assertNull(resultSet.getNString("column"));
  }

  @Test
  public void testGetNCharacterStream() throws SQLException {
    assertNull(resultSet.getNCharacterStream(1));
  }

  @Test
  public void testGetNCharacterStreamWithLabel() throws SQLException {
    assertNull(resultSet.getNCharacterStream("column"));
  }

  @Test
  public void testGetObjectWithClass() throws SQLException {
    assertNull(resultSet.getObject(1, Object.class));
  }

  @Test
  public void testGetObjectWithClassAndLabel() throws SQLException {
    assertNull(resultSet.getObject("column", Object.class));
  }

  @Test
  public void testGetBigDecimalWithScale() throws SQLException {
    assertNull(resultSet.getBigDecimal(1, 2));
  }

  @Test
  public void testGetAsciiStreamWithLabel() throws SQLException {
    assertNull(resultSet.getAsciiStream("column"));
  }

  @Test
  public void testGetMethodsReturnZeroOrEquivalent() throws SQLException {
    EmptyResultSet resultSet =
        new EmptyResultSet(); // Assuming the constructor doesn't throw SQLException

    // Numeric and boolean types return 0 or false
    assertEquals(0, resultSet.getInt("anyString"));
    assertEquals(0L, resultSet.getLong("anyString"));
    assertEquals(0.0f, resultSet.getFloat("anyString"));
    assertEquals(0.0, resultSet.getDouble("anyString"));
    assertEquals((short) 0, resultSet.getShort("anyString"));
    assertEquals((byte) 0, resultSet.getByte("anyString"));
    assertEquals(resultSet.getBytes("anyString").length, 0);
    assertFalse(resultSet.getBoolean("anyString"));

    assertEquals(0, resultSet.getRow());
    assertFalse(resultSet.first());
    assertFalse(resultSet.last());
    assertFalse(resultSet.absolute(1));
    assertFalse(resultSet.relative(1));
    assertFalse(resultSet.previous());
    assertFalse(resultSet.rowUpdated());
    assertFalse(resultSet.rowInserted());
    assertFalse(resultSet.rowDeleted());

    // Object types return null
    assertNull(resultSet.getBigDecimal("anyString"));
    assertNull(resultSet.getBigDecimal("anyString", 2));
    assertEquals(resultSet.getString("anyString"), "");
    assertNull(resultSet.getDate("anyString"));
    assertNull(resultSet.getTime("anyString"));
    assertNull(resultSet.getTimestamp("anyString"));
    assertNull(resultSet.getUnicodeStream("anyString"));
    assertNull(resultSet.getBinaryStream("anyString"));
    assertNull(resultSet.getObject("anyString"));

    // For getBigDecimal(int) - assuming 1 as a placeholder for column index
    assertNull(resultSet.getBigDecimal(1));
    assertTrue(resultSet.getStatementId().isEmpty());
    assertEquals(StatementState.SUCCEEDED, resultSet.getStatementStatus().getState());
    assertEquals(0, resultSet.getUpdateCount());
    assertFalse(resultSet.hasUpdateCount());
  }

  @Test
  public void testUpdateMethodsDoNotThrowExceptions() {
    EmptyResultSet resultSet = new EmptyResultSet(); // Initialize your EmptyResultSet here

    // Numeric and boolean updates
    assertDoesNotThrow(() -> resultSet.updateNull(1));
    assertDoesNotThrow(() -> resultSet.updateBoolean(1, true));
    assertDoesNotThrow(() -> resultSet.updateByte(1, (byte) 0));
    assertDoesNotThrow(() -> resultSet.updateShort(1, (short) 0));
    assertDoesNotThrow(() -> resultSet.updateInt(1, 0));
    assertDoesNotThrow(() -> resultSet.updateLong(1, 0L));
    assertDoesNotThrow(() -> resultSet.updateFloat(1, 0.0f));
    assertDoesNotThrow(() -> resultSet.updateDouble(1, 0.0));
    assertDoesNotThrow(() -> resultSet.updateBigDecimal(1, BigDecimal.ZERO));
    assertDoesNotThrow(() -> resultSet.updateString(1, ""));
    assertDoesNotThrow(() -> resultSet.updateBytes(1, new byte[0]));
    assertDoesNotThrow(() -> resultSet.updateNull("columnIndex"));
    assertDoesNotThrow(() -> resultSet.updateBoolean("columnIndex", true));
    assertDoesNotThrow(() -> resultSet.updateByte("columnIndex", (byte) 0));
    assertDoesNotThrow(() -> resultSet.updateShort("columnIndex", (short) 0));
    assertDoesNotThrow(() -> resultSet.updateInt("columnIndex", 0));
    assertDoesNotThrow(() -> resultSet.updateLong("columnIndex", 0L));
    assertDoesNotThrow(() -> resultSet.updateFloat("columnIndex", 0.0f));
    assertDoesNotThrow(() -> resultSet.updateDouble("columnIndex", 0.0));
    assertDoesNotThrow(() -> resultSet.updateBigDecimal("columnIndex", BigDecimal.ZERO));
    assertDoesNotThrow(() -> resultSet.updateString("columnIndex", ""));
    assertDoesNotThrow(() -> resultSet.updateBytes("columnIndex", new byte[0]));

    assertDoesNotThrow(resultSet::beforeFirst);
    assertDoesNotThrow(resultSet::afterLast);
    assertDoesNotThrow(() -> resultSet.setFetchDirection(ResultSet.FETCH_FORWARD));
    assertDoesNotThrow(() -> resultSet.setFetchSize(0));
    assertDoesNotThrow(resultSet::clearWarnings);

    // Object and stream updates
    assertDoesNotThrow(() -> resultSet.updateDate(1, Date.valueOf("2020-01-01")));
    assertDoesNotThrow(() -> resultSet.updateTime(1, Time.valueOf("00:00:00")));
    assertDoesNotThrow(
        () -> resultSet.updateTimestamp(1, Timestamp.valueOf("2020-01-01 00:00:00")));
    assertDoesNotThrow(() -> resultSet.updateAsciiStream(1, null, 0));
    assertDoesNotThrow(() -> resultSet.updateBinaryStream(1, null, 0));
    assertDoesNotThrow(() -> resultSet.updateCharacterStream(1, null, 0));
    assertDoesNotThrow(() -> resultSet.updateObject(1, null));
    assertDoesNotThrow(() -> resultSet.updateObject(1, null, 1));
    assertDoesNotThrow(() -> resultSet.updateRowId(1, null));
    assertDoesNotThrow(() -> resultSet.updateDate("columnIndex", Date.valueOf("2020-01-01")));
    assertDoesNotThrow(() -> resultSet.updateTime("columnIndex", Time.valueOf("00:00:00")));
    assertDoesNotThrow(
        () -> resultSet.updateTimestamp("columnIndex", Timestamp.valueOf("2020-01-01 00:00:00")));
    assertDoesNotThrow(() -> resultSet.updateAsciiStream("columnIndex", null, 0));
    assertDoesNotThrow(() -> resultSet.updateBinaryStream("columnIndex", null, 0));
    assertDoesNotThrow(() -> resultSet.updateCharacterStream("columnIndex", null, 0));
    assertDoesNotThrow(() -> resultSet.updateObject("columnIndex", null));
    assertDoesNotThrow(() -> resultSet.updateObject("columnIndex", null, 1));
    assertDoesNotThrow(() -> resultSet.updateRowId("columnIndex", null));

    // Large object and specific type updates
    assertDoesNotThrow(() -> resultSet.updateBlob(1, (Blob) null));
    assertDoesNotThrow(() -> resultSet.updateClob(1, (Clob) null));
    assertDoesNotThrow(() -> resultSet.updateArray(1, null));
    assertDoesNotThrow(() -> resultSet.updateRef(1, null));
    assertDoesNotThrow(() -> resultSet.updateNString(1, ""));
    assertDoesNotThrow(() -> resultSet.updateNClob(1, (NClob) null));
    assertDoesNotThrow(() -> resultSet.updateSQLXML(1, null));
    assertDoesNotThrow(() -> resultSet.updateBlob("columnIndex", (Blob) null));
    assertDoesNotThrow(() -> resultSet.updateClob("columnIndex", (Clob) null));
    assertDoesNotThrow(() -> resultSet.updateArray("columnIndex", null));
    assertDoesNotThrow(() -> resultSet.updateRef("columnIndex", null));
    assertDoesNotThrow(() -> resultSet.updateNString("columnIndex", ""));
    assertDoesNotThrow(() -> resultSet.updateNClob("columnIndex", (NClob) null));
    assertDoesNotThrow(() -> resultSet.updateSQLXML("columnIndex", null));

    // Row updates
    assertDoesNotThrow(resultSet::insertRow);
    assertDoesNotThrow(resultSet::updateRow);
    assertDoesNotThrow(resultSet::deleteRow);
    assertDoesNotThrow(resultSet::refreshRow);
    assertDoesNotThrow(resultSet::cancelRowUpdates);
    assertDoesNotThrow(resultSet::moveToInsertRow);
    assertDoesNotThrow(resultSet::moveToCurrentRow);

    // Stream updates with length
    assertDoesNotThrow(() -> resultSet.updateBlob(1, null, 0L));
    assertDoesNotThrow(() -> resultSet.updateClob(1, null, 0L));
    assertDoesNotThrow(() -> resultSet.updateNClob(1, null, 0L));
    assertDoesNotThrow(() -> resultSet.updateAsciiStream(1, null, 0L));
    assertDoesNotThrow(() -> resultSet.updateBinaryStream(1, null, 0L));
    assertDoesNotThrow(() -> resultSet.updateCharacterStream(1, null, 0L));
    assertDoesNotThrow(() -> resultSet.updateNCharacterStream(1, null, 0L));
    assertDoesNotThrow(() -> resultSet.updateBlob("columnIndex", null, 0L));
    assertDoesNotThrow(() -> resultSet.updateClob("columnIndex", null, 0L));
    assertDoesNotThrow(() -> resultSet.updateNClob("columnIndex", null, 0L));
    assertDoesNotThrow(() -> resultSet.updateAsciiStream("columnIndex", null, 0L));
    assertDoesNotThrow(() -> resultSet.updateBinaryStream("columnIndex", null, 0L));
    assertDoesNotThrow(() -> resultSet.updateCharacterStream("columnIndex", null, 0L));
    assertDoesNotThrow(() -> resultSet.updateNCharacterStream("columnIndex", null, 0L));

    // Large object updates with InputStream and Reader
    ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
    StringReader reader = new StringReader("");
    assertDoesNotThrow(() -> resultSet.updateAsciiStream(1, inputStream));
    assertDoesNotThrow(() -> resultSet.updateBinaryStream(1, inputStream));
    assertDoesNotThrow(() -> resultSet.updateCharacterStream(1, reader));
    assertDoesNotThrow(() -> resultSet.updateNCharacterStream(1, reader));
    assertDoesNotThrow(() -> resultSet.updateBlob(1, inputStream));
    assertDoesNotThrow(() -> resultSet.updateClob(1, reader));
    assertDoesNotThrow(() -> resultSet.updateNClob(1, reader));
    assertDoesNotThrow(() -> resultSet.updateAsciiStream("columnIndex", inputStream));
    assertDoesNotThrow(() -> resultSet.updateBinaryStream("columnIndex", inputStream));
    assertDoesNotThrow(() -> resultSet.updateCharacterStream("columnIndex", reader));
    assertDoesNotThrow(() -> resultSet.updateNCharacterStream("columnIndex", reader));
    assertDoesNotThrow(() -> resultSet.updateBlob("columnIndex", inputStream));
    assertDoesNotThrow(() -> resultSet.updateClob("columnIndex", reader));
    assertDoesNotThrow(() -> resultSet.updateNClob("columnIndex", reader));
  }
}
