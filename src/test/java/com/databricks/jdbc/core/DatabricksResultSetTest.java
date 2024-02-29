package com.databricks.jdbc.core;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.StatementType;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksResultSetTest {
  @Mock InlineJsonResult mockedExecutionResult;
  @Mock DatabricksResultSetMetaData mockedResultSetMetadata;

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
  void testNext() throws SQLException {
    when(mockedExecutionResult.next()).thenReturn(true);
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    assertTrue(resultSet.next());
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
    // Test with column label
    when(mockedExecutionResult.getObject(0)).thenReturn((short) 100);
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    assertEquals((short) 100, resultSet.getShort("columnLabel"));
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
        () -> resultSet.updateObject("column", new Object(), 1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject(1, new Object()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject("column", new Object()));

    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.first());
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
  void testUnsupportedOperationsThrowUnsupportedOperationException() {
    when(mockedResultSetMetadata.getColumnNameIndex("column")).thenReturn(1);
    DatabricksResultSet resultSet = getResultSet(StatementState.SUCCEEDED, null);
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getTime(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getTime("column"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getURL(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getURL("column"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.updateRef(1, null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.updateRef("column", null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.updateBlob(1, (Blob) null));
    assertThrows(
        UnsupportedOperationException.class, () -> resultSet.updateBlob("column", (Blob) null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.updateClob(1, (Clob) null));
    assertThrows(
        UnsupportedOperationException.class, () -> resultSet.updateClob("column", (Clob) null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.updateArray(1, null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.updateArray("column", null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getRowId(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getRowId("column"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.updateRowId(1, null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.updateRowId("column", null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getNClob(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getNClob("column"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getSQLXML(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getSQLXML("column"));
    assertThrows(
        UnsupportedOperationException.class,
        () -> resultSet.getTimestamp(1, Calendar.getInstance()));
    assertThrows(
        UnsupportedOperationException.class,
        () -> resultSet.getTimestamp("column", Calendar.getInstance()));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.unwrap(null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.isWrapperFor(null));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getHoldability());
  }
}
