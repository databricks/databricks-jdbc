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
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::rowUpdated);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::rowInserted);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::rowDeleted);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::insertRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::updateRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::deleteRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::refreshRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::cancelRowUpdates);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::moveToInsertRow);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::moveToCurrentRow);

    assertThrows(DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateNull(1));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateByte(1, (byte) 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateShort(1, (short) 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateInt(1, 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateLong(1, 100L));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateFloat(1, 100.0f));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateDouble(1, 100.0));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBigDecimal(1, new BigDecimal("123.456")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateString(1, "test"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBytes(1, new byte[] {0x01, 0x02}));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateDate(1, Date.valueOf("2021-01-01")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTime(1, Time.valueOf("12:00:00")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTimestamp(1, Timestamp.valueOf("2021-01-01 12:00:00")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(1, InputStream.nullInputStream(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(1, InputStream.nullInputStream(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream(1, Reader.nullReader(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject(1, new Object(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject(1, new Object()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateNull("columnLabel"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBoolean("columnLabel", false));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateBoolean(1, false));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateByte("columnLabel", (byte) 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateShort("columnLabel", (short) 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateInt("columnLabel", 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateLong("columnLabel", 100L));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateFloat("columnLabel", 100.0f));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateDouble("columnLabel", 100.0));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBigDecimal("columnLabel", new BigDecimal("123.456")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateString("columnLabel", "test"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBytes("columnLabel", new byte[] {0x01, 0x02}));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateDate("columnLabel", Date.valueOf("2021-01-01")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTime("columnLabel", Time.valueOf("12:00:00")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTimestamp("columnLabel", Timestamp.valueOf("2021-01-01 12:00:00")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("columnLabel", InputStream.nullInputStream(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("columnLabel", InputStream.nullInputStream(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("columnLabel", Reader.nullReader(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject("columnLabel", new Object(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject("columnLabel", new Object()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateNString(1, "test"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNString("columnLabel", "test"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob("columnLabel", (NClob) null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob(1, (NClob) null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateSQLXML(1, null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateSQLXML("columnLabel", null));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(1, InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(1, InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream(1, Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("columnLabel", InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("columnLabel", InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("columnLabel", Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBlob(1, InputStream.nullInputStream()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBlob("columnLabel", InputStream.nullInputStream()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateClob(1, Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateClob("columnLabel", Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob(1, Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob("columnLabel", Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNCharacterStream(1, Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNCharacterStream("columnLabel", Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(1, InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(1, InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream(1, Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("columnLabel", InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("columnLabel", InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("columnLabel", Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBlob(1, InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBlob("columnLabel", InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateClob(1, Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateClob("columnLabel", Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob(1, Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNClob("columnLabel", Reader.nullReader(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNCharacterStream(1, Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateNCharacterStream("columnLabel", Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(1, InputStream.nullInputStream()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(1, InputStream.nullInputStream()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream(1, Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("columnLabel", Reader.nullReader()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("columnLabel", InputStream.nullInputStream()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("columnLabel", InputStream.nullInputStream()));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream(1, InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream(1, InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("columnLabel", InputStream.nullInputStream(), 10));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("columnLabel", InputStream.nullInputStream(), 10));
  }

  @Test
  void testUnimplementedFunctions() {
    // This test is to improve coverage. We need to write actual tests once we start implementing
    // them.
    when(mockedResultSetMetadata.getColumnNameIndex("columnLabel")).thenReturn(1);
    DatabricksResultSet resultSet = getResultSet(StatementState.PENDING, null);
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getNClob(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getNClob("columnLabel"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getSQLXML(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getSQLXML("columnLabel"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getNString(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getNString("columnLabel"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getNCharacterStream(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getBlob(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getClob(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getArray(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getRef(1));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getBlob("columnLabel"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getClob("columnLabel"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getArray("columnLabel"));
    assertThrows(UnsupportedOperationException.class, () -> resultSet.getRef("columnLabel"));
    assertThrows(UnsupportedOperationException.class, resultSet::getHoldability);
    assertThrows(
        UnsupportedOperationException.class, () -> resultSet.getNCharacterStream("columnLabel"));
  }
}
