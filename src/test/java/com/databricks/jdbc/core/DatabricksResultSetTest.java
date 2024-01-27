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

  private DatabricksResultSet getResultSet() {
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "test-statementID",
        StatementType.METADATA,
        null,
        mockedExecutionResult,
        mockedResultSetMetadata);
  }

  @Test
  void testClose() throws SQLException {
    DatabricksResultSet resultSet = getResultSet();
    assertFalse(resultSet.isClosed());
    resultSet.close();
    assertTrue(resultSet.isClosed());
    assertThrows(DatabricksSQLException.class, resultSet::next);
  }

  @Test
  void testNext() throws SQLException {
    when(mockedExecutionResult.next()).thenReturn(true);
    DatabricksResultSet resultSet = getResultSet();
    assertTrue(resultSet.next());
  }

  @Test
  void testUpdateFunctionsThrowsError() {
    DatabricksResultSet resultSet = getResultSet();
    // column int updates
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::rowUpdated);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::rowInserted);
    assertThrows(DatabricksSQLFeatureNotSupportedException.class, resultSet::rowDeleted);
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
    // column label updates
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateNull("col"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBoolean("col", false));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateByte("col", (byte) 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateShort("col", (short) 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateInt("col", 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> resultSet.updateLong("col", 100L));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateFloat("col", 100.0f));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateDouble("col", 100.0));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBigDecimal("col", new BigDecimal("123.456")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateString("col", "test"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBytes("col", new byte[] {0x01, 0x02}));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateDate("col", Date.valueOf("2021-01-01")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTime("col", Time.valueOf("12:00:00")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateTimestamp("col", Timestamp.valueOf("2021-01-01 12:00:00")));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateAsciiStream("col", InputStream.nullInputStream(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateBinaryStream("col", InputStream.nullInputStream(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateCharacterStream("col", Reader.nullReader(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject("col", new Object(), 100));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> resultSet.updateObject("col", new Object()));
  }

  @Test
  void testGetStringAndWasNull() throws SQLException {
    when(mockedExecutionResult.getObject(0)).thenReturn(null);
    DatabricksResultSet resultSet = getResultSet();
    assertNull(resultSet.getString(1));
    assertTrue(resultSet.wasNull());
    when(mockedExecutionResult.getObject(0)).thenReturn("test");
    when(mockedResultSetMetadata.getColumnType(1)).thenReturn(Types.VARCHAR);
    assertEquals("test", resultSet.getString(1));
    assertFalse(resultSet.wasNull());
  }
}
