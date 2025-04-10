package com.databricks.jdbc.api.impl.batch;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatabricksBatchExecutorTest {
  @Mock IDatabricksConnectionContext connectionContext;
  @Mock private IDatabricksStatement mockStatement;
  private DatabricksBatchExecutor databricksBatchExecutor;
  private final int MAX_BATCH_SIZE = 5;

  @BeforeEach
  public void setUp() {
    databricksBatchExecutor = new DatabricksBatchExecutor(mockStatement, MAX_BATCH_SIZE);
  }

  /** Test adding valid commands to the batch. */
  @Test
  public void testAddCommand_Success() throws SQLException {
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (1)");
    databricksBatchExecutor.addCommand("UPDATE table2 SET column='value'");
    // No exception should be thrown
    assertEquals(2, databricksBatchExecutor.commands.size());
  }

  /** Test adding a null command to the batch. */
  @Test
  public void testAddCommand_NullCommand() {
    SQLException exception =
        assertThrows(SQLException.class, () -> databricksBatchExecutor.addCommand(null));
    assertEquals("SQL command cannot be null", exception.getMessage());
  }

  /** Test exceeding the batch size limit. */
  @Test
  public void testAddCommand_ExceedsBatchSizeLimit() throws SQLException {
    for (int i = 0; i < MAX_BATCH_SIZE; i++) {
      databricksBatchExecutor.addCommand("INSERT INTO table VALUES (" + i + ")");
    }
    // Next command should throw an exception
    SQLException exception =
        assertThrows(
            SQLException.class,
            () -> databricksBatchExecutor.addCommand("INSERT INTO table VALUES (999)"));
    assertEquals(
        "Batch size limit exceeded. Maximum allowed is " + MAX_BATCH_SIZE, exception.getMessage());
  }

  /** Test clearing the batch commands. */
  @Test
  public void testClearCommands() throws SQLException {
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (1)");
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (2)");
    assertEquals(2, databricksBatchExecutor.commands.size());

    databricksBatchExecutor.clearCommands();
    assertEquals(0, databricksBatchExecutor.commands.size());
  }

  /** Test executing an empty batch. */
  @Test
  public void testExecuteBatch_EmptyBatch() throws SQLException {
    int[] updateCounts = databricksBatchExecutor.executeBatch();
    assertEquals(0, updateCounts.length);
  }

  /** Test executing a batch where all commands succeed. */
  @Test
  public void testExecuteBatch_AllCommandsSucceed() throws SQLException {
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (1)");
    databricksBatchExecutor.addCommand("UPDATE table2 SET column='value'");
    databricksBatchExecutor.addCommand("DELETE FROM table3 WHERE id=3");

    when(mockStatement.execute(anyString())).thenReturn(false);
    when(mockStatement.getUpdateCount()).thenReturn(1);

    int[] updateCounts = databricksBatchExecutor.executeBatch();

    assertEquals(3, updateCounts.length);
    assertArrayEquals(new int[] {1, 1, 1}, updateCounts);

    verify(mockStatement, times(3)).execute(anyString());
    verify(mockStatement, times(3)).getUpdateCount();
    assertEquals(0, databricksBatchExecutor.commands.size());
  }

  /** Test executing a batch where a command fails with SQLException. */
  @Test
  public void testExecuteBatch_CommandFails() throws SQLException {
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (1)");
    databricksBatchExecutor.addCommand("BAD SQL COMMAND");
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (3)");

    when(mockStatement.execute(anyString()))
        .thenReturn(false)
        .thenThrow(new SQLException("Syntax error"))
        .thenReturn(false);
    when(mockStatement.getUpdateCount()).thenReturn(1);

    BatchUpdateException exception =
        assertThrows(BatchUpdateException.class, () -> databricksBatchExecutor.executeBatch());

    assertEquals("Batch execution failed at command 1: Syntax error", exception.getMessage());
    assertArrayEquals(new int[] {1}, exception.getUpdateCounts());

    verify(mockStatement, times(2)).execute(anyString());
    verify(mockStatement, times(1)).getUpdateCount();
    assertEquals(0, databricksBatchExecutor.commands.size());
  }

  /** Test executing a batch where a command returns a ResultSet. */
  @Test
  public void testExecuteBatch_CommandReturnsResultSet() throws SQLException {
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (1)");
    databricksBatchExecutor.addCommand("SELECT * FROM table1");
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (3)");

    when(mockStatement.execute(anyString()))
        .thenReturn(false)
        .thenReturn(true) // Returns ResultSet
        .thenReturn(false);
    when(mockStatement.getUpdateCount()).thenReturn(1);

    BatchUpdateException exception =
        assertThrows(BatchUpdateException.class, () -> databricksBatchExecutor.executeBatch());

    assertEquals(
        "Batch execution failed at command 1: Command 1 in the batch attempted to return a ResultSet",
        exception.getMessage());
    assertArrayEquals(new int[] {1}, exception.getUpdateCounts());

    verify(mockStatement, times(2)).execute(anyString());
    verify(mockStatement, times(2)).getUpdateCount();
    assertEquals(0, databricksBatchExecutor.commands.size());
  }

  /** Test that after executing a batch, the batch is cleared. */
  @Test
  public void testBatchClearedAfterExecution() throws SQLException {
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (1)");
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (2)");

    when(mockStatement.execute(anyString())).thenReturn(false);
    when(mockStatement.getUpdateCount()).thenReturn(1);

    databricksBatchExecutor.executeBatch();

    assertEquals(0, databricksBatchExecutor.commands.size());
  }

  /** Test that telemetry methods are invoked. */
  @Test
  public void testTelemetryMethodsInvoked() throws SQLException {
    databricksBatchExecutor.addCommand("INSERT INTO table1 VALUES (1)");

    when(mockStatement.execute(anyString())).thenReturn(false);
    when(mockStatement.getUpdateCount()).thenReturn(1);

    // Spy on the databricksBatchExecutor to verify method calls
    DatabricksBatchExecutor spyDatabricksBatchExecutor = Mockito.spy(databricksBatchExecutor);

    spyDatabricksBatchExecutor.executeBatch();

    verify(spyDatabricksBatchExecutor, times(1))
        .logCommandExecutionTime(anyInt(), any(Instant.class), eq(true));
  }
}
