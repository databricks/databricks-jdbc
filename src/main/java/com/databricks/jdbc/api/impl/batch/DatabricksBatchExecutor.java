package com.databricks.jdbc.api.impl.batch;

import com.databricks.jdbc.exception.DatabricksBatchUpdateException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The {@code DatabricksBatchExecutor} class handles the execution of batch SQL commands. It
 * encapsulates batch logic, maintains the list of commands, and manages the execution flow.
 *
 * <p>This class is responsible for:
 *
 * <ul>
 *   <li>Adding commands to the batch.
 *   <li>Clearing the batch commands.
 *   <li>Executing the batch and handling exceptions according to JDBC specifications.
 *   <li>Tracking telemetry such as execution time for each command and total batch execution time.
 *   <li>Enforcing a maximum batch size limit.
 * </ul>
 */
public class DatabricksBatchExecutor {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksBatchExecutor.class);

  final Statement parentStatement;
  final List<BatchCommand> commands = new ArrayList<>();
  final int maxBatchSize;

  /**
   * Constructs a {@code DatabricksBatchExecutor} with the specified parent {@code Statement} and
   * maximum batch size.
   *
   * @param parentStatement the parent {@code Statement} that will execute the commands
   * @param maxBatchSize the maximum number of commands allowed in the batch
   */
  public DatabricksBatchExecutor(Statement parentStatement, int maxBatchSize) {
    this.parentStatement = parentStatement;
    this.maxBatchSize = maxBatchSize;
  }

  /**
   * Adds a new SQL command to the batch.
   *
   * @param sql the SQL command to be added
   * @throws DatabricksValidationException if the SQL command is null or the batch size limit is
   *     exceeded
   */
  public void addCommand(String sql) throws DatabricksValidationException {
    if (sql == null) {
      throw new DatabricksValidationException("SQL command cannot be null");
    }
    if (commands.size() >= maxBatchSize) {
      throw new DatabricksValidationException(
          "Batch size limit exceeded. Maximum allowed is " + maxBatchSize);
    }
    commands.add(ImmutableBatchCommand.builder().sql(sql).build());
  }

  /** Clears all the commands from the batch. */
  public void clearCommands() {
    commands.clear();
  }

  /**
   * Executes all the commands in the batch sequentially. If any command fails or attempts to return
   * a {@code ResultSet}, the execution stops, and a {@code BatchUpdateException} is thrown.
   *
   * <p>The driver stops execution upon encountering a failure and does not process subsequent
   * commands. This method also tracks and logs telemetry data such as execution time for each
   * command and total batch time.
   *
   * @return an array of update counts for each command in the batch
   * @throws DatabricksBatchUpdateException if a database access error occurs or batch execution
   *     fails
   */
  public int[] executeBatch() throws DatabricksBatchUpdateException {
    if (commands.isEmpty()) {
      LOGGER.warn("No commands to execute in the batch");
      return new int[0];
    }

    int[] updateCounts = new int[commands.size()];
    Instant batchStartTime = Instant.now();

    try {
      for (int i = 0; i < commands.size(); i++) {
        BatchCommand command = commands.get(i);
        Instant commandStartTime = Instant.now();

        try {
          LOGGER.debug(String.format("Executing batch command %d: %s", i, command.getSql()));

          boolean hasResultSet = parentStatement.execute(command.getSql());
          int updateCount = parentStatement.getUpdateCount();

          logCommandExecutionTime(i, commandStartTime, true);

          if (hasResultSet) {
            // According to JDBC spec, batch execution stops if a command returns a ResultSet
            String message =
                String.format("Command %d in the batch attempted to return a ResultSet", i);
            handleBatchFailure(updateCounts, i, batchStartTime, message, null);
          } else if (updateCount != -1) {
            updateCounts[i] = updateCount;
          } else {
            updateCounts[i] = Statement.SUCCESS_NO_INFO;
          }
        } catch (SQLException e) {
          // Command failed, log the error and handle the failure

          logCommandExecutionTime(i, commandStartTime, false);

          LOGGER.error(e, "Error executing batch command at index %d: %s", i, e.getMessage());

          String message =
              String.format("Batch execution failed at command %d: %s", i, e.getMessage());
          handleBatchFailure(updateCounts, i, batchStartTime, message, e);
        }
      }
      // Successfully executed all commands
      clearCommands();

      Duration batchDuration = Duration.between(batchStartTime, Instant.now());
      LOGGER.debug(String.format("Total batch execution time: %d ms", batchDuration.toMillis()));

      return updateCounts;
    } catch (DatabricksBatchUpdateException e) {
      LOGGER.error(e, "BatchUpdateException occurred: %s", e.getMessage());
      throw e;
    }
  }

  /**
   * Logs the execution time of a batch command.
   *
   * @param commandIndex the index of the command in the batch
   * @param commandStartTime the start time of the command execution
   * @param success {@code true} if the command executed successfully; {@code false} if it failed
   */
  void logCommandExecutionTime(int commandIndex, Instant commandStartTime, boolean success) {
    Instant commandEndTime = Instant.now();
    Duration commandDuration = Duration.between(commandStartTime, commandEndTime);
    String status = success ? "executed" : "failed after";
    LOGGER.debug(
        String.format("Command %d %s %d ms", commandIndex, status, commandDuration.toMillis()));
  }

  /**
   * Handles a failure during batch execution by logging the failure details, clearing the remaining
   * commands, and throwing a {@link DatabricksBatchUpdateException}.
   *
   * @param updateCounts an array containing the update counts for the commands executed so far
   * @param commandIndex the index of the command that caused the failure
   * @param batchStartTime the start time of the batch execution
   * @param message a descriptive message explaining the failure
   * @param cause the {@link SQLException} that caused the failure, or {@code null} if not
   *     applicable
   * @throws DatabricksBatchUpdateException always thrown to indicate batch execution failure
   */
  void handleBatchFailure(
      int[] updateCounts,
      int commandIndex,
      Instant batchStartTime,
      String message,
      SQLException cause)
      throws DatabricksBatchUpdateException {
    int[] countsSoFar = Arrays.copyOf(updateCounts, commandIndex);
    clearCommands();

    Duration batchDuration = Duration.between(batchStartTime, Instant.now());
    LOGGER.debug(
        String.format("Total batch execution time until failure: %d ms", batchDuration.toMillis()));

    DatabricksBatchUpdateException exception;
    if (cause != null) {
      exception =
          new DatabricksBatchUpdateException(
              message, cause.getSQLState(), cause.getErrorCode(), countsSoFar, cause);
    } else {
      exception =
          new DatabricksBatchUpdateException(
              message, DatabricksDriverErrorCode.BATCH_EXECUTE_EXCEPTION, countsSoFar);
    }

    throw exception;
  }
}
