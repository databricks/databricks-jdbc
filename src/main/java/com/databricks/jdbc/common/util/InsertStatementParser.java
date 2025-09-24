package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.INSERT_PATTERN;

import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for parsing INSERT statements to extract table and column information. Supports
 * detecting compatible INSERT statements that can be combined into multi-row batches.
 */
public class InsertStatementParser {

  // Pattern to extract table and columns from INSERT INTO table (col1, col2, ...) VALUES format
  private static final Pattern INSERT_DETAILS_PATTERN =
      Pattern.compile(
          "^\\s*INSERT\\s+INTO\\s+([\\w`\\.]+)\\s*\\(([^)]+)\\)\\s+VALUES\\s*\\(",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** Represents the parsed components of an INSERT statement. */
  public static class InsertInfo {
    private final String tableName;
    private final List<String> columns;
    private final String originalSql;

    public InsertInfo(String tableName, List<String> columns, String originalSql) {
      this.tableName = tableName;
      this.columns = columns;
      this.originalSql = originalSql;
    }

    public String getTableName() {
      return tableName;
    }

    public List<String> getColumns() {
      return columns;
    }

    public String getOriginalSql() {
      return originalSql;
    }

    public int getColumnCount() {
      return columns.size();
    }

    /**
     * Checks if this INSERT is compatible with another INSERT for batching. Two INSERTs are
     * compatible if they target the same table with the same columns in the same order.
     *
     * <p>Compatible INSERT operations can be combined into multi-row INSERT statements for improved
     * performance. For example, these two statements are compatible:
     *
     * <pre>
     *   INSERT INTO users (id, name, email) VALUES (?, ?, ?)
     *   INSERT INTO users (id, name, email) VALUES (?, ?, ?)
     * </pre>
     *
     * These can be batched into:
     *
     * <pre>
     *   INSERT INTO users (id, name, email) VALUES (?, ?, ?), (?, ?, ?)
     * </pre>
     */
    public boolean isCompatibleWith(InsertInfo other) {
      return Objects.equals(this.tableName, other.tableName)
          && Objects.equals(this.columns, other.columns);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InsertInfo that = (InsertInfo) o;
      return Objects.equals(tableName, that.tableName) && Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableName, columns);
    }
  }

  /**
   * Parses an INSERT statement to extract table and column information.
   *
   * @param sql the INSERT SQL statement to parse
   * @return InsertInfo object containing parsed information, or null if not a valid INSERT
   */
  public static InsertInfo parseInsert(String sql) {
    try {
      return parseInsertStrict(sql);
    } catch (DatabricksParsingException e) {
      return null;
    }
  }

  /**
   * Parses an INSERT statement to extract table and column information with strict error handling.
   *
   * @param sql the INSERT SQL statement to parse
   * @return InsertInfo object containing parsed information
   * @throws DatabricksParsingException if the SQL is not a properly formatted INSERT statement
   */
  public static InsertInfo parseInsertStrict(String sql) throws DatabricksParsingException {
    if (sql == null || sql.trim().isEmpty()) {
      throw new DatabricksParsingException(
          "SQL statement cannot be null or empty",
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }

    String trimmedSql = sql.trim();

    // First check if it's an INSERT query using the shared pattern
    if (!INSERT_PATTERN.matcher(trimmedSql).find()) {
      throw new DatabricksParsingException(
          "SQL statement is not an INSERT operation: " + trimmedSql,
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }

    // Then extract detailed information using our specific pattern
    Matcher matcher = INSERT_DETAILS_PATTERN.matcher(trimmedSql);

    if (!matcher.find()) {
      throw new DatabricksParsingException(
          "INSERT statement does not match the expected format 'INSERT INTO table (columns) VALUES': "
              + trimmedSql,
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }

    String tableName = matcher.group(1).trim();
    String columnsStr = matcher.group(2).trim();

    // Parse column names, handling quoted identifiers and whitespace
    List<String> columns = parseColumns(columnsStr);

    if (columns.isEmpty()) {
      throw new DatabricksParsingException(
          "INSERT statement does not contain any valid column names: " + trimmedSql,
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }

    return new InsertInfo(tableName, columns, trimmedSql);
  }

  /** Parses a comma-separated list of column names, handling quoted identifiers. */
  private static List<String> parseColumns(String columnsStr) {
    return Arrays.stream(columnsStr.split(","))
        .map(String::trim)
        .map(col -> col.replaceAll("^`|`$", "")) // Remove backticks if present
        .filter(col -> !col.isEmpty())
        .collect(Collectors.toList());
  }

  /**
   * Checks if the given SQL statement is a parametrized INSERT statement suitable for batching.
   *
   * @param sql the SQL statement to check
   * @return true if it's a parametrized INSERT that can be batched, false otherwise
   */
  public static boolean isParametrizedInsert(String sql) {
    // Use the shared INSERT pattern for efficient detection
    if (sql == null || !INSERT_PATTERN.matcher(sql.trim()).find()) {
      return false;
    }
    return sql.contains("?");
  }

  /**
   * Generates a multi-row INSERT statement from the template and number of rows.
   *
   * @param insertInfo the parsed INSERT information
   * @param numberOfRows the number of rows to include in the batch
   * @return the multi-row INSERT SQL statement
   * @throws DatabricksParsingException if insertInfo is null or numberOfRows is invalid
   */
  public static String generateMultiRowInsert(InsertInfo insertInfo, int numberOfRows)
      throws DatabricksParsingException {
    if (insertInfo == null) {
      throw new DatabricksParsingException(
          "InsertInfo cannot be null", DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
    if (numberOfRows <= 0) {
      throw new DatabricksParsingException(
          "Number of rows must be positive, got: " + numberOfRows,
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }

    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ")
        .append(insertInfo.getTableName())
        .append(" (")
        .append(String.join(", ", insertInfo.getColumns()))
        .append(") VALUES ");

    // Generate placeholders for each row
    String valueClause = "(" + "?, ".repeat(insertInfo.getColumns().size() - 1) + "?)";

    for (int i = 0; i < numberOfRows; i++) {
      if (i > 0) {
        sql.append(", ");
      }
      sql.append(valueClause);
    }

    return sql.toString();
  }
}
