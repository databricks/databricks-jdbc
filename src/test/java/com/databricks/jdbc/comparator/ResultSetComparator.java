package com.databricks.jdbc.comparator;

import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.ErrorComparator;
import com.databricks.jdbc.comparator.error.ErrorComparison;
import com.databricks.jdbc.comparator.error.ErrorPolicy;
import java.io.IOException;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Note: If diff string formats are changed here, update {@link ComparisonResult#csvSummary()} which
 * parses these strings to generate concise CSV summaries.
 */
public class ResultSetComparator {

  /**
   * JDBC-spec-mandated row ordering per DatabaseMetaData method. When the comparator runs one of
   * these methods, both result sets are sorted by these columns (in order) before the row-by-row
   * diff. This eliminates spurious cascading diffs when both backends return identical rows in
   * different orders.
   *
   * <p>Sort columns are matched by name (case-insensitive) against the ResultSetMetaData of each
   * side; if any sort column is missing, sorting is skipped gracefully.
   */
  private static final Map<String, List<String>> SORT_KEYS_BY_METHOD =
      Map.ofEntries(
          Map.entry("getCatalogs", List.of("TABLE_CAT")),
          Map.entry("getSchemas", List.of("TABLE_CATALOG", "TABLE_SCHEM")),
          Map.entry("getTables", List.of("TABLE_TYPE", "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME")),
          Map.entry(
              "getColumns", List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "ORDINAL_POSITION")),
          Map.entry("getPrimaryKeys", List.of("COLUMN_NAME")),
          Map.entry(
              "getImportedKeys",
              List.of("PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "KEY_SEQ")),
          Map.entry(
              "getExportedKeys",
              List.of("FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "KEY_SEQ")),
          Map.entry(
              "getCrossReference",
              List.of("FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "KEY_SEQ")),
          Map.entry(
              "getFunctions",
              List.of("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "SPECIFIC_NAME")),
          Map.entry(
              "getFunctionColumns",
              List.of("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "SPECIFIC_NAME")),
          Map.entry(
              "getProcedures",
              List.of("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME")),
          Map.entry(
              "getProcedureColumns",
              List.of("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "SPECIFIC_NAME")),
          Map.entry("getColumnPrivileges", List.of("COLUMN_NAME", "PRIVILEGE")),
          Map.entry(
              "getTablePrivileges", List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "PRIVILEGE")),
          Map.entry("getBestRowIdentifier", List.of("SCOPE")),
          Map.entry(
              "getIndexInfo", List.of("NON_UNIQUE", "TYPE", "INDEX_NAME", "ORDINAL_POSITION")),
          Map.entry("getUDTs", List.of("DATA_TYPE", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME")),
          Map.entry(
              "getAttributes", List.of("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "ORDINAL_POSITION")),
          Map.entry(
              "getPseudoColumns",
              List.of("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME")));

  public static ComparisonResult compare(
      String queryType, String queryOrMethod, Object[] methodArgs, Object result1, Object result2)
      throws SQLException {
    return compare(queryType, queryOrMethod, methodArgs, result1, result2, Collections.emptySet());
  }

  /**
   * Compares two results with optional schema row filtering. When skipSchemas is non-empty and both
   * results are ResultSets, rows matching TABLE_SCHEM values in skipSchemas are excluded before
   * comparison.
   */
  public static ComparisonResult compare(
      String queryType,
      String queryOrMethod,
      Object[] methodArgs,
      Object result1,
      Object result2,
      Set<String> skipSchemas)
      throws SQLException {
    ComparisonResult result = new ComparisonResult(queryType, queryOrMethod, methodArgs);
    result.metadataDifferences = new ArrayList<>();
    result.dataDifferences = new ArrayList<>();

    if (result1 == null && result2 == null) {
      return result;
    }

    if (result1 instanceof ResultSet && result2 instanceof ResultSet) {
      ResultSet rs1 = (ResultSet) result1;
      ResultSet rs2 = (ResultSet) result2;
      ResultSetMetaData md1 = rs1.getMetaData();
      ResultSetMetaData md2 = rs2.getMetaData();

      // Compare metadata (shared for both paths)
      result.metadataDifferences = compareMetadata(md1, md2);

      // Compare data — drain+filter+sort path when skipSchemas is set OR the method has a
      // JDBC-spec sort order, streaming otherwise.
      List<String> sortColumns = SORT_KEYS_BY_METHOD.get(queryOrMethod);
      if (!skipSchemas.isEmpty() || sortColumns != null) {
        List<Object[]> rows1 = drainResultSet(rs1, md1.getColumnCount());
        List<Object[]> rows2 = drainResultSet(rs2, md2.getColumnCount());
        filterBySchema(rows1, md1, skipSchemas);
        filterBySchema(rows2, md2, skipSchemas);
        if (sortColumns != null) {
          sortByColumns(rows1, md1, sortColumns);
          sortByColumns(rows2, md2, sortColumns);
        }
        result.dataDifferences = compareRowData(rows1, rows2, md1, md2);
      } else {
        result.dataDifferences = compareData(rs1, rs2);
      }
    } else if (compareErrorsDeeply(
        queryType, queryOrMethod, methodArgs, result1, result2, result)) {
      // Deep error comparison handled it (at least one side is a Throwable and the
      // ERROR_COMPARISON_MODE gate is on). Diffs, if any, were folded into `result`.
    } else if (!(result1 instanceof ResultSet) && !(result2 instanceof ResultSet)) {
      // Both are not of type ResultSet
      if (result1 == null || !resultIsSame(result1, result2)) {
        if (result1 instanceof Throwable && result2 instanceof Throwable) {
          // When both are exceptions, first must be a super of the second
          // This is ok as new driver is throwing a subclass exception of the existing driver's
          // exception
          if (!result1.getClass().isAssignableFrom(result2.getClass())) {
            result.dataDifferences.add(result1 + " vs " + result2);
          }
        } else {
          result.dataDifferences.add(result1 + " vs " + result2);
        }
      }
    } else {
      // when we see different classes of results, it would generally mean that one result is an
      // exception and the other is an actual result set.
      String r1Label = describeResult(result1);
      String r2Label = describeResult(result2);
      result.metadataDifferences.add(r1Label + " vs " + r2Label);
    }

    return result;
  }

  /**
   * When {@code ERROR_COMPARISON_MODE} is on and at least one side is a {@link Throwable}, compares
   * the errors deeply (class + SQLState + code + serverCode + message) and folds any diffs into
   * {@code result}. Returns true when it handled the comparison; false (a no-op) when the gate is
   * off or neither side threw, so the legacy branches below take over unchanged.
   */
  private static boolean compareErrorsDeeply(
      String queryType,
      String queryOrMethod,
      Object[] methodArgs,
      Object result1,
      Object result2,
      ComparisonResult result) {
    ErrorPolicy policy = ErrorPolicy.active();
    if (!policy.isDeepComparisonEnabled()) {
      return false;
    }
    if (!(result1 instanceof Throwable) && !(result2 instanceof Throwable)) {
      return false;
    }
    CapturedOutcome left = toOutcome(result1);
    CapturedOutcome right = toOutcome(result2);
    ErrorComparison comparison =
        ErrorComparator.compare(left, right, ResultSetComparator::describeResult);
    result.metadataDifferences.addAll(comparison.metadataDiffs);
    result.dataDifferences.addAll(comparison.dataDiffs);
    return true;
  }

  private static CapturedOutcome toOutcome(Object result) {
    if (result instanceof Throwable) {
      return CapturedOutcome.threw((Throwable) result);
    }
    return CapturedOutcome.returned(result);
  }

  private static String describeResult(Object result) {
    if (result == null) {
      return "null";
    }
    if (result instanceof ResultSet) {
      try {
        ResultSet rs = (ResultSet) result;
        int rowCount = 0;
        while (rs.next()) rowCount++;
        return result.getClass() + (rowCount == 0 ? " (empty)" : " (" + rowCount + " rows)");
      } catch (SQLException e) {
        return result.getClass().toString();
      }
    }
    return result.getClass().toString();
  }

  private static boolean resultIsSame(Object result1, Object result2) {
    if (result1 == null || result2 == null) {
      return result1 == result2;
    }
    if (result1.equals(result2)) {
      return true;
    }
    if (result1.getClass().isArray() && result2.getClass().isArray()) {
      if (result1 instanceof byte[]) {
        return Arrays.equals((byte[]) result1, (byte[]) result2);
      }
      if (result1 instanceof Object[]) {
        return Arrays.equals((Object[]) result1, (Object[]) result2);
      }
    }
    if (result1 instanceof Reader && result2 instanceof Reader) {
      return readersHaveSameContent((Reader) result1, (Reader) result2);
    }
    return false;
  }

  private static boolean readersHaveSameContent(Reader r1, Reader r2) {
    try {
      int c1, c2;
      while ((c1 = r1.read()) != -1) {
        c2 = r2.read();
        if (c1 != c2) return false;
      }
      return r2.read() == -1; // both should be exhausted
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Compares two ResultSetMetaData objects and returns a ComparisonResult with metadata-only diffs.
   */
  public static ComparisonResult compareMetadata(
      String queryType,
      String queryOrMethod,
      Object[] methodArgs,
      ResultSetMetaData md1,
      ResultSetMetaData md2)
      throws SQLException {
    ComparisonResult result = new ComparisonResult(queryType, queryOrMethod, methodArgs);
    result.metadataDifferences = compareMetadata(md1, md2);
    result.dataDifferences = new ArrayList<>();
    return result;
  }

  private static List<String> compareMetadata(ResultSetMetaData md1, ResultSetMetaData md2)
      throws SQLException {
    List<String> differences = new ArrayList<>();

    int columnCount1 = md1.getColumnCount();
    int columnCount2 = md2.getColumnCount();

    if (columnCount1 != columnCount2) {
      differences.add("Column count mismatch: " + columnCount1 + " vs " + columnCount2);
      String extraCols1 = getExtraColumns(md1, md2);
      String extraCols2 = getExtraColumns(md2, md1);
      if (!extraCols1.isEmpty())
        differences.add("Extra columns in first ResultSet: " + getExtraColumns(md1, md2));
      if (!extraCols2.isEmpty())
        differences.add("Extra columns in second ResultSet: " + getExtraColumns(md2, md1));
    }

    int columnCount = Math.min(columnCount1, columnCount2);
    for (int i = 1; i <= columnCount; i++) {
      compareColumnMetadata(md1, md2, i, differences);
    }

    return differences;
  }

  private static void compareColumnMetadata(
      ResultSetMetaData md1, ResultSetMetaData md2, int columnIndex, List<String> differences)
      throws SQLException {
    String columnName1 = md1.getColumnName(columnIndex);
    String columnName2 = md2.getColumnName(columnIndex);

    if (!columnName1.equals(columnName2)) {
      differences.add(
          "Column " + columnIndex + " name mismatch: " + columnName1 + " vs " + columnName2);
    }

    compareMetadataProperty(
        columnName1,
        "ColumnType",
        md1.getColumnType(columnIndex),
        md2.getColumnType(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "ColumnTypeName",
        md1.getColumnTypeName(columnIndex),
        md2.getColumnTypeName(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "ColumnClassName",
        md1.getColumnClassName(columnIndex),
        md2.getColumnClassName(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "ColumnLabel",
        md1.getColumnLabel(columnIndex),
        md2.getColumnLabel(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "SchemaName",
        md1.getSchemaName(columnIndex),
        md2.getSchemaName(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "CatalogName",
        md1.getCatalogName(columnIndex),
        md2.getCatalogName(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "TableName",
        md1.getTableName(columnIndex),
        md2.getTableName(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "Precision",
        md1.getPrecision(columnIndex),
        md2.getPrecision(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1, "Scale", md1.getScale(columnIndex), md2.getScale(columnIndex), differences);
    compareMetadataProperty(
        columnName1,
        "ColumnDisplaySize",
        md1.getColumnDisplaySize(columnIndex),
        md2.getColumnDisplaySize(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "IsAutoIncrement",
        md1.isAutoIncrement(columnIndex),
        md2.isAutoIncrement(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "IsCaseSensitive",
        md1.isCaseSensitive(columnIndex),
        md2.isCaseSensitive(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "IsSearchable",
        md1.isSearchable(columnIndex),
        md2.isSearchable(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "IsCurrency",
        md1.isCurrency(columnIndex),
        md2.isCurrency(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "IsNullable",
        md1.isNullable(columnIndex),
        md2.isNullable(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1, "IsSigned", md1.isSigned(columnIndex), md2.isSigned(columnIndex), differences);
    compareMetadataProperty(
        columnName1,
        "IsReadOnly",
        md1.isReadOnly(columnIndex),
        md2.isReadOnly(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "IsWritable",
        md1.isWritable(columnIndex),
        md2.isWritable(columnIndex),
        differences);
    compareMetadataProperty(
        columnName1,
        "IsDefinitelyWritable",
        md1.isDefinitelyWritable(columnIndex),
        md2.isDefinitelyWritable(columnIndex),
        differences);
  }

  private static void compareMetadataProperty(
      String columnName,
      String propertyName,
      Object value1,
      Object value2,
      List<String> differences) {
    if (!objectsEqual(value1, value2)) {
      differences.add(
          "Column name "
              + columnName
              + " "
              + propertyName
              + " mismatch: "
              + value1
              + " vs "
              + value2);
    }
  }

  private static String getExtraColumns(ResultSetMetaData md1, ResultSetMetaData md2)
      throws SQLException {
    StringBuilder extra = new StringBuilder();
    for (int i = 1; i <= md1.getColumnCount(); i++) {
      boolean found = false;
      for (int j = 1; j <= md2.getColumnCount(); j++) {
        if (md1.getColumnName(i).equals(md2.getColumnName(j))) {
          found = true;
          break;
        }
      }
      if (!found) {
        if (extra.length() > 0) extra.append(", ");
        extra.append(md1.getColumnName(i));
      }
    }
    return extra.toString();
  }

  // ---------------------------------------------------------------------------
  // Drain + filter helpers (used when skipSchemas is non-empty)
  // ---------------------------------------------------------------------------

  private static List<Object[]> drainResultSet(ResultSet rs, int columnCount) throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    while (rs.next()) {
      Object[] row = new Object[columnCount];
      for (int i = 0; i < columnCount; i++) {
        row[i] = rs.getObject(i + 1);
      }
      rows.add(row);
    }
    return rows;
  }

  private static void filterBySchema(
      List<Object[]> rows, ResultSetMetaData md, Set<String> skipSchemas) throws SQLException {
    int schemaCol = findColumnIndex(md, "TABLE_SCHEM");
    if (schemaCol < 0) return;
    rows.removeIf(row -> row[schemaCol] != null && skipSchemas.contains(row[schemaCol].toString()));
  }

  /**
   * Sorts rows in place by the given list of column names (in order). Skips sorting gracefully if
   * any column is missing from the ResultSetMetaData. Null cell values sort first.
   */
  private static void sortByColumns(
      List<Object[]> rows, ResultSetMetaData md, List<String> columnNames) throws SQLException {
    int[] indices = new int[columnNames.size()];
    for (int i = 0; i < columnNames.size(); i++) {
      indices[i] = findColumnIndex(md, columnNames.get(i));
      if (indices[i] < 0) return;
    }
    rows.sort(
        (a, b) -> {
          for (int idx : indices) {
            int c = compareNullable(a[idx], b[idx]);
            if (c != 0) return c;
          }
          return 0;
        });
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static int compareNullable(Object a, Object b) {
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;
    if (a instanceof Comparable && a.getClass() == b.getClass()) {
      return ((Comparable) a).compareTo(b);
    }
    return a.toString().compareTo(b.toString());
  }

  private static int findColumnIndex(ResultSetMetaData md, String columnName) throws SQLException {
    for (int i = 1; i <= md.getColumnCount(); i++) {
      if (columnName.equalsIgnoreCase(md.getColumnName(i))) {
        return i - 1; // 0-based for array access
      }
    }
    return -1;
  }

  // ---------------------------------------------------------------------------
  // Shared row comparison helpers (used by both streaming and list-based paths)
  // ---------------------------------------------------------------------------

  /** Returns a formatted cell mismatch string, or null if values are equal. */
  private static String cellMismatch(int rowNum, String columnName, Object value1, Object value2) {
    if (objectsEqual(value1, value2)) return null;
    String type1 = value1 != null ? value1.getClass().getSimpleName() : "null";
    String type2 = value2 != null ? value2.getClass().getSimpleName() : "null";
    return "Row "
        + rowNum
        + ", Column "
        + columnName
        + " mismatch: "
        + value1
        + " ("
        + type1
        + ") vs "
        + value2
        + " ("
        + type2
        + ")";
  }

  /** Formats an extra row entry from column names and values (0-based array). */
  private static String formatExtraRow(int rowNum, ResultSetMetaData md, Object[] row)
      throws SQLException {
    StringBuilder rowData = new StringBuilder();
    for (int i = 0; i < md.getColumnCount(); i++) {
      if (i > 0) rowData.append(", ");
      rowData.append(md.getColumnName(i + 1)).append(": ").append(row[i]);
    }
    return "Extra row " + rowNum + ": " + rowData;
  }

  // ---------------------------------------------------------------------------
  // List-based row comparison (used after drain+filter)
  // ---------------------------------------------------------------------------

  private static List<String> compareRowData(
      List<Object[]> rows1, List<Object[]> rows2, ResultSetMetaData md1, ResultSetMetaData md2)
      throws SQLException {
    List<String> differences = new ArrayList<>();
    int columnCount = Math.min(md1.getColumnCount(), md2.getColumnCount());
    int commonRows = Math.min(rows1.size(), rows2.size());

    for (int row = 0; row < commonRows; row++) {
      Object[] r1 = rows1.get(row);
      Object[] r2 = rows2.get(row);
      for (int i = 0; i < columnCount; i++) {
        String diff = cellMismatch(row + 1, md1.getColumnName(i + 1), r1[i], r2[i]);
        if (diff != null) differences.add(diff);
      }
    }

    if (rows1.size() != rows2.size()) {
      List<Object[]> extra = rows1.size() > rows2.size() ? rows1 : rows2;
      String which = rows1.size() > rows2.size() ? "First" : "Second";
      ResultSetMetaData md = rows1.size() > rows2.size() ? md1 : md2;
      for (int row = commonRows; row < extra.size(); row++) {
        differences.add(formatExtraRow(row + 1, md, extra.get(row)));
      }
      differences.add(which + " ResultSet has " + (extra.size() - commonRows) + " extra rows");
    }

    return differences;
  }

  // ---------------------------------------------------------------------------
  // Streaming row comparison (used when no filtering needed)
  // ---------------------------------------------------------------------------

  private static List<String> compareData(ResultSet rs1, ResultSet rs2) throws SQLException {
    List<String> differences = new ArrayList<>();
    int rowCount = 0;
    ResultSetMetaData md1 = rs1.getMetaData();
    ResultSetMetaData md2 = rs2.getMetaData();
    int columnCount = Math.min(md1.getColumnCount(), md2.getColumnCount());
    boolean rs1HasMore = false;
    boolean rs2HasMore = false;

    while (true) {
      boolean has1 = rs1.next();
      boolean has2 = rs2.next();
      if (!has1 || !has2) {
        rs1HasMore = has1;
        rs2HasMore = has2;
        break;
      }
      rowCount++;
      for (int i = 1; i <= columnCount; i++) {
        String diff =
            cellMismatch(rowCount, md1.getColumnName(i), rs1.getObject(i), rs2.getObject(i));
        if (diff != null) differences.add(diff);
      }
    }

    // Check if one ResultSet has more rows than the other
    if (rs1HasMore || rs2HasMore) {
      if (rs1HasMore) {
        int extraRows = countAndLogExtraRows(rs1, md1, rowCount, differences);
        differences.add("First ResultSet has " + extraRows + " extra rows");
      } else {
        int extraRows = countAndLogExtraRows(rs2, md2, rowCount, differences);
        differences.add("Second ResultSet has " + extraRows + " extra rows");
      }
    }

    return differences;
  }

  private static int countAndLogExtraRows(
      ResultSet rs, ResultSetMetaData md, int startingRowCount, List<String> differences)
      throws SQLException {
    int extraRows = 0;
    do {
      extraRows++;
      Object[] row = new Object[md.getColumnCount()];
      for (int i = 0; i < row.length; i++) {
        row[i] = rs.getObject(i + 1);
      }
      differences.add(formatExtraRow(startingRowCount + extraRows, md, row));
    } while (rs.next());
    return extraRows;
  }

  private static boolean objectsEqual(Object o1, Object o2) {
    if (java.util.Objects.deepEquals(o1, o2)) {
      return true;
    }
    // Fallback: compare toString() for objects that don't implement equals()
    // (e.g., DatabricksArray, DatabricksMap, DatabricksStruct)
    if (o1 != null && o2 != null) {
      return o1.toString().equals(o2.toString());
    }
    return false;
  }
}
