package com.jayant;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetComparator {
  public static ComparisonResult compare(
      String queryType, String queryOrMethod, Object[] methodArgs, Object result1, Object result2)
      throws SQLException {
    ComparisonResult result = new ComparisonResult(queryType, queryOrMethod, methodArgs);
    result.metadataDifferences = new ArrayList<>();
    result.dataDifferences = new ArrayList<>();

    if (result1 instanceof ResultSet && result2 instanceof ResultSet) {
      ResultSet rs1 = (ResultSet) result1;
      ResultSet rs2 = (ResultSet) result2;
      // Compare metadata
      result.metadataDifferences = compareMetadata(rs1.getMetaData(), rs2.getMetaData());

      // Compare data
      result.dataDifferences = compareData(rs1, rs2);
    } else if (!(result1 instanceof ResultSet) && !(result2 instanceof ResultSet)) {
      // Both are not of type ResultSet
      if (result1 == null || !result1.equals(result2)) {
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
      result.metadataDifferences.add(result1.getClass() + " vs " + result2.getClass());
    }

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

  private static List<String> compareData(ResultSet rs1, ResultSet rs2) throws SQLException {
    List<String> differences = new ArrayList<>();
    int rowCount = 0;
    ResultSetMetaData md1 = rs1.getMetaData();
    ResultSetMetaData md2 = rs2.getMetaData();
    int columnCount = Math.min(md1.getColumnCount(), md2.getColumnCount());

    while (rs1.next() && rs2.next()) {
      rowCount++;
      for (int i = 1; i <= columnCount; i++) {
        Object value1 = rs1.getObject(i);
        Object value2 = rs2.getObject(i);

        if (!objectsEqual(value1, value2)) {
          String type1 = value1 != null ? value1.getClass().getSimpleName() : "null";
          String type2 = value2 != null ? value2.getClass().getSimpleName() : "null";

          differences.add(
              "Row "
                  + rowCount
                  + ", Column "
                  + md1.getColumnName(i)
                  + " mismatch: "
                  + value1
                  + " ("
                  + type1
                  + ")"
                  + " vs "
                  + value2
                  + " ("
                  + type2
                  + ")");
        }
      }
    }

    // Check if one ResultSet has more rows than the other
    boolean rs1HasMore = rs1.next();
    boolean rs2HasMore = rs2.next();

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
    StringBuilder rowData = new StringBuilder();
    do {
      extraRows++;
      rowData.setLength(0);
      for (int i = 1; i <= md.getColumnCount(); i++) {
        if (i > 1) rowData.append(", ");
        rowData.append(md.getColumnName(i)).append(": ").append(rs.getObject(i));
      }
      differences.add("Extra row " + (startingRowCount + extraRows) + ": " + rowData);
    } while (rs.next());
    return extraRows;
  }

  private static boolean objectsEqual(Object o1, Object o2) {
    if (o1 == null && o2 == null) {
      return true;
    }
    if (o1 == null || o2 == null) {
      return false;
    }
    return o1.equals(o2);
  }
}
