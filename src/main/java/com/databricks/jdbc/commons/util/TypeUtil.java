package com.databricks.jdbc.commons.util;

import static com.databricks.sdk.service.sql.ColumnInfoTypeName.*;

import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class consists of utility functions with respect to wildcard strings that are required in
 * building SQL queries
 */
public class TypeUtil {
  private static ArrayList<ColumnInfoTypeName> SIGNED_TYPES =
      new ArrayList<>(Arrays.asList(DECIMAL, DOUBLE, FLOAT, INT, LONG, SHORT));
  private static ArrayList<ColumnInfoTypeName> CASE_SENSITIVE_TYPES =
      new ArrayList<>(Arrays.asList(CHAR, STRING));

  public static int getColumnType(ColumnInfoTypeName typeName) {
    switch (typeName) {
      case BYTE:
        return Types.TINYINT;
      case SHORT:
        return Types.SMALLINT;
      case INT:
        return Types.INTEGER;
      case LONG:
        return Types.BIGINT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      case DECIMAL:
        return Types.DECIMAL;
      case BINARY:
        return Types.BINARY;
      case BOOLEAN:
        return Types.BOOLEAN;
      case CHAR:
        return Types.CHAR;
      case STRING:
        return Types.VARCHAR;
      case TIMESTAMP:
        return Types.TIMESTAMP;
      case DATE:
        return Types.DATE;
      case STRUCT:
        return Types.STRUCT;
      case ARRAY:
        return Types.ARRAY;
      case NULL:
        return Types.NULL;
      default:
        throw new IllegalStateException("Unknown column type: " + typeName);
    }
  }

  public static String getColumnTypeClassName(ColumnInfoTypeName typeName) {
    switch (typeName) {
      case BYTE:
      case SHORT:
      case INT:
        return "java.lang.Integer";
      case LONG:
        return "java.lang.Long";
      case FLOAT:
      case DOUBLE:
        return "java.lang.Double";
      case DECIMAL:
        return "java.math.BigDecimal";
      case BINARY:
        return "[B";
      case BOOLEAN:
        return "java.lang.Boolean";
      case CHAR:
      case STRING:
        return "java.lang.String";
      case TIMESTAMP:
        return "java.sql.Timestamp";
      case DATE:
        return "java.sql.Date";
      case STRUCT:
        return "java.sql.Struct";
      case ARRAY:
        return "java.sql.Array";
      case NULL:
        return "null";
      default:
        throw new IllegalStateException("Unknown column type: " + typeName);
    }
  }

  public static int getDisplaySize(ColumnInfoTypeName typeName, int precision) {
    switch (typeName) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return precision + 1; // including negative sign
      case CHAR:
        return precision;
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return 24;
      case BOOLEAN:
        return 5; // length of `false`
      case TIMESTAMP:
        return 29; // as per
        // https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/java/sql/Timestamp.html#toString()
      case DATE:
        return 10;
      case NULL:
        return 4; // Length of `NULL`
      case BINARY: // hive has no max limit for binary
      case ARRAY:
      case STRUCT:
      default:
        return Integer.MAX_VALUE;
    }
  }

  public static boolean isSigned(ColumnInfoTypeName typeName) {
    return SIGNED_TYPES.contains(typeName);
  }

  public static boolean isCaseSensitive(ColumnInfoTypeName typeName) {
    return CASE_SENSITIVE_TYPES.contains(typeName);
  }
}
