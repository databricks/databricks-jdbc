package com.databricks.jdbc.core;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Databricks types as supported in https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
 */
public class DatabricksTypes {

  public static final String BIGINT = "BIGINT";
  public static final String BINARY = "BINARY";
  public static final String BOOLEAN = "BOOLEAN";
  public static final String DATE = "DATE";
  public static final String DECIMAL = "DECIMAL";
  public static final String DOUBLE = "DOUBLE";
  public static final String FLOAT = "FLOAT";
  public static final String INT = "INT";
  public static final String INTERVAL = "INTERVAL";
  public static final String VOID = "VOID";
  public static final String SMALLINT = "SMALLINT";
  public static final String STRING = "STRING";
  public static final String TINYINT = "TINYINT";
  public static final String TIMESTAMP = "TIMESTAMP";
  public static final String TIMESTAMP_NTZ = "TIMESTAMP_NTZ";
  public static final String MAP = "MAP";
  public static final String ARRAY = "ARRAY";
  public static final String STRUCT = "STRUCT";

  /**
   * Converts SQL type into Databricks type as defined in
   * https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
   * @param sqlType SQL type input
   * @return databricks type
   */
  public static String getDatabricksTypeFromSQLType(int sqlType) {
    switch (sqlType) {
      case Types.ARRAY:
        return ARRAY;
      case Types.BIGINT:
        return BIGINT;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return BINARY;
      case Types.DATE:
        return DATE;
      case Types.DECIMAL:
        return DECIMAL;
      case Types.BOOLEAN:
        return BOOLEAN;
      case Types.DOUBLE:
        return DOUBLE;
      case Types.FLOAT:
        return FLOAT;
      case Types.INTEGER:
        return INT;
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
        return STRING;
      case Types.TIMESTAMP:
        return TIMESTAMP_NTZ;
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return TIMESTAMP;
      case Types.STRUCT:
        return STRUCT;
      case Types.TINYINT:
        return TINYINT;
      case Types.SMALLINT:
        return SMALLINT;
      default:
        // TODO: handle more types
        return null;
    }
  }

  /**
   * Infers Databricks type from class of given object as defined in
   * https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
   * @param obj input object
   * @return inferred Databricks type
   */
  public static String inferDatabricksType(Object obj) {
    String type = null;
    if (obj == null) {
      type = VOID;
    } else if (obj instanceof Long) {
      type = BIGINT;
    } else if (obj instanceof Short) {
      type = SMALLINT;
    } else if (obj instanceof Byte) {
      type = TINYINT;
    } else if (obj instanceof Float) {
      type = FLOAT;
    } else if (obj instanceof String) {
      type = STRING;
    } else if (obj instanceof Integer) {
      type = INT;
    } else if (obj instanceof Timestamp) {
      type = TIMESTAMP;
    } else if (obj instanceof Date) {
      type = DATE;
    } else if (obj instanceof Double) {
      type = DOUBLE;
    }
    // TODO: handle more types
    return type;
  }
}
