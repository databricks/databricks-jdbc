package com.databricks.jdbc.core;

import static java.sql.ParameterMetaData.parameterNullable;

import com.databricks.jdbc.client.impl.thrift.generated.TPrimitiveTypeEntry;
import com.databricks.jdbc.client.impl.thrift.generated.TTypeDesc;
import com.databricks.jdbc.client.impl.thrift.generated.TTypeEntry;
import com.databricks.jdbc.client.impl.thrift.generated.TTypeId;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Databricks types as supported in
 * https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
 */
public class DatabricksTypeUtil {

  public static final String BIGINT = "BIGINT";
  public static final String BINARY = "BINARY";
  public static final String BOOLEAN = "BOOLEAN";
  public static final String DATE = "DATE";
  public static final String DECIMAL = "DECIMAL";
  public static final String DOUBLE = "DOUBLE";
  public static final String FLOAT = "FLOAT";
  public static final String INT = "INT";
  public static final String BYTE = "BYTE";
  public static final String VOID = "VOID";
  public static final String SMALLINT = "SHORT";
  public static final String NULL = "NULL";
  public static final String STRING = "STRING";
  public static final String TINYINT = "TINYINT";
  public static final String TIMESTAMP = "TIMESTAMP";
  public static final String TIMESTAMP_NTZ = "TIMESTAMP_NTZ";
  public static final String MAP = "MAP";
  public static final String ARRAY = "ARRAY";
  public static final String STRUCT = "STRUCT";

  private static final ArrayList<ColumnInfoTypeName> SIGNED_TYPES =
      new ArrayList<>(
          Arrays.asList(
              ColumnInfoTypeName.DECIMAL,
              ColumnInfoTypeName.DOUBLE,
              ColumnInfoTypeName.FLOAT,
              ColumnInfoTypeName.INT,
              ColumnInfoTypeName.LONG,
              ColumnInfoTypeName.SHORT));
  private static final ArrayList<ColumnInfoTypeName> CASE_SENSITIVE_TYPES =
      new ArrayList<>(Arrays.asList(ColumnInfoTypeName.CHAR, ColumnInfoTypeName.STRING));

  public static ColumnInfoTypeName getColumnInfoType(String typeName) {
    switch (typeName) {
      case DatabricksTypeUtil.STRING:
        return ColumnInfoTypeName.STRING;
      case DatabricksTypeUtil.DATE:
      case DatabricksTypeUtil.TIMESTAMP:
      case DatabricksTypeUtil.TIMESTAMP_NTZ:
        return ColumnInfoTypeName.TIMESTAMP;
      case DatabricksTypeUtil.SMALLINT:
      case DatabricksTypeUtil.TINYINT:
        return ColumnInfoTypeName.SHORT;
      case DatabricksTypeUtil.BYTE:
        return ColumnInfoTypeName.BYTE;
      case DatabricksTypeUtil.INT:
        return ColumnInfoTypeName.INT;
      case DatabricksTypeUtil.BIGINT:
        return ColumnInfoTypeName.LONG;
      case DatabricksTypeUtil.FLOAT:
        return ColumnInfoTypeName.FLOAT;
      case DatabricksTypeUtil.DOUBLE:
        return ColumnInfoTypeName.DOUBLE;
      case DatabricksTypeUtil.BINARY:
        return ColumnInfoTypeName.BINARY;
      case DatabricksTypeUtil.BOOLEAN:
        return ColumnInfoTypeName.BOOLEAN;
      case DatabricksTypeUtil.DECIMAL:
        return ColumnInfoTypeName.DECIMAL;
      case DatabricksTypeUtil.STRUCT:
        return ColumnInfoTypeName.STRUCT;
      case DatabricksTypeUtil.ARRAY:
        return ColumnInfoTypeName.ARRAY;
      case DatabricksTypeUtil.VOID:
      case DatabricksTypeUtil.NULL:
        return ColumnInfoTypeName.NULL;
      case DatabricksTypeUtil.MAP:
        return ColumnInfoTypeName.MAP;
    }
    return ColumnInfoTypeName.USER_DEFINED_TYPE;
  }

  public static int getColumnType(ColumnInfoTypeName typeName) {
    if (typeName == null) {
      return Types.OTHER;
    }
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
        // TODO: Handle complex data types
      case MAP:
      case INTERVAL:
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
      case USER_DEFINED_TYPE:
        return Types.OTHER;
      default:
        LoggingUtil.log(LogLevel.ERROR, "Unknown column type: " + typeName);
        throw new IllegalStateException("Unknown column type: " + typeName);
    }
  }

  public static String getColumnTypeClassName(ColumnInfoTypeName typeName) {
    if (typeName == null) {
      return "null";
    }
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
        // TODO: Handle complex data types
      case INTERVAL:
      case USER_DEFINED_TYPE:
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
      case MAP:
        return "java.util.Map";
      default:
        LoggingUtil.log(LogLevel.ERROR, "Unknown column type class name: " + typeName);
        throw new IllegalStateException("Unknown column type class name: " + typeName);
    }
  }

  public static int getDisplaySize(ColumnInfoTypeName typeName, int precision) {
    if (typeName == null) {
      return 255;
    }
    switch (typeName) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case BINARY:
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
      case ARRAY:
      case STRING:
      case STRUCT:
      default:
        return 255;
    }
  }

  public static int getPrecision(ColumnInfoTypeName typeName) {
    if (typeName == null) {
      return 0;
    }
    switch (typeName) {
      case BYTE:
      case SHORT:
        return 5;
      case INT:
      case DATE:
      case DECIMAL:
        return 10;
      case LONG:
        return 19;
      case CHAR:
      case BOOLEAN:
      case BINARY:
        return 1;
      case FLOAT:
        return 7;
      case DOUBLE:
        return 15;
      case TIMESTAMP:
        return 29;
      case ARRAY:
      case STRING:
      case STRUCT:
      default:
        return 255;
    }
  }

  public static int isNullable(ColumnInfoTypeName typeName) {
    // Todo : we need to figure out schema fetch [PECO-1711]
    return parameterNullable;
  }

  public static int getScale(ColumnInfoTypeName typeName) {
    if (typeName == null) {
      return 0;
    }
    return typeName == ColumnInfoTypeName.TIMESTAMP ? 9 : 0;
  }

  public static boolean isSigned(ColumnInfoTypeName typeName) {
    return SIGNED_TYPES.contains(typeName);
  }

  /**
   * Converts SQL type into Databricks type as defined in
   * https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
   *
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
        return NULL;
    }
  }

  /**
   * Infers Databricks type from class of given object as defined in
   * https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
   *
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

  public static TTypeId getThriftTypeFromTypeDesc(TTypeDesc typeDesc) {
    return Optional.ofNullable(typeDesc)
        .map(TTypeDesc::getTypes)
        .map(t -> t.get(0))
        .map(TTypeEntry::getPrimitiveEntry)
        .map(TPrimitiveTypeEntry::getType)
        .orElse(TTypeId.STRING_TYPE);
  }

  public static ArrowType mapThriftToArrowType(TTypeId typeId) throws DatabricksSQLException {
    switch (typeId) {
      case BOOLEAN_TYPE:
        return ArrowType.Bool.INSTANCE;
      case TINYINT_TYPE:
        return new ArrowType.Int(8, true);
      case SMALLINT_TYPE:
        return new ArrowType.Int(16, true);
      case INT_TYPE:
        return new ArrowType.Int(32, true);
      case BIGINT_TYPE:
        return new ArrowType.Int(64, true);
      case FLOAT_TYPE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE_TYPE:
        return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case INTERVAL_DAY_TIME_TYPE:
      case INTERVAL_YEAR_MONTH_TYPE:
      case STRING_TYPE:
      case ARRAY_TYPE:
      case MAP_TYPE:
      case STRUCT_TYPE:
      case USER_DEFINED_TYPE:
      case DECIMAL_TYPE:
      case UNION_TYPE:
      case VARCHAR_TYPE:
      case CHAR_TYPE:
        return ArrowType.Utf8.INSTANCE;
      case TIMESTAMP_TYPE:
        return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
      case BINARY_TYPE:
        return ArrowType.Binary.INSTANCE;
      case DATE_TYPE:
        return new ArrowType.Date(DateUnit.DAY);
      case NULL_TYPE:
        return ArrowType.Null.INSTANCE;
      default:
        throw new DatabricksSQLFeatureNotSupportedException("Unsupported Hive type: " + typeId);
    }
  }
}
