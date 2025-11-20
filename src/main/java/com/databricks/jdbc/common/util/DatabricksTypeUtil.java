package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.Nullable;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TPrimitiveTypeEntry;
import com.databricks.jdbc.model.client.thrift.generated.TTypeDesc;
import com.databricks.jdbc.model.client.thrift.generated.TTypeEntry;
import com.databricks.jdbc.model.client.thrift.generated.TTypeId;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Utility class for handling various type conversions and mappings between <a
 * href="https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html">Databricks-specific</a>
 * data types, SQL types, and Arrow types.
 */
public class DatabricksTypeUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksTypeUtil.class);
  public static final String BIGINT = "BIGINT";
  public static final String LONG = "LONG";
  public static final String BINARY = "BINARY";
  public static final String BOOLEAN = "BOOLEAN";
  public static final String DATE = "DATE";
  public static final String DECIMAL = "DECIMAL";
  public static final String DOUBLE = "DOUBLE";
  public static final String FLOAT = "FLOAT";
  public static final String INT = "INT";
  public static final String BYTE = "BYTE";
  public static final String VOID = "VOID";
  public static final String SHORT = "SHORT";
  public static final String SMALLINT = "SMALLINT";
  public static final String NULL = "NULL";
  public static final String STRING = "STRING";
  public static final String TINYINT = "TINYINT";
  public static final String TIMESTAMP = "TIMESTAMP";
  public static final String TIME = "TIME";
  public static final String TIMESTAMP_NTZ = "TIMESTAMP_NTZ";
  public static final String MAP = "MAP";
  public static final String ARRAY = "ARRAY";
  public static final String STRUCT = "STRUCT";
  public static final String VARIANT = "VARIANT";
  public static final String CHAR = "CHAR";
  public static final String INTERVAL = "INTERVAL";
  public static final String GEOMETRY = "GEOMETRY";
  public static final String GEOGRAPHY = "GEOGRAPHY";
  public static final String GEOMETRY_CLASS_NAME = "com.databricks.jdbc.api.IGeometry";
  public static final String GEOGRAPHY_CLASS_NAME = "com.databricks.jdbc.api.IGeography";
  public static final String MEASURE = "measure";
  private static final ArrayList<ColumnInfoTypeName> SIGNED_TYPES =
      new ArrayList<>(
          Arrays.asList(
              ColumnInfoTypeName.DECIMAL,
              ColumnInfoTypeName.DOUBLE,
              ColumnInfoTypeName.FLOAT,
              ColumnInfoTypeName.INT,
              ColumnInfoTypeName.LONG,
              ColumnInfoTypeName.SHORT,
              ColumnInfoTypeName.SMALLINT,
              ColumnInfoTypeName.TINYINT,
              ColumnInfoTypeName.BYTE,
              ColumnInfoTypeName.BIGINT));

  // only used for PreparedStatement
  public static ColumnInfoTypeName getColumnInfoType(String typeName) {
    switch (typeName) {
      case DatabricksTypeUtil.CHAR:
      case DatabricksTypeUtil.STRING:
        return ColumnInfoTypeName.STRING; // both char, string passed as STRING param
      case DatabricksTypeUtil.DATE:
      case DatabricksTypeUtil.TIMESTAMP:
      case DatabricksTypeUtil.TIMESTAMP_NTZ:
        return ColumnInfoTypeName.TIMESTAMP;
      case DatabricksTypeUtil.SMALLINT:
      case DatabricksTypeUtil.SHORT:
        return ColumnInfoTypeName.SHORT;
      case DatabricksTypeUtil.TINYINT:
        return ColumnInfoTypeName.TINYINT;
      case DatabricksTypeUtil.BYTE:
        return ColumnInfoTypeName.BYTE;
      case DatabricksTypeUtil.INT:
        return ColumnInfoTypeName.INT;
      case DatabricksTypeUtil.BIGINT:
      case DatabricksTypeUtil.LONG:
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
      case TINYINT:
      case BYTE:
        return Types.TINYINT;
      case SMALLINT:
      case SHORT:
        return Types.SMALLINT;
      case INT:
        return Types.INTEGER;
      case LONG:
      case BIGINT:
        return Types.BIGINT;
      case VOID:
        return Types.OTHER;
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
      case MAP:
      case INTERVAL:
      case NULL:
        return Types.VARCHAR;
      case TIMESTAMP:
        return Types.TIMESTAMP;
      case DATE:
        return Types.DATE;
      case STRUCT:
        return Types.STRUCT;
      case ARRAY:
        return Types.ARRAY;
      case GEOMETRY:
      case GEOGRAPHY:
      case USER_DEFINED_TYPE:
        return Types.OTHER;
      default:
        String errorMsg = "Unknown column type: " + typeName;
        LOGGER.error(errorMsg);
        throw new IllegalStateException(errorMsg);
    }
  }

  public static String getColumnTypeClassName(ColumnInfoTypeName typeName) {
    if (typeName == null) {
      return "null";
    }
    switch (typeName) {
      case BYTE:
      case SHORT:
      case SMALLINT:
        return "java.lang.Short";
      case INT:
        return "java.lang.Integer";
      case TINYINT:
        return "java.lang.Byte";
      case BIGINT:
      case LONG:
        return "java.lang.Long";
      case FLOAT:
        return "java.lang.Float";
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
      case GEOMETRY:
        return GEOMETRY_CLASS_NAME;
      case GEOGRAPHY:
        return GEOGRAPHY_CLASS_NAME;
      case NULL:
      case VOID:
        return "null";
      case MAP:
        return "java.util.Map";
      default:
        String errorMsg = "Unknown column type class name: " + typeName;
        LOGGER.error(errorMsg);
        throw new IllegalStateException(errorMsg);
    }
  }

  /*
   * Returns default precision and scale based on column type. For string columns, returns the default string col length in precision.
   */
  public static int[] getBasePrecisionAndScale(int columnType, IDatabricksConnectionContext ctx) {
    if (columnType == Types.VARCHAR || columnType == Types.CHAR) {
      return new int[] {ctx.getDefaultStringColumnLength(), 0};
    }
    return new int[] {
      DatabricksTypeUtil.getPrecision(columnType), DatabricksTypeUtil.getScale(columnType)
    };
  }

  private static int calculateDisplaySize(int scale, int precision) {
    // scale = precision => only fractional digits. +3 for decimal point, sign and leading zero
    // scale = 0 => only integral part. +1 for sign
    // scale > 0 => both integral and fractional part. +2 for sign and decimal point
    return scale == precision ? precision + 3 : scale == 0 ? precision + 1 : precision + 2;
  }

  public static int getDisplaySize(ColumnInfoTypeName typeName, int precision, int scale) {
    if (typeName == null) {
      return 255;
    }
    switch (typeName) {
      case BYTE:
        return 4;
      case SHORT:
      case INT:
      case LONG:
      case BINARY:
        return precision + 1; // including negative sign
      case CHAR:
      case STRING:
        return precision;
      case FLOAT:
        return 14;
      case DOUBLE:
        return 24;
      case DECIMAL:
        return calculateDisplaySize(scale, precision);
      case BOOLEAN:
        return 1; // 0 or 1
      case TIMESTAMP:
        return 29; // as per
        // https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/java/sql/Timestamp.html#toString()
      case DATE:
        return 10;
      case NULL:
        return 4; // Length of `NULL`
      case ARRAY:
      case STRUCT:
      case GEOMETRY:
      case GEOGRAPHY:
      default:
        return 255;
    }
  }

  /**
   * Returns the display size for a given SQL type and precision. This method is used only in
   * pre-defined result set metadata flow.
   *
   * @param sqlType the SQL type as defined in {@link java.sql.Types}
   * @param precision the precision of the column
   * @return the display size for the given SQL type and precision
   */
  public static int getDisplaySize(int sqlType, int precision) {
    switch (sqlType) {
      case Types.SMALLINT:
      case Types.INTEGER:
        return precision + 1;
      case Types.CHAR:
        return precision;
      case Types.BOOLEAN:
        return 5;
      case Types.BIT:
        return 1;
      case Types.VARCHAR:
        return 128;
      default:
        return 255; // Default size for unhandled types
    }
  }

  public static int getMetadataColPrecision(Integer columnType) {
    switch (columnType) {
      case Types.SMALLINT:
        return 5;
      case Types.INTEGER:
        return 10;
      case Types.CHAR:
      case Types.BOOLEAN:
      case Types.BIT:
        return 1;
      case Types.VARCHAR:
        return 128;
      default:
        return 255;
    }
  }

  public static int getPrecision(Integer columnType) {
    if (columnType == null) {
      return 0;
    }
    switch (columnType) {
      case Types.TINYINT:
        return 3;
      case Types.SMALLINT:
        return 5;
      case Types.INTEGER:
      case Types.DATE:
      case Types.DECIMAL:
        return 10;
      case Types.BIGINT:
        return 19;
      case Types.CHAR:
      case Types.BOOLEAN:
      case Types.BINARY:
        return 1;
      case Types.FLOAT:
        return 7;
      case Types.DOUBLE:
        return 15;
      case Types.TIMESTAMP:
        return 29;
      case Types.ARRAY:
      case Types.LONGNVARCHAR:
      case Types.STRUCT:
      default:
        return 255;
    }
  }

  public static int getScale(Integer columnType) {
    if (columnType == null) {
      return 0;
    }
    return columnType == Types.TIMESTAMP ? 9 : 0;
  }

  public static boolean isSigned(ColumnInfoTypeName typeName) {
    return SIGNED_TYPES.contains(typeName);
  }

  public static Nullable getNullableFromValue(Integer isNullable) {
    if (isNullable == null) {
      return Nullable.UNKNOWN;
    } else if (isNullable == 0) {
      return Nullable.NO_NULLS;
    } else if (isNullable == 1) {
      return Nullable.NULLABLE;
    } else {
      return Nullable.UNKNOWN;
    }
  }

  /**
   * Converts SQL type into Databricks type as defined <a
   * href="https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html">here</a>
   *
   * @param sqlType SQL type input
   * @return databricks type
   */
  public static String getDatabricksTypeFromSQLType(int sqlType) {
    switch (sqlType) {
      case Types.CHAR:
        return DatabricksTypeUtil.CHAR;
      case Types.ARRAY:
        return ARRAY;
      case Types.BIGINT:
        return LONG;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return BINARY;
      case Types.DATE:
        return DATE;
      case Types.DECIMAL:
      case Types.NUMERIC:
        return DECIMAL; // Databricks treats NUMERIC as DECIMAL
      case Types.BIT:
      case Types.BOOLEAN:
        return BOOLEAN;
      case Types.DOUBLE:
        return DOUBLE;
      case Types.FLOAT:
      case Types.REAL: // REAL is float(24)
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
        // TODO: Handle more SQL types
        return NULL;
    }
  }

  /**
   * Infers Databricks type from class of given object as defined in <a
   * href="https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html">here</a>
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
    } else if (isTimestamp(obj)) {
      type = TIMESTAMP;
    } else if (isDate(obj)) {
      type = DATE;
    } else if (obj instanceof Double) {
      type = DOUBLE;
    }
    // TODO: Handle more object types
    return type;
  }

  public static boolean isTimestamp(Object obj) {
    return obj instanceof java.sql.Timestamp
        || obj instanceof java.time.LocalDateTime
        || obj instanceof java.time.Instant
        || obj instanceof java.time.ZonedDateTime;
  }

  public static boolean isDate(Object obj) {
    return obj instanceof java.sql.Date || obj instanceof java.time.LocalDate;
  }

  public static boolean isTemporalType(Object obj) {
    return isTimestamp(obj) || isDate(obj);
  }

  public static TPrimitiveTypeEntry getTPrimitiveTypeOrDefault(TTypeDesc typeDesc) {
    TPrimitiveTypeEntry defaultPrimitiveTypeEntry = new TPrimitiveTypeEntry(TTypeId.STRING_TYPE);
    return Optional.ofNullable(typeDesc)
        .map(TTypeDesc::getTypes)
        .map(t -> t.get(0))
        .map(TTypeEntry::getPrimitiveEntry)
        .orElse(defaultPrimitiveTypeEntry);
  }

  public static ArrowType mapThriftToArrowType(TTypeId typeId) throws SQLException {
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
        throw new DatabricksSQLFeatureNotSupportedException(
            "Unsupported mapping of Thrift to ArrowType: " + typeId);
    }
  }

  /*
   * Returns the Databricks type string for a given BigDecimal object.
   * Format: DECIMAL(p,s) where p is the precision and s is the scale.
   * Note: precision cannot be less than scale.
   */
  public static String getDecimalTypeString(BigDecimal bd) {
    int precision = bd.precision();
    int scale = bd.scale();
    if (precision < scale) {
      // In type(p,q) -> p should not be less than q. case BigDecimal("0.00")
      precision = scale;
    }
    return DECIMAL + "(" + precision + "," + scale + ")";
  }
}
