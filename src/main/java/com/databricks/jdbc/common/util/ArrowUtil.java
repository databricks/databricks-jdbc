package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getColumnInfoFromTColumnDesc;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.getTPrimitiveTypeOrDefault;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.mapThriftToArrowType;
import static com.databricks.jdbc.common.util.DecompressionUtil.decompress;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TColumnDesc;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.client.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.model.client.thrift.generated.TPrimitiveTypeEntry;
import com.databricks.jdbc.model.client.thrift.generated.TSparkArrowBatch;
import com.databricks.jdbc.model.client.thrift.generated.TTableSchema;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ResultSchema;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

/**
 * Utility class for Arrow operations.
 *
 * <p>Provides methods for:
 *
 * <ul>
 *   <li>Converting Thrift/Hive schemas to Arrow schemas and serialization
 *   <li>Creating Arrow IPC byte streams from Thrift responses
 *   <li>Processing Arrow batches with decompression
 * </ul>
 *
 * <p>This consolidates Arrow handling logic used by both streaming and lazy inline Arrow result
 * handlers.
 */
public final class ArrowUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowUtil.class);
  private static final byte[] ARROW_STREAM_END_MARKER = {
    (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0
  };

  private ArrowUtil() {
    // Utility class - prevent instantiation
  }

  // ==================== Schema Operations ====================

  /**
   * Gets the serialized Arrow schema from Thrift metadata.
   *
   * <p>If the metadata contains a pre-serialized Arrow schema, it is returned directly. Otherwise,
   * the Hive schema is converted to Arrow format and serialized.
   *
   * @param metadata The Thrift result set metadata
   * @return The serialized Arrow schema bytes
   * @throws DatabricksParsingException if schema conversion or serialization fails
   */
  public static byte[] getSerializedSchema(TGetResultSetMetadataResp metadata)
      throws DatabricksParsingException {
    if (metadata == null) {
      LOGGER.debug("Metadata is null, returning empty schema");
      return new byte[0];
    }

    // Use pre-serialized Arrow schema if available
    if (metadata.getArrowSchema() != null) {
      return metadata.getArrowSchema();
    }

    // Convert Hive schema to Arrow and serialize
    Schema arrowSchema = hiveSchemaToArrowSchema(metadata.getSchema());
    try {
      return SchemaUtility.serialize(arrowSchema);
    } catch (IOException e) {
      LOGGER.error(e, "Failed to serialize arrow schema");
      throw new DatabricksParsingException(
          "Failed to serialize Arrow schema: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  /**
   * Converts a Hive TTableSchema to an Arrow Schema.
   *
   * @param hiveSchema The Hive table schema from Thrift
   * @return The equivalent Arrow schema
   * @throws DatabricksParsingException if conversion fails
   */
  public static Schema hiveSchemaToArrowSchema(TTableSchema hiveSchema)
      throws DatabricksParsingException {
    List<Field> fields = new ArrayList<>();
    if (hiveSchema == null) {
      LOGGER.debug("Hive schema is null, returning empty Arrow schema");
      return new Schema(fields);
    }

    try {
      LOGGER.debug(
          "Converting Hive schema to Arrow schema with {} columns", hiveSchema.getColumnsSize());
      for (TColumnDesc columnDesc : hiveSchema.getColumns()) {
        fields.add(columnDescToArrowField(columnDesc));
      }
    } catch (SQLException e) {
      LOGGER.error("Failed to convert Hive schema to Arrow: {}", e.getMessage(), e);
      throw new DatabricksParsingException(
          "Failed to convert Hive schema to Arrow: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
    return new Schema(fields);
  }

  /** Serializes a SQL Execution API result schema as the first message of an Arrow IPC stream. */
  private static byte[] serializeResultSchema(ResultSchema resultSchema)
      throws DatabricksParsingException {
    Schema arrowSchema = resultSchemaToArrowSchema(resultSchema);
    try {
      return SchemaUtility.serialize(arrowSchema);
    } catch (IOException e) {
      throw new DatabricksParsingException(
          "Failed to serialize SQL Execution API Arrow schema: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  /** Converts the structured SQL Execution API schema used by native APC Arrow batches. */
  public static Schema resultSchemaToArrowSchema(ResultSchema resultSchema)
      throws DatabricksParsingException {
    if (resultSchema == null || resultSchema.getColumnCount() == null) {
      throw arrowBatchSchemaError("missing result schema or column count");
    }
    if (resultSchema.getColumnCount() < 0) {
      throw arrowBatchSchemaError("negative column count");
    }

    Collection<ColumnInfo> columns = resultSchema.getColumns();
    int actualColumnCount = columns == null ? 0 : columns.size();
    if (resultSchema.getColumnCount() != actualColumnCount) {
      throw arrowBatchSchemaError(
          String.format(
              "column count mismatch: manifest=%d columns=%d",
              resultSchema.getColumnCount(), actualColumnCount));
    }

    List<Field> fields = new ArrayList<>(actualColumnCount);
    if (columns == null) {
      return new Schema(fields);
    }
    for (ColumnInfo column : columns) {
      if (column == null || column.getName() == null || column.getName().isEmpty()) {
        throw arrowBatchSchemaError("missing column or column name");
      }
      fields.add(new Field(column.getName(), new FieldType(true, toArrowType(column), null), null));
    }
    return new Schema(fields);
  }

  /** Builds a complete Arrow IPC stream from one schema and ordered record-batch messages. */
  public static ByteArrayInputStream createArrowBatchByteStream(
      ResultSchema resultSchema, List<ArrowBatch> arrowBatches) throws DatabricksParsingException {
    if (arrowBatches == null) {
      throw arrowBatchSchemaError("missing arrow_batches");
    }
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try {
      output.write(serializeResultSchema(resultSchema));
      for (ArrowBatch arrowBatch : arrowBatches) {
        output.write(arrowBatch.getBatch());
      }
      output.write(ARROW_STREAM_END_MARKER);
      return new ByteArrayInputStream(output.toByteArray());
    } catch (IOException e) {
      throw new DatabricksParsingException(
          "Failed to assemble native Arrow batches: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  private static ArrowType toArrowType(ColumnInfo column) throws DatabricksParsingException {
    ColumnInfoTypeName typeName = column.getTypeName();
    if (typeName == null) {
      typeName = DatabricksTypeUtil.recoverStringType(column.getTypeText());
    }
    if (typeName == null) {
      throw arrowBatchSchemaError("missing type for column " + column.getName());
    }

    switch (typeName) {
      case BOOLEAN:
        return ArrowType.Bool.INSTANCE;
      case BYTE:
      case TINYINT:
        return new ArrowType.Int(8, true);
      case SHORT:
      case SMALLINT:
        return new ArrowType.Int(16, true);
      case INT:
        return new ArrowType.Int(32, true);
      case BIGINT:
      case LONG:
        return new ArrowType.Int(64, true);
      case FLOAT:
        return new ArrowType.FloatingPoint(
            org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
      case DOUBLE:
        return new ArrowType.FloatingPoint(
            org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
      case CHAR:
      case STRING:
        return ArrowType.Utf8.INSTANCE;
      case BINARY:
        return ArrowType.Binary.INSTANCE;
      case DATE:
        return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
      case TIMESTAMP:
        return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
      case DECIMAL:
        return decimalArrowType(column);
      case NULL:
      case VOID:
        return ArrowType.Null.INSTANCE;
      default:
        throw arrowBatchSchemaError(
            "unsupported type " + typeName + " for column " + column.getName());
    }
  }

  private static ArrowType decimalArrowType(ColumnInfo column) throws DatabricksParsingException {
    if (column.getTypePrecision() == null || column.getTypeScale() == null) {
      throw arrowBatchSchemaError("missing decimal precision or scale for " + column.getName());
    }
    try {
      int precision = Math.toIntExact(column.getTypePrecision());
      int scale = Math.toIntExact(column.getTypeScale());
      if (precision < 1 || precision > 38 || scale < 0 || scale > precision) {
        throw arrowBatchSchemaError(
            String.format(
                "invalid decimal(%d,%d) for column %s", precision, scale, column.getName()));
      }
      return new ArrowType.Decimal(precision, scale, 128);
    } catch (ArithmeticException e) {
      throw arrowBatchSchemaError(
          "decimal precision or scale is out of range for " + column.getName());
    }
  }

  private static DatabricksParsingException arrowBatchSchemaError(String detail) {
    return new DatabricksParsingException(
        "Cannot construct native Arrow-batch schema: " + detail,
        DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
  }

  /**
   * Creates an Arrow Field from a Thrift column descriptor.
   *
   * @param columnDesc The Thrift column descriptor
   * @return The equivalent Arrow field
   * @throws SQLException if type mapping fails
   */
  public static Field columnDescToArrowField(TColumnDesc columnDesc) throws SQLException {
    TPrimitiveTypeEntry primitiveTypeEntry = getTPrimitiveTypeOrDefault(columnDesc.getTypeDesc());
    ArrowType arrowType = mapThriftToArrowType(primitiveTypeEntry.getType());
    FieldType fieldType = new FieldType(true, arrowType, null);
    return new Field(columnDesc.getColumnName(), fieldType, null);
  }

  // ==================== Arrow Stream Operations ====================

  /**
   * Creates a ByteArrayInputStream containing Arrow IPC data from the response.
   *
   * <p>This method combines the cached schema with decompressed Arrow batches to create a complete
   * Arrow IPC stream that can be parsed by Arrow readers.
   *
   * @param cachedSchema The serialized Arrow schema bytes (should be cached from first response)
   * @param response The Thrift fetch response containing Arrow batches
   * @param callerClass The calling class for logging context
   * @return ByteArrayInputStream containing the Arrow IPC data
   * @throws DatabricksParsingException if processing fails
   */
  public static ByteArrayInputStream createArrowByteStream(
      byte[] cachedSchema, TFetchResultsResp response, Class<?> callerClass)
      throws DatabricksParsingException {
    String context = callerClass.getSimpleName();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CompressionCodec compressionCodec =
        CompressionCodec.getCompressionMapping(response.getResultSetMetadata());

    try {
      // Write schema if available
      if (cachedSchema != null && cachedSchema.length > 0) {
        baos.write(cachedSchema);
      }

      // Write arrow batches
      List<TSparkArrowBatch> arrowBatches =
          response.getResults() != null ? response.getResults().getArrowBatches() : null;
      writeArrowBatchesToStream(compressionCodec, arrowBatches, baos, context);

      return new ByteArrayInputStream(baos.toByteArray());
    } catch (DatabricksSQLException | IOException e) {
      LOGGER.error("{}: Failed to create Arrow byte stream: {}", context, e.getMessage(), e);
      throw new DatabricksParsingException(
          "Failed to create Arrow byte stream: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  /**
   * Writes decompressed Arrow batches to the output stream.
   *
   * @param compressionCodec The compression codec used for the batches
   * @param arrowBatchList The list of Arrow batches to write
   * @param baos The output stream to write to
   * @param context Context string for logging (typically caller class simple name)
   * @throws DatabricksSQLException if decompression fails
   * @throws IOException if writing fails
   */
  static void writeArrowBatchesToStream(
      CompressionCodec compressionCodec,
      List<TSparkArrowBatch> arrowBatchList,
      ByteArrayOutputStream baos,
      String context)
      throws DatabricksSQLException, IOException {
    if (arrowBatchList == null) {
      return;
    }
    for (TSparkArrowBatch arrowBatch : arrowBatchList) {
      byte[] decompressedBytes =
          decompress(
              arrowBatch.getBatch(),
              compressionCodec,
              String.format(
                  "%s Arrow batch [%d rows] with decompression: [%s]",
                  context, arrowBatch.getRowCount(), compressionCodec));
      baos.write(decompressedBytes);
    }
  }

  /**
   * Gets the total row count from all Arrow batches in the response.
   *
   * @param response The Thrift fetch response
   * @return The total number of rows across all batches
   */
  public static long getTotalRowsInResponse(TFetchResultsResp response) {
    long totalRows = 0;
    if (response.getResults() != null && response.getResults().getArrowBatches() != null) {
      for (TSparkArrowBatch arrowBatch : response.getResults().getArrowBatches()) {
        totalRows += arrowBatch.getRowCount();
      }
    }
    return totalRows;
  }

  // ==================== Column Info Operations ====================

  /**
   * Extracts column information from Thrift result set metadata.
   *
   * <p>Converts each column descriptor in the Thrift schema to a {@link ColumnInfo} object.
   *
   * @param resultManifest The Thrift result set metadata containing schema information
   * @return A list of ColumnInfo objects, empty list if schema is null
   */
  public static List<ColumnInfo> getColumnInfoList(TGetResultSetMetadataResp resultManifest)
      throws DatabricksSQLException {
    List<ColumnInfo> columnInfos = new ArrayList<>();
    List<String> arrowMetadataList = DatabricksThriftUtil.getArrowMetadata(resultManifest);
    if (resultManifest.getSchema() == null) {
      return columnInfos;
    }
    List<TColumnDesc> columns = resultManifest.getSchema().getColumns();
    for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
      TColumnDesc tColumnDesc = columns.get(columnIndex);
      String columnArrowMetadata =
          arrowMetadataList != null && columnIndex < arrowMetadataList.size()
              ? arrowMetadataList.get(columnIndex)
              : null;
      columnInfos.add(getColumnInfoFromTColumnDesc(tColumnDesc, columnArrowMetadata));
    }
    return columnInfos;
  }
}
