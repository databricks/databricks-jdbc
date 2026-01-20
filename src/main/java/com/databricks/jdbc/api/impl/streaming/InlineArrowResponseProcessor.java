package com.databricks.jdbc.api.impl.streaming;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.getTPrimitiveTypeOrDefault;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.mapThriftToArrowType;
import static com.databricks.jdbc.common.util.DecompressionUtil.decompress;

import com.databricks.jdbc.api.impl.arrow.ArrowResultChunk;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
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
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

/**
 * Processes Thrift responses into Arrow chunks.
 *
 * <p>This processor converts {@link TFetchResultsResp} into {@link ArrowResultChunk} for inline
 * Arrow result handling. It caches the Arrow schema from the first response for use in subsequent
 * batches.
 */
public class InlineArrowResponseProcessor implements ThriftResponseProcessor<ArrowResultChunk> {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(InlineArrowResponseProcessor.class);

  private final StatementId statementId;
  private volatile byte[]
      cachedSchema; // Cache schema from first response, volatile for visibility across threads

  /**
   * Creates a new inline Arrow response processor.
   *
   * @param statementId The statement ID for logging and chunk creation
   */
  public InlineArrowResponseProcessor(StatementId statementId) {
    this.statementId = statementId;
  }

  @Override
  public StreamingBatch<ArrowResultChunk> processInitialResponse(TFetchResultsResp response)
      throws DatabricksSQLException {
    LOGGER.debug("Processing initial inline Arrow response");
    // Cache the schema for subsequent batches
    try {
      this.cachedSchema = getSerializedSchema(response.getResultSetMetadata());
    } catch (DatabricksParsingException e) {
      throw new DatabricksSQLException(
          "Failed to serialize Arrow schema",
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
    return processResponse(response, 0, 0);
  }

  @Override
  public StreamingBatch<ArrowResultChunk> processResponse(
      TFetchResultsResp response, long batchIndex, long rowOffset) throws DatabricksSQLException {

    StreamingBatch<ArrowResultChunk> batch =
        new StreamingBatch<>(batchIndex, rowOffset, getReleaseAction());

    try {
      ByteArrayInputStream byteStream = createArrowByteStream(response);
      long rowCount = getTotalRowsInResponse(response);

      ArrowResultChunk.Builder builder =
          ArrowResultChunk.builder().withInputStream(byteStream, rowCount);

      if (statementId != null) {
        builder.withStatementId(statementId);
      }

      ArrowResultChunk chunk = builder.build();
      batch.setData(chunk, rowCount, response.hasMoreRows);

      LOGGER.debug(
          "Processed inline Arrow batch {}: rows={}, hasMoreRows={}",
          batchIndex,
          rowCount,
          response.hasMoreRows);

      return batch;

    } catch (DatabricksParsingException e) {
      LOGGER.error("Failed to process inline Arrow batch {}: {}", batchIndex, e.getMessage(), e);
      batch.setError(e);
      throw new DatabricksSQLException(
          "Failed to process Arrow data", e, DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  @Override
  public Consumer<ArrowResultChunk> getReleaseAction() {
    // Arrow chunks require explicit native memory cleanup
    return chunk -> {
      if (chunk != null) {
        chunk.releaseChunk();
        LOGGER.debug("Released Arrow chunk native memory");
      }
    };
  }

  /**
   * Creates a ByteArrayInputStream containing the Arrow IPC data from the response.
   *
   * @param response The Thrift fetch response
   * @return A ByteArrayInputStream containing the Arrow data
   * @throws DatabricksParsingException if processing fails
   */
  private ByteArrayInputStream createArrowByteStream(TFetchResultsResp response)
      throws DatabricksParsingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CompressionCodec compressionType = getCompressionCodec(response);

    try {
      // Write schema if available (for first batch, cachedSchema is set; for subsequent, use it)
      if (cachedSchema != null && cachedSchema.length > 0) {
        baos.write(cachedSchema);
      }

      // Write arrow batches
      writeArrowBatchesToStream(compressionType, response.getResults().getArrowBatches(), baos);

      return new ByteArrayInputStream(baos.toByteArray());
    } catch (DatabricksSQLException | IOException e) {
      LOGGER.error("Failed to create Arrow byte stream: {}", e.getMessage(), e);
      throw new DatabricksParsingException(
          "Failed to create Arrow byte stream: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  /** Writes decompressed Arrow batches to the output stream. */
  private void writeArrowBatchesToStream(
      CompressionCodec compressionCodec,
      List<TSparkArrowBatch> arrowBatchList,
      ByteArrayOutputStream baos)
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
                  "Streaming inline Arrow batch [%d] for statement [%s] with decompression: [%s]",
                  arrowBatch.getRowCount(), statementId, compressionCodec));
      baos.write(decompressedBytes);
    }
  }

  /** Gets the total row count from all Arrow batches in the response. */
  private long getTotalRowsInResponse(TFetchResultsResp response) {
    long totalRows = 0;
    if (response.getResults() != null && response.getResults().getArrowBatches() != null) {
      for (TSparkArrowBatch arrowBatch : response.getResults().getArrowBatches()) {
        totalRows += arrowBatch.getRowCount();
      }
    }
    return totalRows;
  }

  /**
   * Gets the serialized Arrow schema from metadata, or converts from Hive schema if not present.
   */
  private byte[] getSerializedSchema(TGetResultSetMetadataResp metadata)
      throws DatabricksParsingException {
    if (metadata.getArrowSchema() != null) {
      return metadata.getArrowSchema();
    }
    Schema arrowSchema = hiveSchemaToArrowSchema(metadata.getSchema());
    try {
      return SchemaUtility.serialize(arrowSchema);
    } catch (IOException e) {
      throw new DatabricksParsingException(
          "Failed to serialize Arrow schema: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  /** Converts a Hive TTableSchema to an Arrow Schema. */
  private Schema hiveSchemaToArrowSchema(TTableSchema hiveSchema)
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
        fields.add(getArrowField(columnDesc));
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

  /** Creates an Arrow Field from a Thrift column descriptor. */
  private Field getArrowField(TColumnDesc columnDesc) throws SQLException {
    TPrimitiveTypeEntry primitiveTypeEntry = getTPrimitiveTypeOrDefault(columnDesc.getTypeDesc());
    ArrowType arrowType = mapThriftToArrowType(primitiveTypeEntry.getType());
    FieldType fieldType = new FieldType(true, arrowType, null);
    return new Field(columnDesc.getColumnName(), fieldType, null);
  }
}
