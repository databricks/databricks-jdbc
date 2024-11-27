package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.*;
import static com.databricks.jdbc.common.util.DecompressionUtil.decompress;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

/** Class to manage inline Arrow chunks */
public class InlineChunkProvider implements ChunkProvider {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(InlineChunkProvider.class);
  private long totalRows;
  private long currentChunkIndex;

  ArrowResultChunk arrowResultChunk; // There is only one packet of data in case of inline arrow

  InlineChunkProvider(
      TFetchResultsResp resultsResp,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws DatabricksParsingException {
    this.currentChunkIndex = -1;
    this.totalRows = 0;
    ByteArrayInputStream byteStream = initializeByteStream(resultsResp, session, parentStatement);
    arrowResultChunk = ArrowResultChunk.builder().withInputStream(byteStream, totalRows).build();
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasNextChunk() {
    return this.currentChunkIndex == -1;
  }

  /** {@inheritDoc} */
  @Override
  public boolean next() {
    if (!hasNextChunk()) {
      return false;
    }
    this.currentChunkIndex++;
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public ArrowResultChunk getChunk() {
    return arrowResultChunk;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    arrowResultChunk.releaseChunk();
  }

  @Override
  public long getRowCount() {
    return totalRows;
  }

  @Override
  public long getChunkCount() {
    return 0;
  }

  private ByteArrayInputStream initializeByteStream(
      TFetchResultsResp resultsResp,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws DatabricksParsingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CompressionCodec compressionType =
        CompressionCodec.getCompressionMapping(resultsResp.getResultSetMetadata());
    try {
      byte[] serializedSchema = getSerializedSchema(resultsResp.getResultSetMetadata());
      if (serializedSchema != null) {
        baos.write(serializedSchema);
      }
      writeToByteOutputStream(
          compressionType, parentStatement, resultsResp.getResults().getArrowBatches(), baos);
      while (resultsResp.hasMoreRows) {
        resultsResp =
            ((DatabricksThriftServiceClient) session.getDatabricksClient())
                .getMoreResults(parentStatement);
        writeToByteOutputStream(
            compressionType, parentStatement, resultsResp.getResults().getArrowBatches(), baos);
      }
      return new ByteArrayInputStream(baos.toByteArray());
    } catch (DatabricksSQLException | IOException e) {
      handleError(e);
    }
    return null;
  }

  void writeToByteOutputStream(
      CompressionCodec compressionCodec,
      IDatabricksStatementInternal parentStatement,
      List<TSparkArrowBatch> arrowBatchList,
      ByteArrayOutputStream baos)
      throws DatabricksSQLException, IOException {
    for (TSparkArrowBatch arrowBatch : arrowBatchList) {
      byte[] decompressedBytes =
          decompress(
              arrowBatch.getBatch(),
              compressionCodec,
              String.format(
                  "Data fetch for inline arrow batch [%d] and statement [%s] with decompression algorithm : [%s]",
                  arrowBatch.getRowCount(), parentStatement, compressionCodec));
      totalRows += arrowBatch.getRowCount();
      baos.write(decompressedBytes);
    }
  }

  private byte[] getSerializedSchema(TGetResultSetMetadataResp metadata)
      throws DatabricksSQLException {
    if (metadata.getArrowSchema() != null) {
      return metadata.getArrowSchema();
    }
    Schema arrowSchema = hiveSchemaToArrowSchema(metadata.getSchema());
    try {
      return SchemaUtility.serialize(arrowSchema);
    } catch (IOException e) {
      handleError(e);
    }
    // should never reach here;
    return null;
  }

  private static Schema hiveSchemaToArrowSchema(TTableSchema hiveSchema)
      throws DatabricksParsingException {
    List<Field> fields = new ArrayList<>();
    if (hiveSchema == null) {
      return new Schema(fields);
    }
    try {
      hiveSchema
          .getColumns()
          .forEach(
              columnDesc -> {
                try {
                  fields.add(getArrowField(columnDesc));
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (RuntimeException e) {
      handleError(e);
    }
    return new Schema(fields);
  }

  private static Field getArrowField(TColumnDesc columnDesc) throws SQLException {
    TTypeId thriftType = getThriftTypeFromTypeDesc(columnDesc.getTypeDesc());
    ArrowType arrowType = mapThriftToArrowType(thriftType);
    FieldType fieldType = new FieldType(true, arrowType, null);
    return new Field(columnDesc.getColumnName(), fieldType, null);
  }

  @VisibleForTesting
  static void handleError(Exception e) throws DatabricksParsingException {
    String errorMessage = "Cannot process inline arrow format. Error: " + e.getMessage();
    LOGGER.error(errorMessage);
    throw new DatabricksParsingException(errorMessage, e);
  }
}
