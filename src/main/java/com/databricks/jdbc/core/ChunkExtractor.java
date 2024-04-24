package com.databricks.jdbc.core;

import static com.databricks.jdbc.core.DatabricksTypeUtil.getThriftTypeFromTypeDesc;
import static com.databricks.jdbc.core.DatabricksTypeUtil.maptoArrowType;

import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.core.types.CompressionType;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to manage inline Arrow chunks */
public class ChunkExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkExtractor.class);
  private long totalRows;
  private long currentChunkIndex;
  private ByteArrayInputStream byteStream;

  ArrowResultChunk arrowResultChunk; // There is only one packet of data in case of inline arrow

  ChunkExtractor(List<TSparkArrowBatch> arrowBatches, TGetResultSetMetadataResp metadata)
      throws DatabricksParsingException {
    this.currentChunkIndex = -1;
    this.totalRows = 0;
    initializeByteStream(arrowBatches, metadata);
    // Todo : Add compression appropriately
    arrowResultChunk = new ArrowResultChunk(totalRows, null, CompressionType.NONE, byteStream);
  }

  public boolean hasNext() {
    return this.currentChunkIndex == -1;
  }

  public ArrowResultChunk next() {
    if (this.currentChunkIndex != -1) {
      return null;
    }
    this.currentChunkIndex++;
    return arrowResultChunk;
  }

  private void initializeByteStream(
      List<TSparkArrowBatch> arrowBatches, TGetResultSetMetadataResp metadata)
      throws DatabricksParsingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] serializedSchema = getSerializedSchema(metadata);
      if (serializedSchema != null) {
        baos.write(serializedSchema);
      }
      for (TSparkArrowBatch arrowBatch : arrowBatches) {
        totalRows += arrowBatch.getRowCount();
        baos.write(arrowBatch.getBatch());
      }
      this.byteStream = new ByteArrayInputStream(baos.toByteArray());
    } catch (DatabricksSQLException | IOException e) {
      handleError(e);
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
                } catch (DatabricksSQLException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (RuntimeException e) {
      handleError(e);
    }
    return new Schema(fields);
  }

  private static Field getArrowField(TColumnDesc columnDesc) throws DatabricksSQLException {
    TTypeId thriftType = getThriftTypeFromTypeDesc(columnDesc.getTypeDesc());
    ArrowType arrowType = null;
    arrowType = maptoArrowType(thriftType);
    FieldType fieldType = new FieldType(true, arrowType, null);
    return new Field(columnDesc.getColumnName(), fieldType, null);
  }

  @VisibleForTesting
  static void handleError(Exception e) throws DatabricksParsingException {
    String errorMessage = "Cannot process inline arrow format. Error: " + e.getMessage();
    LOGGER.error(errorMessage);
    throw new DatabricksParsingException(errorMessage, e);
  }

  public void releaseChunk() {
    this.arrowResultChunk.releaseChunk();
  }
}
