package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.util.DatabricksTypeUtil.*;

import com.databricks.jdbc.common.CompressionType;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.dbclient.impl.thrift.generated.*;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
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

/** Class to manage inline Arrow chunks */
public class ChunkExtractor {
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
    arrowType = mapThriftToArrowType(thriftType);
    FieldType fieldType = new FieldType(true, arrowType, null);
    return new Field(columnDesc.getColumnName(), fieldType, null);
  }

  @VisibleForTesting
  static void handleError(Exception e) throws DatabricksParsingException {
    String errorMessage = "Cannot process inline arrow format. Error: " + e.getMessage();
    LoggingUtil.log(LogLevel.ERROR, errorMessage);
    throw new DatabricksParsingException(errorMessage, e);
  }

  public void releaseChunk() {
    this.arrowResultChunk.releaseChunk();
  }
}
