package com.databricks.jdbc.core;

import static com.databricks.jdbc.core.DatabricksTypeUtil.getThriftTypeFromTypeDesc;
import static com.databricks.jdbc.core.DatabricksTypeUtil.maptoArrowType;

import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.core.types.CompressionType;
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
      baos.write(getSerializedSchema(metadata));
      for (TSparkArrowBatch arrowBatch : arrowBatches) {
        totalRows += arrowBatch.getRowCount();
        baos.write(arrowBatch.getBatch());
      }
      this.byteStream = new ByteArrayInputStream(baos.toByteArray());
    } catch (DatabricksSQLException | IOException e) {
      String errorMessage = "Unable to extract arrow vectors from inline arrowJson";
      LOGGER.error(errorMessage);
      throw new DatabricksParsingException(errorMessage, e);
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
      throw new DatabricksParsingException("Couldn't convert to serialised schema");
    }
  }

  private static Schema hiveSchemaToArrowSchema(TTableSchema hiveSchema) {
    List<Field> fields = new ArrayList<>();
    hiveSchema
        .getColumns()
        .forEach(
            columnDesc -> {
              TTypeId thriftType = getThriftTypeFromTypeDesc(columnDesc.getTypeDesc());
              ArrowType arrowType = null;
              try {
                arrowType = maptoArrowType(thriftType);
              } catch (DatabricksSQLException e) {
                throw new RuntimeException(e);
              }
              FieldType fieldType = new FieldType(true, arrowType, null);
              fields.add(new Field(columnDesc.getColumnName(), fieldType, null));
            });
    return new Schema(fields);
  }
}
