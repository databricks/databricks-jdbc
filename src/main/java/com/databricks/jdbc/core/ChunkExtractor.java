package com.databricks.jdbc.core;

import com.databricks.jdbc.client.IDatabricksHttpClient;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.client.impl.thrift.generated.TGetResultSetMetadataResp;
import com.databricks.jdbc.client.impl.thrift.generated.TSparkArrowBatch;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.core.types.CompressionType;
import com.databricks.sdk.service.sql.BaseChunkInfo;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/** Class to manage inline Arrow chunks */
public class ChunkExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkExtractor.class);
  private long totalRows;
  private long currentChunkIndex;
  private boolean isClosed;
  private ByteArrayInputStream byteStream;

  ArrowResultChunk arrowResultChunk; // There is only one packet of data in case of inline arrow

  ChunkExtractor(List<TSparkArrowBatch>arrowBatches, TGetResultSetMetadataResp metadata) throws DatabricksParsingException {
    this.currentChunkIndex = -1;
    this.isClosed = false;
    this.totalRows = 0;
    initializeByteStream(arrowBatches,metadata);
    arrowResultChunk = new ArrowResultChunk(totalRows,null,CompressionType.NONE,byteStream);
  }

  private void initializeByteStream(List<TSparkArrowBatch>arrowBatches,TGetResultSetMetadataResp metadata) throws DatabricksParsingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try{
    baos.write(metadata.getArrowSchema());
    for (TSparkArrowBatch arrowBatch : arrowBatches) {
      totalRows+= arrowBatch.getRowCount();
      baos.write(arrowBatch.getBatch());
    }
    this.byteStream = new ByteArrayInputStream(baos.toByteArray());
    } catch (IOException e) {
        String errorMessage = "Unable to extract arrow vectors from inline arrowJson";
        LOGGER.error(errorMessage);
        throw new DatabricksParsingException(errorMessage,e);
    }
  }

}
