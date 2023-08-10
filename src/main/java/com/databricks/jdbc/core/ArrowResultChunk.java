package com.databricks.jdbc.core;

import com.databricks.client.jdbc42.internal.apache.arrow.memory.RootAllocator;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.FieldVector;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.ValueVector;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.VectorSchemaRoot;
import com.databricks.client.jdbc42.internal.apache.arrow.vector.ipc.ArrowStreamReader;
import com.databricks.sdk.service.sql.ChunkInfo;
import com.databricks.sdk.service.sql.ExternalLink;

import java.text.SimpleDateFormat;
import java.time.Instant;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ArrowResultChunk {

  /**
   * The status of a chunk would proceed in following path:
   * <ul>
   *   <li>Create placeholder for chunk, along with the chunk cardinal</li>
   *   <li>Fetch chunk url</li>
   *   <li>Submit task for data download</li>
   *   <ul>
   *     <li>Download has completed</li>
   *     <li>Download has failed and we will retry</li>
   *     <li>Download has failed and we gave up</li>
   *   </ul>
   *   <li>Data has been consumed and chunk is free to be released from memory</li>
   * </ul>
   * ->
   */
  enum DownloadStatus {
    // Default status, though for the ArrowChunk, it should be initialized with Pending state
    UNKNOWN,
    // This is a placeholder for chunk, we don't even have the Url
    PENDING,
    // We have the Url for the chunk, and it is ready for download
    URL_FETCHED,
    // Download task has been submitted
    DOWNLOAD_IN_PROGRESS,
    // Data has been downloaded and ready for consumption
    DOWNLOAD_SUCCEEDED,
    // Download has failed and it would be retried
    DOWNLOAD_FAILED_RETRYABLE,
    // Download has failed and we have given up
    DOWNLOAD_FAILED_ABORTED,
    // Download has been cancelled
    CANCELLED,
    // Chunk has been consumed, and is free to be released. Since we do not support backward scroll in result set,
    // the chunk won't be needed again
    CHUNK_CONSUMED,
    // Chunk memory has been released
    CHUNK_RELEASED;
  }

  private final long chunkIndex;
  final long numRows;
  final long rowOffset;
  final long byteCount;

  private String chunkUrl;
  private Long nextChunkIndex;
  private Instant expiryTime;
  private DownloadStatus status;

  private InputStream inputStream;

  private final ArrayList<ArrayList<ValueVector>> recordBatchList;

  private final ArrowResultChunkIterator chunkIterator;

  private RootAllocator rootAllocator;

  ArrowResultChunk(ChunkInfo chunkInfo) {
    this.chunkIndex = chunkInfo.getChunkIndex();
    this.numRows = chunkInfo.getRowCount();
    this.rowOffset = chunkInfo.getRowOffset();
    this.nextChunkIndex = chunkInfo.getNextChunkIndex();
    this.byteCount = chunkInfo.getByteCount();
    this.status = DownloadStatus.PENDING;
    this.recordBatchList = new ArrayList<>();
    this.chunkIterator = new ArrowResultChunkIterator(this);
    this.chunkUrl = null;
  }

  /**
   * Sets link details for the given chunk.
   */
  void setChunkUrl(ExternalLink chunk) {
    this.chunkUrl = chunk.getExternalLink();
    this.nextChunkIndex = chunk.getNextChunkIndex();
    this.expiryTime = Instant.parse(chunk.getExpiration());
    this.status = DownloadStatus.URL_FETCHED;
  }

  /**
   * Updates status for the chunk
   */
  void setStatus(DownloadStatus status) {
    this.status = status;
  }

  private void getArrowDataFromInputStream() throws Exception {
    // add check to see if input stream has been populated
    ArrowStreamReader arrowStreamReader = new ArrowStreamReader(this.inputStream, this.rootAllocator);
    VectorSchemaRoot vectorSchemaRoot = arrowStreamReader.getVectorSchemaRoot();
    while(arrowStreamReader.loadNextBatch()) {
      ArrayList<ValueVector> vectors = new ArrayList<>();
      List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
      for(FieldVector fieldVector: fieldVectors) {
        vectors.add((ValueVector) fieldVector);
      }
      this.recordBatchList.add(vectors);
    }
  }

  public void nextInChunk() {
    this.chunkIterator.nextRow();
  }

  int getRecordBatchCountInChunk() {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Checks if the link is valid
   */
  boolean isChunkLinkValid() {
    return expiryTime == null || expiryTime.isAfter(Instant.now());
  }

  /**
   * Returns the status for the chunk
   */
  DownloadStatus getStatus() {
    return this.status;
  }

  /**
   * Returns next chunk index for given chunk. Null is returned for last chunk.
   */
  Long getNextChunkIndex() {
    // This should never be called for pending state
    if (status == DownloadStatus.PENDING) {
      // TODO: log this
      throw new IllegalStateException("Next index called for pending state chunk");
    }
    return this.nextChunkIndex;
  }

  /**
   * Returns the chunk download link
   */
  String getChunkUrl() {
    return chunkUrl;
  }
}
