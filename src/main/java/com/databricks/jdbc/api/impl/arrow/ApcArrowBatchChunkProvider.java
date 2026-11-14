package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DecompressionUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Pages APC Arrow batches using server continuation markers, without manifest chunk totals. */
final class ApcArrowBatchChunkProvider implements ChunkProvider {
  private final IDatabricksSession session;
  private final StatementId statementId;
  private final IDatabricksHttpClient httpClient;
  private final CompressionCodec compressionCodec;
  private final byte[] schemaMessage;
  private final ArrayDeque<ArrowResultChunk> pending = new ArrayDeque<>();
  private final ExecutorService downloadExecutor =
      Executors.newSingleThreadExecutor(
          task -> {
            Thread thread = new Thread(task, "apc-arrow-batch-downloader");
            thread.setDaemon(true);
            return thread;
          });
  private ArrowResultChunk current;
  private Long nextChunkIndex;
  private long nextRowOffset;
  private long rowCount;
  private long chunkCount;
  private boolean closed;

  ApcArrowBatchChunkProvider(
      ResultData initialData,
      ResultManifest manifest,
      StatementId statementId,
      IDatabricksSession session,
      IDatabricksHttpClient httpClient)
      throws DatabricksSQLException {
    this.session = session;
    this.statementId = statementId;
    this.httpClient = httpClient;
    this.compressionCodec = manifest.getResultCompression();
    this.schemaMessage = ArrowBatchSchemaBuilder.serialize(manifest.getSchema());
    if (initialData == null) {
      nextChunkIndex = 0L;
    } else {
      try {
        addPage(initialData);
      } catch (DatabricksSQLException | RuntimeException e) {
        close();
        throw e;
      }
    }
  }

  @Override
  public boolean hasNextChunk() {
    return !closed && (!pending.isEmpty() || nextChunkIndex != null);
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (closed) {
      return false;
    }
    if (current != null) {
      current.releaseChunk();
      current = null;
    }
    while (pending.isEmpty() && nextChunkIndex != null) {
      long index = nextChunkIndex;
      long offset = nextRowOffset;
      ResultData page =
          session.getDatabricksClient().getResultChunksData(statementId, index, offset);
      if (page == null) {
        throw invalidPage("APC result page is missing");
      }
      addPage(page);
      if (pending.isEmpty()
          && nextChunkIndex != null
          && index == nextChunkIndex
          && offset == nextRowOffset) {
        throw invalidPage("APC result continuation did not advance");
      }
    }
    if (pending.isEmpty()) {
      return false;
    }
    current = pending.removeFirst();
    if (current.getChunkLink() != null) {
      ArrowResultChunk chunk = current;
      StreamingChunkDownloadTask task =
          new StreamingChunkDownloadTask(
              chunk,
              httpClient,
              compressionCodec,
              (index, offset) -> {
                ExternalLink refreshed =
                    new SeaChunkLinkFetcher(session, statementId).refetchLink(index, offset);
                chunk.setChunkLink(refreshed);
                return refreshed;
              },
              session.getConnectionContext().getCloudFetchSpeedThreshold());
      try {
        downloadExecutor.submit(task).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DatabricksSQLException(
            "Interrupted downloading APC result chunk",
            DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR.name(),
            DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR.getCode(),
            e);
      } catch (ExecutionException e) {
        throw new DatabricksSQLException(
            "Unable to download APC result chunk",
            DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.name(),
            DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.getCode(),
            e.getCause());
      }
    }
    chunkCount++;
    return true;
  }

  private void addPage(ResultData page) throws DatabricksSQLException {
    List<ArrowBatch> batches = page.getArrowBatches();
    Collection<ExternalLink> links = page.getExternalLinks();
    long inlineRows = 0;
    if (batches != null && !batches.isEmpty()) {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      try {
        stream.write(schemaMessage);
        for (ArrowBatch batch : batches) {
          if (batch.getBatch() == null || batch.getRowCount() == null) {
            throw invalidPage("APC Arrow batch is missing bytes or row_count");
          }
          stream.write(
              DecompressionUtil.decompress(
                  batch.getBatch(), compressionCodec, "APC inline Arrow batch"));
          inlineRows += batch.getRowCount();
        }
      } catch (IOException e) {
        DatabricksDriverErrorCode code = DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR;
        throw new DatabricksParsingException(
            "Unable to assemble APC Arrow batches", e, code.name(), code.getCode());
      }
      pending.add(
          ArrowResultChunk.builder()
              .withStatementId(statementId)
              .withInputStream(new ByteArrayInputStream(stream.toByteArray()), inlineRows)
              .build());
      rowCount += inlineRows;
    }
    if (links != null && !links.isEmpty()) {
      if (!session.getConnectionContext().isCloudFetchEnabled()) {
        throw invalidPage("APC returned external links when result download is disabled");
      }
      List<ExternalLink> ordered = new ArrayList<>(links);
      for (ExternalLink link : ordered) {
        if (link.getChunkIndex() == null
            || link.getRowCount() == null
            || link.getRowOffset() == null) {
          throw invalidPage("APC external link is missing paging metadata");
        }
        ArrowResultChunk chunk =
            ArrowResultChunk.builder()
                .withStatementId(statementId)
                .withConnectionContext(session.getConnectionContext())
                .withChunkMetadata(link.getChunkIndex(), link.getRowCount(), link.getRowOffset())
                .build();
        chunk.setChunkLink(link);
        pending.add(chunk);
        rowCount += link.getRowCount();
      }
      ExternalLink last = ordered.get(ordered.size() - 1);
      nextChunkIndex = last.getNextChunkIndex();
      nextRowOffset = last.getRowOffset() + last.getRowCount();
    } else {
      nextChunkIndex = page.getNextChunkIndex();
      nextRowOffset += inlineRows;
    }
  }

  private static DatabricksParsingException invalidPage(String message) {
    DatabricksDriverErrorCode code = DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR;
    return new DatabricksParsingException(message, null, code.name(), code.getCode());
  }

  @Override
  public ArrowResultChunk getChunk() {
    return current;
  }

  @Override
  public void close() {
    closed = true;
    if (current != null) {
      current.releaseChunk();
      current = null;
    }
    for (ArrowResultChunk chunk : pending) {
      chunk.releaseChunk();
    }
    pending.clear();
    downloadExecutor.shutdownNow();
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public long getChunkCount() {
    return chunkCount;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
