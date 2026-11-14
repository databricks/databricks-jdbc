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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Pages APC Arrow batches using server continuation markers, without manifest chunk totals. */
final class ApcArrowBatchChunkProvider implements ChunkProvider {
  private final IDatabricksSession session;
  private final StatementId statementId;
  private final IDatabricksHttpClient httpClient;
  private final CompressionCodec compressionCodec;
  private final byte[] schemaMessage;
  private final Object stateLock = new Object();
  private final ArrayDeque<ArrowResultChunk> pending = new ArrayDeque<>();
  private final ExecutorService downloadExecutor =
      Executors.newSingleThreadExecutor(
          task -> {
            Thread thread = new Thread(task, "apc-arrow-batch-downloader");
            thread.setDaemon(true);
            return thread;
          });
  private ArrowResultChunk current;
  private ArrowResultChunk downloadWorkerChunk;
  private DatabricksSQLException terminalFailure;
  private Long nextChunkIndex;
  private long nextRowOffset;
  private long rowCount;
  private long chunkCount;
  private volatile boolean closed;

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
    String timeZone = "UTC";
    Map<String, String> configs = session.getSessionConfigs();
    if (configs != null) {
      synchronized (configs) {
        configs = new HashMap<>(configs);
      }
      for (Map.Entry<String, String> config : configs.entrySet()) {
        if ("spark.sql.session.timeZone".equalsIgnoreCase(config.getKey())
            && config.getValue() != null) {
          timeZone = config.getValue();
        }
      }
      for (Map.Entry<String, String> config : configs.entrySet()) {
        if ("TIMEZONE".equalsIgnoreCase(config.getKey()) && config.getValue() != null) {
          timeZone = config.getValue();
        }
      }
      // A SET TIME ZONE update publishes the canonical key before removing older key variants.
      // Prefer it if a reader snapshots the map during that short transition.
      if (configs.get("timezone") != null) {
        timeZone = configs.get("timezone");
      }
    }
    // APC bare batches have no wire schema. The manifest is authoritative even when the
    // optional arrow_schema is populated or malformed.
    this.schemaMessage = ArrowBatchSchemaBuilder.serialize(manifest.getSchema(), timeZone);
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
    synchronized (stateLock) {
      return !closed && (terminalFailure != null || !pending.isEmpty() || nextChunkIndex != null);
    }
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    synchronized (stateLock) {
      if (terminalFailure != null) {
        throw terminalFailure;
      }
      if (closed) {
        return false;
      }
      if (current != null) {
        current.releaseChunkAndAllocator();
        current = null;
      }
    }
    while (true) {
      long index;
      long offset;
      synchronized (stateLock) {
        if (closed) {
          return false;
        }
        if (!pending.isEmpty() || nextChunkIndex == null) {
          break;
        }
        index = nextChunkIndex;
        offset = nextRowOffset;
      }
      ResultData page =
          session.getDatabricksClient().getResultChunksData(statementId, index, offset);
      if (page == null) {
        throw invalidPage("APC result page is missing");
      }
      addPage(page);
      synchronized (stateLock) {
        if (!closed
            && pending.isEmpty()
            && nextChunkIndex != null
            && index == nextChunkIndex
            && offset == nextRowOffset) {
          throw invalidPage("APC result continuation did not advance");
        }
      }
    }
    ArrowResultChunk chunk;
    Future<Void> download = null;
    synchronized (stateLock) {
      if (closed || pending.isEmpty()) {
        return false;
      }
      chunk = pending.removeFirst();
      current = chunk;
      if (chunk.getChunkLink() != null) {
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
        download =
            downloadExecutor.submit(
                () -> {
                  synchronized (stateLock) {
                    if (closed) {
                      return null;
                    }
                    downloadWorkerChunk = chunk;
                  }
                  try {
                    return task.call();
                  } finally {
                    synchronized (stateLock) {
                      downloadWorkerChunk = null;
                      if (closed) {
                        chunk.releaseChunkAndAllocator();
                      }
                    }
                  }
                });
      }
    }
    if (download != null) {
      try {
        int timeoutSeconds = session.getConnectionContext().getChunkReadyTimeoutSeconds();
        try {
          if (timeoutSeconds <= 0) {
            download.get();
          } else {
            download.get(timeoutSeconds, TimeUnit.SECONDS);
          }
        } catch (TimeoutException e) {
          download.cancel(true);
          throw failDownload(
              new DatabricksSQLException(
                  "Timed out waiting for APC result chunk",
                  DatabricksDriverErrorCode.CHUNK_READY_ERROR.name(),
                  DatabricksDriverErrorCode.CHUNK_READY_ERROR.getCode(),
                  e));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw failDownload(
            new DatabricksSQLException(
                "Interrupted downloading APC result chunk",
                DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR.name(),
                DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR.getCode(),
                e));
      } catch (ExecutionException e) {
        throw failDownload(
            new DatabricksSQLException(
                "Unable to download APC result chunk",
                DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.name(),
                DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.getCode(),
                e.getCause()));
      } catch (CancellationException e) {
        if (closed) {
          return false;
        }
        throw failDownload(
            new DatabricksSQLException(
                "APC result chunk download was cancelled",
                DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.name(),
                DatabricksDriverErrorCode.CHUNK_DOWNLOAD_ERROR.getCode(),
                e));
      }
    }
    synchronized (stateLock) {
      if (closed) {
        return false;
      }
      chunkCount++;
      return true;
    }
  }

  private DatabricksSQLException failDownload(DatabricksSQLException error) {
    synchronized (stateLock) {
      if (terminalFailure == null) {
        terminalFailure = error;
      }
      return terminalFailure;
    }
  }

  private void addPage(ResultData page) throws DatabricksSQLException {
    List<ArrowBatch> batches = page.getArrowBatches();
    Collection<ExternalLink> links = page.getExternalLinks();
    ArrayDeque<ArrowResultChunk> staged = new ArrayDeque<>();
    long inlineRows = 0;
    long pageRows = 0;
    Long continuation;
    long offset;
    synchronized (stateLock) {
      offset = nextRowOffset;
    }
    try {
      if (batches != null && !batches.isEmpty()) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
          stream.write(schemaMessage);
          for (ArrowBatch batch : batches) {
            if (batch.getBatch() == null
                || batch.getRowCount() == null
                || batch.getRowCount() < 0) {
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
        if (inlineRows > 0) {
          try {
            staged.add(
                ArrowResultChunk.builder()
                    .withStatementId(statementId)
                    .withInputStream(new ByteArrayInputStream(stream.toByteArray()), inlineRows)
                    .build());
          } catch (DatabricksParsingException e) {
            throw new DatabricksParsingException(
                "Unable to parse APC inline Arrow batch",
                e,
                DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR.name(),
                DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR.getCode());
          }
        }
      }
      pageRows += inlineRows;
      if (links != null && !links.isEmpty()) {
        if (!session.getConnectionContext().isCloudFetchEnabled()) {
          throw invalidPage("APC returned external links when result download is disabled");
        }
        List<ExternalLink> ordered = new ArrayList<>(links);
        for (ExternalLink link : ordered) {
          if (link.getChunkIndex() == null
              || link.getRowCount() == null
              || link.getRowCount() < 0
              || link.getRowOffset() == null) {
            throw invalidPage("APC external link is missing paging metadata");
          }
          if (link.getRowCount() > 0) {
            ArrowResultChunk chunk =
                ArrowResultChunk.builder()
                    .withStatementId(statementId)
                    .withConnectionContext(session.getConnectionContext())
                    .withChunkMetadata(
                        link.getChunkIndex(), link.getRowCount(), link.getRowOffset())
                    .build();
            staged.add(chunk);
            chunk.setChunkLink(link);
          }
          pageRows += link.getRowCount();
        }
        ExternalLink last = ordered.get(ordered.size() - 1);
        continuation = last.getNextChunkIndex();
        offset = last.getRowOffset() + last.getRowCount();
      } else {
        continuation = page.getNextChunkIndex();
        offset += inlineRows;
      }
      synchronized (stateLock) {
        if (!closed) {
          pending.addAll(staged);
          rowCount += pageRows;
          nextChunkIndex = continuation;
          nextRowOffset = offset;
          return;
        }
      }
    } catch (DatabricksSQLException | RuntimeException e) {
      for (ArrowResultChunk chunk : staged) {
        chunk.releaseChunkAndAllocator();
      }
      throw e;
    }
    for (ArrowResultChunk chunk : staged) {
      chunk.releaseChunkAndAllocator();
    }
  }

  private static DatabricksParsingException invalidPage(String message) {
    DatabricksDriverErrorCode code = DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR;
    return new DatabricksParsingException(message, null, code.name(), code.getCode());
  }

  @Override
  public ArrowResultChunk getChunk() {
    synchronized (stateLock) {
      return current;
    }
  }

  @Override
  public void close() {
    ArrowResultChunk active;
    ArrayDeque<ArrowResultChunk> abandoned;
    synchronized (stateLock) {
      if (closed) {
        return;
      }
      closed = true;
      active = current;
      current = null;
      abandoned = new ArrayDeque<>(pending);
      pending.clear();
    }
    downloadExecutor.shutdownNow();
    try {
      downloadExecutor.awaitTermination(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    synchronized (stateLock) {
      // An active worker owns its chunk until its finally block. Inline chunks and queued or
      // completed downloads have no worker to perform cleanup, even if close was interrupted.
      if (active != null && active != downloadWorkerChunk) {
        active.releaseChunkAndAllocator();
      }
    }
    for (ArrowResultChunk chunk : abandoned) {
      chunk.releaseChunkAndAllocator();
    }
  }

  @Override
  public long getRowCount() {
    synchronized (stateLock) {
      return rowCount;
    }
  }

  @Override
  public long getChunkCount() {
    synchronized (stateLock) {
      return chunkCount;
    }
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
