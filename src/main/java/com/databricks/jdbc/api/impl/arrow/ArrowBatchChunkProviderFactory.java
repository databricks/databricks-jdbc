package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.ArrowUtil.createArrowBatchByteStream;
import static com.databricks.jdbc.common.util.DecompressionUtil.decompress;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ArrowBatch;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Builds chunk providers for APC native Arrow record batches. */
final class ArrowBatchChunkProviderFactory {
  private ArrowBatchChunkProviderFactory() {}

  static ChunkProvider create(ResultData resultData, ResultManifest resultManifest)
      throws DatabricksSQLException {
    return create(resultData, resultManifest, null, null);
  }

  /**
   * Creates a provider for an inline ARROW_BATCH result. Results that span several pages are read
   * lazily through {@code session}, one page per chunk.
   */
  static ChunkProvider create(
      ResultData resultData,
      ResultManifest resultManifest,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    if (resultData == null) {
      throw parsingError("ARROW_BATCH response is missing result data");
    }
    if (resultManifest == null) {
      throw parsingError("ARROW_BATCH response is missing its manifest");
    }
    validatePageShape(resultData);

    if (hasMorePages(resultData)
        || (resultManifest.getTotalChunkCount() != null
            && resultManifest.getTotalChunkCount() > 1)) {
      if (statementId == null || session == null) {
        throw parsingError("Paginated ARROW_BATCH responses require a session to fetch pages");
      }
      return new PagedArrowBatchChunkProvider(resultData, resultManifest, statementId, session);
    }

    List<ArrowBatch> arrowBatches = normalizeArrowBatches(resultData, resultManifest, true);
    long totalRows = validateAndCountRows(arrowBatches, resultData);
    if (resultManifest.getTotalRowCount() != null
        && resultManifest.getTotalRowCount() != totalRows) {
      throw parsingError(
          String.format(
              "Arrow batch row_count total %d does not match manifest total_row_count %d",
              totalRows, resultManifest.getTotalRowCount()));
    }
    ByteArrayInputStream stream = toArrowStream(arrowBatches, resultManifest);
    try {
      InlineChunkProvider provider = new InlineChunkProvider(stream, totalRows);
      if (totalRows > 0) {
        return provider;
      }
      provider.close();
      return new EmptyChunkProvider();
    } catch (RuntimeException e) {
      throw nativeArrowParsingError(e);
    }
  }

  /** Decodes one inline page into a chunk holding exactly that page's rows. */
  static ArrowResultChunk decodePage(ResultData page, ResultManifest resultManifest)
      throws DatabricksSQLException {
    validatePageShape(page);
    List<ArrowBatch> arrowBatches = normalizeArrowBatches(page, resultManifest, false);
    long pageRows = validateAndCountRows(arrowBatches, page);
    ByteArrayInputStream stream = toArrowStream(arrowBatches, resultManifest);
    try {
      return ArrowResultChunk.builder().withInputStream(stream, pageRows).build();
    } catch (RuntimeException e) {
      throw nativeArrowParsingError(e);
    }
  }

  static boolean hasMorePages(ResultData resultData) {
    return resultData.getNextChunkIndex() != null;
  }

  private static void validatePageShape(ResultData resultData) throws DatabricksParsingException {
    if (resultData.getExternalLinks() != null && !resultData.getExternalLinks().isEmpty()) {
      throw parsingError("External-link ARROW_BATCH responses are not supported");
    }
    if (resultData.getNextChunkInternalLink() != null && resultData.getNextChunkIndex() == null) {
      throw parsingError("ARROW_BATCH pagination requires next_chunk_index");
    }
  }

  private static ByteArrayInputStream toArrowStream(
      List<ArrowBatch> arrowBatches, ResultManifest resultManifest) throws DatabricksSQLException {
    List<ArrowBatch> decompressed =
        decompressArrowBatches(arrowBatches, resultManifest.getResultCompression());
    return createArrowBatchByteStream(resultManifest.getArrowSchema(), decompressed);
  }

  private static long validateAndCountRows(List<ArrowBatch> arrowBatches, ResultData resultData)
      throws DatabricksParsingException {
    long rowCount = 0;
    for (int index = 0; index < arrowBatches.size(); index++) {
      ArrowBatch arrowBatch = arrowBatches.get(index);
      if (arrowBatch == null
          || arrowBatch.getBatch() == null
          || arrowBatch.getBatch().length == 0) {
        throw parsingError("Arrow batch " + index + " is missing batch bytes");
      }
      if (arrowBatch.getRowCount() == null || arrowBatch.getRowCount() < 0) {
        throw parsingError("Arrow batch " + index + " has an invalid row_count");
      }
      try {
        rowCount = Math.addExact(rowCount, arrowBatch.getRowCount());
      } catch (ArithmeticException e) {
        throw parsingError("Arrow batch row_count total overflowed");
      }
    }

    if (resultData.getRowCount() != null && resultData.getRowCount() != rowCount) {
      throw parsingError(
          String.format(
              "Arrow batch row_count total %d does not match result row_count %d",
              rowCount, resultData.getRowCount()));
    }
    return rowCount;
  }

  private static List<ArrowBatch> decompressArrowBatches(
      List<ArrowBatch> arrowBatches, CompressionCodec compressionCodec)
      throws DatabricksSQLException {
    if (compressionCodec == null || compressionCodec == CompressionCodec.NONE) {
      return arrowBatches;
    }

    List<ArrowBatch> decompressedBatches = new ArrayList<>(arrowBatches.size());
    for (int index = 0; index < arrowBatches.size(); index++) {
      ArrowBatch arrowBatch = arrowBatches.get(index);
      decompressedBatches.add(
          new ArrowBatch()
              .setBatch(
                  decompress(
                      arrowBatch.getBatch(), compressionCodec, "Native Arrow batch " + index))
              .setRowCount(arrowBatch.getRowCount()));
    }
    return decompressedBatches;
  }

  /**
   * Returns the page's batches, treating omitted batches as empty only when the page (or, for a
   * single-page result, the manifest) reports zero rows.
   */
  private static List<ArrowBatch> normalizeArrowBatches(
      ResultData resultData, ResultManifest resultManifest, boolean isSinglePage)
      throws DatabricksParsingException {
    List<ArrowBatch> arrowBatches = resultData.getArrowBatches();
    if (arrowBatches != null) {
      return arrowBatches;
    }

    Long resultRowCount = resultData.getRowCount();
    if (Long.valueOf(0L).equals(resultRowCount)
        || (isSinglePage
            && resultRowCount == null
            && Long.valueOf(0L).equals(resultManifest.getTotalRowCount()))) {
      return Collections.emptyList();
    }
    throw parsingError("ARROW_BATCH response is missing arrow_batches");
  }

  private static DatabricksParsingException nativeArrowParsingError(RuntimeException e) {
    return new DatabricksParsingException(
        "Failed to parse native Arrow batches: " + e.getMessage(),
        e,
        DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
  }

  static DatabricksParsingException parsingError(String message) {
    return new DatabricksParsingException(
        message, DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
  }
}
