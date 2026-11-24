package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DecompressionUtil.decompressLazy;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.InputStream;

/** Reads SEA inline Arrow-stream pages with bounded prefetch. */
class SeaInlineArrowChunkProvider extends PagedArrowChunkProvider {
  private static final String WORKER_NAME = "sea-inline-prefetcher";

  SeaInlineArrowChunkProvider(
      ResultData firstPage,
      ResultManifest resultManifest,
      StatementId statementId,
      IDatabricksSession session)
      throws DatabricksSQLException {
    super(
        firstPage,
        statementId,
        session,
        page -> decodePage(page, resultManifest.getResultCompression()),
        WORKER_NAME);
  }

  private static ArrowResultChunk decodePage(ResultData page, CompressionCodec compressionCodec)
      throws DatabricksSQLException {
    byte[] attachment = page.getAttachment();
    if (attachment == null || attachment.length == 0) {
      throw new DatabricksSQLException(
          "No inline Arrow data (attachment) in result",
          DatabricksDriverErrorCode.RESULT_SET_ERROR);
    }
    InputStream decompressedStream =
        decompressLazy(attachment, compressionCodec, "SEA inline Arrow chunk decompression");
    long rowCount = page.getRowCount() == null ? 0 : page.getRowCount();
    return ArrowResultChunk.builder().withInputStream(decompressedStream, rowCount).build();
  }
}
