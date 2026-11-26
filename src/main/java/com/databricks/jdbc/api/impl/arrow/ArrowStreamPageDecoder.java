package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.util.DecompressionUtil.decompressLazy;

import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.io.InputStream;

/** Decodes a complete Arrow IPC stream from a SEA result attachment. */
final class ArrowStreamPageDecoder implements PageDecoder {
  private final CompressionCodec compressionCodec;

  ArrowStreamPageDecoder(CompressionCodec compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  @Override
  public ArrowResultChunk decode(ResultData page) throws DatabricksSQLException {
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
