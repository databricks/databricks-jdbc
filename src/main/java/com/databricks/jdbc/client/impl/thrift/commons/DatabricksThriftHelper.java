package com.databricks.jdbc.client.impl.thrift.commons;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

public class DatabricksThriftHelper {
  public static final TProtocolVersion PROTOCOL = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10;
  public static final List<TStatusCode> SUCCESS_STATUS_LIST =
      List.of(TStatusCode.SUCCESS_STATUS, TStatusCode.SUCCESS_WITH_INFO_STATUS);

  public static TNamespace getNamespace(String catalog, String schema) {
    return new TNamespace().setCatalogName(catalog).setSchemaName(schema);
  }

  public static String byteBufferToString(ByteBuffer buffer) {
    ByteBuffer newBuffer = buffer.duplicate(); // This is to avoid a BufferUnderflowException
    long sigBits = newBuffer.getLong();
    return new UUID(sigBits, sigBits).toString();
  }

  public static void verifySuccessStatus(TStatusCode statusCode, String errorContext)
      throws DatabricksHttpException {
    if (!SUCCESS_STATUS_LIST.contains(statusCode)) {
      throw new DatabricksHttpException("Error while receiving thrift response " + errorContext);
    }
  }
}
