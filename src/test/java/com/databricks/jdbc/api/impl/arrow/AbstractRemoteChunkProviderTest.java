package com.databricks.jdbc.api.impl.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

public class AbstractRemoteChunkProviderTest {

  @Test
  void typedChunkFailureIsPreserved() {
    DatabricksSQLException typedFailure =
        new DatabricksSQLException(
            "Arrow parsing failed", DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);

    DatabricksSQLException result =
        AbstractRemoteChunkProvider.createChunkReadyException(typedFailure);

    assertSame(typedFailure, result);
  }

  @Test
  void untypedChunkFailureIsWrapped() {
    IllegalStateException cause = new IllegalStateException("unexpected failure");

    DatabricksSQLException result = AbstractRemoteChunkProvider.createChunkReadyException(cause);

    assertEquals(DatabricksDriverErrorCode.CHUNK_READY_ERROR.name(), result.getSQLState());
    assertSame(cause, result.getCause());
  }

  @Test
  void timeoutIsPreservedAsCause() {
    TimeoutException timeout = new TimeoutException("chunk was not ready");

    DatabricksSQLException result = AbstractRemoteChunkProvider.createChunkReadyException(timeout);

    assertEquals(DatabricksDriverErrorCode.CHUNK_READY_ERROR.name(), result.getSQLState());
    assertSame(timeout, result.getCause());
  }
}
