package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.sql.SQLException;

/**
 * Indicates that a native batch succeeded but its JDBC update counts could not be read.
 *
 * <p>This is intentionally not a {@code BatchUpdateException}: backend execution did not fail.
 */
class NativeBatchResultException extends DatabricksSQLException {

  NativeBatchResultException(SQLException cause) {
    super(
        "Native batch execution succeeded, but JDBC update counts could not be read. "
            + "Inserted rows may already be committed. Cause: "
            + cause.getMessage(),
        cause,
        DatabricksDriverErrorCode.RESULT_SET_ERROR);
  }
}
