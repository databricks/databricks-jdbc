package com.databricks.jdbc.api.impl.batch;

import org.immutables.value.Value;

/**
 * The {@code BatchCommand} class represents a single SQL command in a batch execution. It
 * encapsulates the SQL command string.
 */
@Value.Immutable
public interface BatchCommand {
  /** Returns the SQL command string. */
  String getSql();
}
