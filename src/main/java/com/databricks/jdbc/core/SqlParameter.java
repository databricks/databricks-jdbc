package com.databricks.jdbc.core;

import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface SqlParameter {

  @Nullable
  Object value();

  String type();

  int cardinal();
}
