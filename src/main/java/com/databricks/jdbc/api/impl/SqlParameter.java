package com.databricks.jdbc.api.impl;

import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
public interface SqlParameter {

  @Nullable
  Object value();

  ColumnInfoTypeName type();

  int cardinal();
}
