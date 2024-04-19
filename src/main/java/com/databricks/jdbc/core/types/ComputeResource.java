package com.databricks.jdbc.core.types;

import com.databricks.jdbc.client.DatabricksClientType;

public interface ComputeResource {

  DatabricksClientType getClientType();
}
