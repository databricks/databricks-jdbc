package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;

@FunctionalInterface
interface PageDecoder {
  ArrowResultChunk decode(ResultData page) throws DatabricksSQLException;
}
