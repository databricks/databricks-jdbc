package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ResultData;

@FunctionalInterface
interface PageDecoder {
  ArrowResultChunk decode(ResultData page) throws DatabricksSQLException;

  default long getRowCount(ResultData page, ArrowResultChunk chunk) {
    return page.getRowCount() == null ? 0 : page.getRowCount();
  }
}
