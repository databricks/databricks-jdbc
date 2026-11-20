package com.databricks.jdbc.model.client.sqlexec;

import com.fasterxml.jackson.annotation.JsonInclude;

/** APC requests omit absent bindings while warehouse requests retain their existing wire shape. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApcExecuteStatementRequest extends ExecuteStatementRequest {}
