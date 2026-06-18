package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.buildProceduresSQL;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CommandConstantsTest {

  @Test
  @DisplayName("Should escape backticks in information schema catalog prefix")
  void shouldEscapeBackticksInInformationSchemaCatalogPrefix() {
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();

    String sql = buildProceduresSQL("cat`alog", null, null, params);

    assertTrue(sql.contains(" FROM `cat``alog`.information_schema.routines"));
  }
}
