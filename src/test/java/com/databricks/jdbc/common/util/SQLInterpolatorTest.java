package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static com.databricks.jdbc.api.impl.DatabricksPreparedStatementTest.getSqlParam;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.databricks.jdbc.api.impl.ImmutableSqlParameter;
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SQLInterpolatorTest {

  @Test
  public void testInterpolateSQLWithStrings() throws DatabricksValidationException {
    String sql = "SELECT * FROM users WHERE name = ? AND city = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, "Alice", DatabricksTypeUtil.STRING));
    params.put(2, getSqlParam(2, "Wonderland", DatabricksTypeUtil.STRING));
    String expected = "SELECT * FROM users WHERE name = 'Alice' AND city = 'Wonderland'";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testInterpolateSQLWithMixedTypes() throws DatabricksValidationException {
    String sql = "INSERT INTO sales (id, amount, active) VALUES (?, ?, ?)";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 101, DatabricksTypeUtil.INT));
    params.put(2, getSqlParam(2, 19.95, DatabricksTypeUtil.FLOAT));
    params.put(3, getSqlParam(3, true, DatabricksTypeUtil.BOOLEAN));
    String expected = "INSERT INTO sales (id, amount, active) VALUES (101, 19.95, true)";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testInterpolateSQLWithNullValues() throws DatabricksValidationException {
    String sql = "UPDATE products SET price = ? WHERE id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, null, DatabricksTypeUtil.NULL));
    params.put(2, getSqlParam(2, 200, DatabricksTypeUtil.INT));
    String expected = "UPDATE products SET price = NULL WHERE id = 200";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }

  @Test
  public void testParameterMismatch() {
    String sql = "DELETE FROM log WHERE date = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>(); // no parameters added
    assertThrows(
        DatabricksValidationException.class,
        () -> {
          SQLInterpolator.interpolateSQL(sql, params);
        });
  }

  @Test
  public void testExtraParameters() {
    String sql = "SELECT * FROM clients WHERE client_id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, 300, DatabricksTypeUtil.INT));
    params.put(2, getSqlParam(2, TEST_STRING, DatabricksTypeUtil.STRING)); // extra parameter
    assertThrows(
        DatabricksValidationException.class,
        () -> {
          SQLInterpolator.interpolateSQL(sql, params);
        });
  }

  @Test
  public void testEscapedValues() throws DatabricksValidationException {
    String sql = "UPDATE products SET price = ? WHERE id = ?";
    Map<Integer, ImmutableSqlParameter> params = new HashMap<>();
    params.put(1, getSqlParam(1, "O'Reilly", DatabricksTypeUtil.STRING));
    params.put(2, getSqlParam(2, 200, DatabricksTypeUtil.INT));
    String expected = "UPDATE products SET price = 'O''Reilly' WHERE id = 200";
    assertEquals(expected, SQLInterpolator.interpolateSQL(sql, params));
  }
}
