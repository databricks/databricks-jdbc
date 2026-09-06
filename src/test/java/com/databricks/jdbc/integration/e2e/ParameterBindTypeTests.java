package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ParameterBindTypeTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  /**
   * PARAMQUERY-021: the declared SQL target type — not the bound value's native type — must drive
   * the wire type. Per the JDBC spec (Appendix B), Types.FLOAT is a synonym for DOUBLE (8-byte
   * double precision) while Types.REAL is 4-byte single precision. Binding the same textual value
   * against Types.FLOAT vs Types.REAL must therefore yield differently-typed result columns for a
   * bare marker projection.
   */
  @Test
  void testTargetTypeDrivesWireTypeAcrossScalarTypes() throws SQLException {
    double value = 1234567.89012345d;

    // Types.FLOAT must ride as the 8-byte DOUBLE per JDBC spec.
    try (PreparedStatement stmt = connection.prepareStatement("SELECT ? AS v")) {
      stmt.setObject(1, value, Types.FLOAT);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "expected exactly 1 row");
        assertEquals(
            Types.DOUBLE,
            rs.getMetaData().getColumnType(1),
            "Types.FLOAT bind must ride as the 8-byte DOUBLE per JDBC spec Appendix B");
        assertEquals(
            value, rs.getDouble(1), 0.0d, "DOUBLE bind must round-trip at double precision");
        assertFalse(rs.next(), "expected exactly 1 row");
      }
    }

    // Types.REAL must ride as the 4-byte FLOAT, distinct from Types.FLOAT.
    try (PreparedStatement stmt = connection.prepareStatement("SELECT ? AS v")) {
      stmt.setObject(1, value, Types.REAL);
      try (ResultSet rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "expected exactly 1 row");
        assertEquals(
            Types.FLOAT,
            rs.getMetaData().getColumnType(1),
            "Types.REAL bind must ride as the 4-byte FLOAT per JDBC spec Appendix B");
        assertFalse(rs.next(), "expected exactly 1 row");
      }
    }
  }
}
