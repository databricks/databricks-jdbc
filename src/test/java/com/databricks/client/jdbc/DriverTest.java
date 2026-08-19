package com.databricks.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class DriverTest {

  @Test
  void connectRejectsMissingRequiredConnectionParameters() {
    DatabricksSQLException exception =
        assertThrows(
            DatabricksSQLException.class,
            () ->
                Driver.getInstance().connect("jdbc:databricks://localhost:8080", new Properties()));

    assertEquals("INPUT_VALIDATION_ERROR", exception.getSQLState());
    assertTrue(exception.getMessage().contains("httppath"));
  }
}
