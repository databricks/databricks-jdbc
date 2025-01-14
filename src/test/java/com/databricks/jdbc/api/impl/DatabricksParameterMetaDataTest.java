package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.TEST_STRING;
import static java.sql.ParameterMetaData.parameterModeIn;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import java.sql.SQLException;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DatabricksParameterMetaDataTest {
  private DatabricksParameterMetaData metaData;

  @BeforeEach
  public void setUp() {
    metaData = new DatabricksParameterMetaData();
    metaData.put(
        1,
        ImmutableSqlParameter.builder()
            .type(ColumnInfoTypeName.STRING)
            .cardinal(1)
            .value(TEST_STRING)
            .build());
    metaData.put(
        2,
        ImmutableSqlParameter.builder()
            .type(ColumnInfoTypeName.INT)
            .cardinal(1)
            .value(123)
            .build());
  }

  @Test
  public void testInitialization() {
    DatabricksParameterMetaData newMetadata = new DatabricksParameterMetaData();
    assertTrue(newMetadata.getParameterBindings().isEmpty());
    assertEquals(2, metaData.getParameterBindings().size());
  }

  @Test
  public void testClear() {
    metaData.clear();
    assertTrue(metaData.getParameterBindings().isEmpty());
  }

  @Test
  public void testGetParameterMode() throws SQLException {
    assertEquals(parameterModeIn, metaData.getParameterMode(1));
  }

  @Test
  public void testGetParameterClassName() throws SQLException {
    assertEquals("java.lang.String", metaData.getParameterClassName(1));
    assertEquals("java.lang.Integer", metaData.getParameterClassName(2));
  }

  @Test
  public void testGetParameterTypeName() throws SQLException {
    assertEquals("STRING", metaData.getParameterTypeName(1));
    assertEquals("INT", metaData.getParameterTypeName(2));
  }

  @Test
  public void testGetParameterType() throws SQLException {
    assertEquals(Types.VARCHAR, metaData.getParameterType(1));
    assertEquals(metaData.getParameterType(2), Types.INTEGER);
  }
}
