package com.databricks.client.jdbc;

import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.util.DatabricksDriverPropertyUtil;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.DriverPropertyInfo;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class DriverTest {

  private static final String HOST_ONLY_URL = "jdbc:databricks://localhost:8080";
  private static final String HTTP_PATH_URL = HOST_ONLY_URL + ";httpPath=/sql/1.0/warehouses/test";

  @ParameterizedTest
  @ValueSource(strings = {"", ";UseThriftClient=1", ";UseThriftClient=0"})
  void getPropertyInfoReportsAllIndependentRequiredProperties(String options)
      throws DatabricksSQLException {
    DriverPropertyInfo[] missing =
        Driver.getInstance().getPropertyInfo(HOST_ONLY_URL + options, new Properties());

    assertEquals(2, missing.length);
    assertEquals(HTTP_PATH.getParamName(), missing[0].name);
    assertEquals(AUTH_MECH.getParamName(), missing[1].name);
    for (DriverPropertyInfo property : missing) {
      assertTrue(property.required);
      assertNotNull(property.description);
    }
  }

  @Test
  void getPropertyInfoOnlyReportsAuthMechAfterHttpPathIsSupplied() throws DatabricksSQLException {
    DriverPropertyInfo[] missing =
        Driver.getInstance().getPropertyInfo(HTTP_PATH_URL, new Properties());

    assertEquals(1, missing.length);
    assertEquals(AUTH_MECH.getParamName(), missing[0].name);
    assertTrue(missing[0].required);
  }

  @Test
  void getPropertyInfoUsesCaseInsensitiveProperties() throws DatabricksSQLException {
    Properties properties = new Properties();
    properties.setProperty("HTTPPATH", "/sql/1.0/warehouses/test");
    DriverPropertyInfo[] missing = Driver.getInstance().getPropertyInfo(HOST_ONLY_URL, properties);

    assertEquals(1, missing.length);
    assertEquals(AUTH_MECH.getParamName(), missing[0].name);
    assertTrue(missing[0].required);

    properties.setProperty("AUTHMECH", "3");
    missing = Driver.getInstance().getPropertyInfo(HOST_ONLY_URL, properties);
    assertEquals(Arrays.asList(PWD.getParamName()), requiredNames(missing));
  }

  @Test
  void getPropertyInfoRequiresPatCredentialOnlyAfterAuthMechIsSupplied()
      throws DatabricksSQLException {
    DriverPropertyInfo[] missing =
        Driver.getInstance().getPropertyInfo(HTTP_PATH_URL + ";AuthMech=3", new Properties());

    assertEquals(Arrays.asList(PWD.getParamName()), requiredNames(missing));
    assertTrue(Arrays.stream(missing).anyMatch(property -> !property.required));
  }

  @Test
  void getPropertyInfoPreservesCompleteUrlOptionalProperties() throws DatabricksSQLException {
    String completeUrl = HTTP_PATH_URL + ";AuthMech=3;PWD=test-token";
    Properties properties = new Properties();
    DriverPropertyInfo[] missing = Driver.getInstance().getPropertyInfo(completeUrl, properties);
    List<DriverPropertyInfo> expected =
        DatabricksDriverPropertyUtil.buildMissingPropertiesList(
            (DatabricksConnectionContext)
                DatabricksConnectionContextFactory.createWithoutError(completeUrl, properties));

    assertFalse(expected.isEmpty());
    assertEquals(expected.size(), missing.length);
    for (int index = 0; index < missing.length; index++) {
      assertFalse(missing[index].required);
      assertEquals(expected.get(index).name, missing[index].name);
      assertEquals(expected.get(index).value, missing[index].value);
      assertEquals(expected.get(index).description, missing[index].description);
    }
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"not-a-jdbc-url", "jdbc:other://localhost:8080"})
  void getPropertyInfoPreservesUnparseableUrlHostPrompt(String url) throws DatabricksSQLException {
    DriverPropertyInfo[] missing = Driver.getInstance().getPropertyInfo(url, new Properties());

    assertEquals(1, missing.length);
    assertEquals("host", missing[0].name);
    assertTrue(missing[0].required);
    assertNotNull(missing[0].description);
  }

  private static List<String> requiredNames(DriverPropertyInfo[] properties) {
    return Arrays.stream(properties)
        .filter(property -> property.required)
        .map(property -> property.name)
        .distinct()
        .collect(Collectors.toList());
  }

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
