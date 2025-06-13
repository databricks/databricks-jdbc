package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.util.stream.Stream;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ValidationUtilTest {
  @Mock StatusLine statusLine;
  @Mock HttpResponse response;

  @Test
  void testCheckIfPositive() {
    assertDoesNotThrow(() -> ValidationUtil.checkIfNonNegative(10, "testField"));
    assertThrows(
        DatabricksSQLException.class, () -> ValidationUtil.checkIfNonNegative(-10, "testField"));
  }

  @Test
  void testSuccessfulResponseCheck() {
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    assertDoesNotThrow(() -> ValidationUtil.checkHTTPError(response));

    when(statusLine.getStatusCode()).thenReturn(202);
    assertDoesNotThrow(() -> ValidationUtil.checkHTTPError(response));
  }

  @Test
  void testUnsuccessfulResponseCheck() {
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(400);
    when(statusLine.toString()).thenReturn("mockStatusLine");
    Throwable exception =
        assertThrows(DatabricksHttpException.class, () -> ValidationUtil.checkHTTPError(response));
    assertEquals(
        "HTTP request failed by code: 400, status line: mockStatusLine.", exception.getMessage());

    when(statusLine.getStatusCode()).thenReturn(102);
    assertThrows(DatabricksHttpException.class, () -> ValidationUtil.checkHTTPError(response));
  }

  @ParameterizedTest
  @MethodSource("jdbcUrlValidityTestCases")
  void testIsValidJdbcUrl(String url, String description, boolean expectedValid) {
    assertEquals(expectedValid, ValidationUtil.isValidJdbcUrl(url), description);
  }

  private static Stream<Arguments> jdbcUrlValidityTestCases() {
    return Stream.of(
        Arguments.of(VALID_URL_1, "Valid URL with auth_flow=2 and log path", true),
        Arguments.of(VALID_URL_2, "Valid URL with invalid LogLevel but valid structure", true),
        Arguments.of(VALID_URL_3, "Valid URL with EnableQueryResultLZ4Compression=0", true),
        Arguments.of(VALID_URL_4, "Valid URL with EnableDirectResults", true),
        Arguments.of(VALID_URL_5, "Valid URL without schema", true),
        Arguments.of(VALID_URL_6, "Valid URL with ConnCatalog and ConnSchema", true),
        Arguments.of(VALID_URL_7, "Valid URL with Arrow disabled", true),
        Arguments.of(VALID_BASE_URL_1, "Valid base URL with trailing semicolon", true),
        Arguments.of(VALID_BASE_URL_2, "Valid base URL without trailing semicolon", true),
        Arguments.of(VALID_BASE_URL_3, "Valid base URL without schema", true),
        Arguments.of(VALID_BASE_URL_4, "Valid base URL with one parameter", true),
        Arguments.of(VALID_BASE_URL_5, "Valid base URL with two parameters", true),
        Arguments.of(VALID_TEST_URL, "Minimal valid test URL", true),
        Arguments.of(VALID_CLUSTER_URL, "Valid cluster URL with protocol path", true),
        Arguments.of(
            VALID_URL_WITH_INVALID_COMPRESSION_TYPE,
            "Valid URL with invalid compression type",
            true),
        Arguments.of(INVALID_URL_1, "Invalid non-Databricks JDBC URL", false),
        Arguments.of(INVALID_URL_2, "Invalid malformed JDBC scheme", false));
  }
}
