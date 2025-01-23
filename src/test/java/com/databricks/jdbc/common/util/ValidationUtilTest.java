package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

  @Test
  public void testIsValidJdbcUrl() {
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_URL_1));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_URL_2));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_URL_3));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_URL_4));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_URL_5));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_URL_6));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_URL_7));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_BASE_URL_1));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_BASE_URL_2));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_BASE_URL_3));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_TEST_URL));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_CLUSTER_URL));
    assertTrue(ValidationUtil.isValidJdbcUrl(VALID_URL_WITH_INVALID_COMPRESSION_TYPE));
    assertFalse(ValidationUtil.isValidJdbcUrl(INVALID_URL_1));
    assertFalse(ValidationUtil.isValidJdbcUrl(INVALID_URL_2));
    assertFalse(ValidationUtil.isValidJdbcUrl(INVALID_CLUSTER_URL));
  }
}
