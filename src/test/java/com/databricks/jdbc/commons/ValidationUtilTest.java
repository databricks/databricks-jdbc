package com.databricks.jdbc.commons;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.commons.util.ValidationUtil;
import com.databricks.jdbc.core.DatabricksSQLException;
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
  private static ValidationUtil validationUtil = new ValidationUtil();

  @Test
  void testCheckIfPositive() {
    assertDoesNotThrow(() -> ValidationUtil.checkIfPositive(10, "testField"));
    assertThrows(
        DatabricksSQLException.class, () -> validationUtil.checkIfPositive(-10, "testField"));
  }

  @Test
  void testSuccessfulResponseCheck() {
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(200);
    assertDoesNotThrow(() -> validationUtil.checkHTTPError(response));

    when(statusLine.getStatusCode()).thenReturn(202);
    assertDoesNotThrow(() -> validationUtil.checkHTTPError(response));
  }

  @Test
  void testUnsuccessfulResponseCheck() {
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(400);
    when(statusLine.toString()).thenReturn("mockStatusLine");
    Throwable exception =
        assertThrows(DatabricksHttpException.class, () -> validationUtil.checkHTTPError(response));
    assertEquals(
        "Unable to fetch HTTP response successfully. HTTP request failed by code: 400, status line: mockStatusLine",
        exception.getMessage());

    when(statusLine.getStatusCode()).thenReturn(102);
    assertThrows(DatabricksHttpException.class, () -> validationUtil.checkHTTPError(response));
  }
}
