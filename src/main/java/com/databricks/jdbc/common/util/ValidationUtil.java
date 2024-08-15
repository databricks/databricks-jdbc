package com.databricks.jdbc.common.util;

import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.util.Map;
import org.apache.http.HttpResponse;

public class ValidationUtil {

  public static void checkIfNonNegative(int number, String fieldName)
      throws DatabricksSQLException {
    if (number < 0) {
      throw new DatabricksValidationException(
          String.format("Invalid input for %s, : %d", fieldName, number));
    }
  }

  public static void throwErrorIfNull(Map<String, String> fields, String context)
      throws DatabricksSQLException {
    for (Map.Entry<String, String> field : fields.entrySet()) {
      if (field.getValue() == null) {
        throw new DatabricksValidationException(
            String.format(
                "Unsupported Input for field {%s}. Context: {%s}", field.getKey(), context));
      }
    }
  }

  public static void checkHTTPError(HttpResponse response) throws DatabricksHttpException {
    int statusCode = response.getStatusLine().getStatusCode();
    String statusLine = response.getStatusLine().toString();
    if (statusCode >= 200 && statusCode < 300) {
      return;
    }
    LoggingUtil.log(LogLevel.DEBUG, "Response has failure HTTP Code");
    String thriftErrorHeader = "X-Thriftserver-Error-Message";
    if (response.containsHeader(thriftErrorHeader)) {
      String errorMessage = response.getFirstHeader(thriftErrorHeader).getValue();
      throw new DatabricksHttpException(
          "HTTP Response code: "
              + response.getStatusLine().getStatusCode()
              + ", Error message: "
              + errorMessage);
    }
    String errorMessage =
        String.format("HTTP request failed by code: %d, status line: %s", statusCode, statusLine);
    throw new DatabricksHttpException(
        "Unable to fetch HTTP response successfully. " + errorMessage);
  }
}
