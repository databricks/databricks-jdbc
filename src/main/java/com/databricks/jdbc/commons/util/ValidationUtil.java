package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.DatabricksValidationException;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ValidationUtil {
  private static final Logger LOGGER = LogManager.getLogger(ValidationUtil.class);

  public static void checkIfNonNegative(int number, String fieldName)
      throws DatabricksSQLException {
    // Todo : Add appropriate exception
    if (number < 0) {
      throw new DatabricksSQLException(
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
    LOGGER.debug("Response has failure HTTP Code");
    String errorMessage =
        String.format("HTTP request failed by code: %d, status line: %s", statusCode, statusLine);
    throw new DatabricksHttpException(
        "Unable to fetch HTTP response successfully. " + errorMessage);
  }
}
