package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.core.DatabricksSQLException;
import java.io.IOException;
import java.util.Optional;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;

public class ValidationUtil {
  public static void checkIfPositive(int number, String fieldName) throws DatabricksSQLException {
    // Todo : Add appropriate exception
    if (number < 0) {
      throw new DatabricksSQLException(
          String.format("Invalid input for %s, : %d", fieldName, number));
    }
  }

  public static void ensureSuccessResponse(HttpResponse response) throws DatabricksHttpException {
    int statusCode = response.getStatusLine().getStatusCode();
    String statusLine = response.getStatusLine().toString();
    Optional<String> responseString =
        Optional.ofNullable(response.getEntity())
            .map(
                entity -> {
                  try {
                    return EntityUtils.toString(entity, "UTF-8");
                  } catch (IOException e) {
                    return null;
                  }
                });
    if (statusCode >= 200 && statusCode < 300) {
      return;
    }
    String errorMessage =
        String.format("HTTP request failed by code: %d, status line: %s", statusCode, statusLine);
    if (responseString.isPresent()) {
      errorMessage = errorMessage + String.format(" responseString: %s", responseString);
    }
    throw new DatabricksHttpException(
        "Unable to fetch HTTP response successfully. " + errorMessage);
  }
}
