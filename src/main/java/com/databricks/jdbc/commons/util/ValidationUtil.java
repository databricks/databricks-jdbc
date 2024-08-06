package com.databricks.jdbc.commons.util;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.DatabricksValidationException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
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

  /**
   * Validates the JDBC URL.
   *
   * @param url JDBC URL
   * @return true if the URL is valid, false otherwise
   */
  public static boolean isValidJdbcUrl(String url) {
    final List<Pattern> PATH_PATTERNS =
        List.of(
            HTTP_CLUSTER_PATH_PATTERN,
            HTTP_WAREHOUSE_PATH_PATTERN,
            HTTP_ENDPOINT_PATH_PATTERN,
            TEST_PATH_PATTERN,
            BASE_PATTERN,
            HTTP_CLI_PATTERN);

    if (!JDBC_URL_PATTERN.matcher(url).matches()) {
      return false;
    }

    return PATH_PATTERNS.stream().anyMatch(pattern -> pattern.matcher(url).matches());
  }
}
