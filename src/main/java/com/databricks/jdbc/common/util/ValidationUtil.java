package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;

import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.http.HttpResponse;

public class ValidationUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ValidationUtil.class);

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
        LOGGER.debug("Field %s is null", field.getKey());
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
        "Unable to fetch HTTP response successfully. " + errorMessage,
        DEFAULT_HTTP_EXCEPTION_SQLSTATE);
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

    // check if URL matches the generic format
    if (!JDBC_URL_PATTERN.matcher(url).matches()) {
      return false;
    }

    // check if path in URL matches any of the specific patterns
    return PATH_PATTERNS.stream().anyMatch(pattern -> pattern.matcher(url).matches());
  }
}
