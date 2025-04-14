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
      String errorMessage = String.format("Invalid input for %s, : %d", fieldName, number);
      LOGGER.error(errorMessage);
      throw new DatabricksValidationException(errorMessage);
    }
  }

  public static void throwErrorIfNull(Map<String, String> fields, String context)
      throws DatabricksSQLException {
    for (Map.Entry<String, String> field : fields.entrySet()) {
      if (field.getValue() == null) {
        String errorMessage =
            String.format(
                "Unsupported null Input for field {%s}. Context: {%s}", field.getKey(), context);
        LOGGER.error(errorMessage);
        throw new DatabricksValidationException(errorMessage);
      }
    }
  }

  public static void throwErrorIfNull(String field, Object value) throws DatabricksSQLException {
    if (value != null) {
      return;
    }
    String errorMessage = String.format("Unsupported null Input for field {%s}", field);
    LOGGER.error(errorMessage);
    throw new DatabricksValidationException(errorMessage);
  }

  public static void checkHTTPError(HttpResponse response) throws DatabricksHttpException {
    int statusCode = response.getStatusLine().getStatusCode();
    String statusLine = response.getStatusLine().toString();
    if (statusCode >= 200 && statusCode < 300) {
      return;
    }
    String errorReason =
        String.format("HTTP request failed by code: %d, status line: %s.", statusCode, statusLine);
    if (response.containsHeader(THRIFT_ERROR_MESSAGE_HEADER)) {
      errorReason +=
          String.format(
              "Thrift Header : %s",
              response.getFirstHeader(THRIFT_ERROR_MESSAGE_HEADER).getValue());
    }
    LOGGER.error(errorReason);
    throw new DatabricksHttpException(errorReason, DEFAULT_HTTP_EXCEPTION_SQLSTATE);
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
