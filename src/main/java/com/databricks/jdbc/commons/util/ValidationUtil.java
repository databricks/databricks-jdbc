package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidationUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationUtil.class);

  public static void checkIfPositive(int number, String fieldName) throws DatabricksSQLException {
    // Todo : Add appropriate exception
    if (number < 0) {
      throw new DatabricksSQLException(
          String.format("Invalid input for %s, : %d", fieldName, number));
    }
  }

  private static boolean checkEmptyOrWildcardValidation(
      String fieldValue, String context, String fieldName) {
    if (WildcardUtil.isNullOrEmpty(fieldValue) || WildcardUtil.isWildcard(fieldValue)) {
      String reason = WildcardUtil.isNullOrEmpty(fieldValue) ? "empty or null" : "a wildcard";
      LOGGER.error(
          "Field {} failed validation. Reason : {}. Context : {}", fieldName, reason, context);
      return true;
    }
    return false;
  }

  public static void throwErrorIfEmptyOrWildcard(Map<String, String> fields, String context)
      throws DatabricksSQLFeatureNotSupportedException {
    for (Map.Entry<String, String> field : fields.entrySet()) {
      if (checkEmptyOrWildcardValidation(field.getValue(), context, field.getKey())) {
        throw new DatabricksSQLFeatureNotSupportedException(
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

  public static boolean isValidSessionConfig(String key) {
    return DatabricksJdbcConstants.ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.containsKey(key);
  }
}
