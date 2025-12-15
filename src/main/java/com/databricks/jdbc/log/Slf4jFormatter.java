package com.databricks.jdbc.log;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * A custom {@link Formatter} implementation that formats log records in the usual SLF4J format.
 * This is used by {@link JulLogger} to maintain consistency with SLF4J logging.
 */
public class Slf4jFormatter extends Formatter {

  private static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z").withZone(ZoneId.systemDefault());

  /** {@inheritDoc} */
  @Override
  public String format(LogRecord record) {
    String timestamp = DATE_FORMATTER.format(Instant.ofEpochMilli(record.getMillis()));
    String level = record.getLevel().getLocalizedName();
    String className = record.getSourceClassName();
    String methodName = record.getSourceMethodName();
    String message = formatMessage(record);

    return String.format("%s %s %s#%s - %s%n", timestamp, level, className, methodName, message);
  }
}
