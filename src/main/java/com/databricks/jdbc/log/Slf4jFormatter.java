package com.databricks.jdbc.log;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

/**
 * A custom {@link Formatter} implementation that formats log records in the usual SLF4J format.
 * This is used by {@link JulLogger} to maintain consistency with SLF4J logging.
 */
public class Slf4jFormatter extends Formatter {

  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private static final SimpleDateFormat dateFormat;

  static {
    dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  /** {@inheritDoc} */
  @Override
  public String format(LogRecord record) {
    String timestamp = dateFormat.format(new Date(record.getMillis()));
    String level = record.getLevel().getLocalizedName();
    String className = record.getSourceClassName();
    String methodName = record.getSourceMethodName();
    String message = formatMessage(record);

    return String.format("%s %s %s#%s - %s%n", timestamp, level, className, methodName, message);
  }
}
