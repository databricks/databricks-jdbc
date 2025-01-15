package com.databricks.jdbc.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.logging.*;
import java.util.stream.Stream;

/**
 * The {@code JulLogger} class provides an implementation of the {@link JdbcLogger} interface using
 * the Java Util Logging (JUL) framework. It supports logging messages at different levels such as
 * trace, debug, info, warn, and error, both with and without associated {@link Throwable} objects.
 *
 * <p>This class also includes a static method to initialize the logger with custom configurations
 * such as log level, log directory, log file size, and log file count. It supports logging to both
 * the console and file system based on the provided configuration.
 *
 * <p>Log messages include the name of the class and method from where the logging request was made,
 * providing a clear context for the log messages. This is achieved by analyzing the stack trace to
 * find the caller information.
 */
public class JulLogger implements JdbcLogger {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(JulLogger.class);

  private static final String DEFAULT_PACKAGE_PREFIX = "com.databricks.jdbc";

  public static final String STDOUT = "STDOUT";

  public static final String PARENT_CLASS_PREFIX = getPackagePrefix();

  public static final String DATABRICKS_LOG_FILE = "databricks_jdbc.log";

  public static final String JAVA_UTIL_LOGGING_CONFIG_FILE = "java.util.logging.config.file";

  private static final Set<String> logMethods = Set.of("debug", "error", "info", "trace", "warn");

  protected Logger logger;

  protected static volatile boolean isLoggerInitialized = false;

  /** Constructs a new {@code JulLogger} object with the specified name. */
  public JulLogger(String name) {
    this.logger = Logger.getLogger(name);
  }

  /** {@inheritDoc} */
  @Override
  public void trace(String message) {
    log(Level.FINEST, message, null);
  }

  @Override
  public void trace(String format, Object... arguments) {
    trace(String.format(format, arguments));
  }

  /** {@inheritDoc} */
  @Override
  public void debug(String message) {
    log(Level.FINE, message, null);
  }

  @Override
  public void debug(String format, Object... arguments) {
    debug(String.format(format, arguments));
  }

  /** {@inheritDoc} */
  @Override
  public void info(String message) {
    log(Level.INFO, message, null);
  }

  @Override
  public void info(String format, Object... arguments) {
    info(String.format(format, arguments));
  }

  /** {@inheritDoc} */
  @Override
  public void warn(String message) {
    log(Level.WARNING, message, null);
  }

  @Override
  public void warn(String format, Object... arguments) {
    warn(String.format(format, arguments));
  }

  /** {@inheritDoc} */
  @Override
  public void error(String message) {
    log(Level.SEVERE, message, null);
  }

  @Override
  public void error(String format, Object... arguments) {
    error(String.format(format, arguments));
  }

  /** {@inheritDoc} */
  @Override
  public void error(Throwable throwable, String message) {
    log(Level.SEVERE, message, throwable);
  }

  @Override
  public void error(Throwable throwable, String format, Object... arguments) {
    error(String.format(format, arguments), throwable);
  }

  /**
   * Initializes the logger with the specified configuration. This method is synchronized to prevent
   * concurrent modifications to the logger configuration.
   *
   * @param level the log level
   * @param logDir the directory for log files or {@code STDOUT} for console output
   * @param logFileSizeBytes the maximum size of a single log file in bytes
   * @param logFileCount the number of log files to rotate
   * @throws IOException if an I/O error occurs
   */
  public static synchronized void initLogger(
      Level level, String logDir, int logFileSizeBytes, int logFileCount) throws IOException {
    if (!isLoggerInitialized) {
      isLoggerInitialized = true;

      // java.util.logging uses hierarchical loggers, so we just need to set the log level on the
      // parent package logger
      Logger jdbcJulLogger = Logger.getLogger(PARENT_CLASS_PREFIX);

      jdbcJulLogger.setLevel(level);
      jdbcJulLogger.setUseParentHandlers(false);

      String logPattern = getLogPattern(logDir);
      Handler handler;
      if (logPattern.equalsIgnoreCase(STDOUT)) {
        handler =
            new StreamHandler(System.out, new Slf4jFormatter()) {
              @Override
              public void publish(LogRecord record) {
                super.publish(record);
                // prompt flushing; full send >>> 🚀
                flush();
              }
            };
      } else {
        handler = new FileHandler(logPattern, logFileSizeBytes, logFileCount, true);
      }
      handler.setLevel(level);
      handler.setFormatter(new Slf4jFormatter());
      jdbcJulLogger.addHandler(handler);
    }
  }

  private void log(Level level, String message, Throwable throwable) {
    String[] callerClassMethod = getCaller();
    if (throwable == null) {
      logger.logp(level, callerClassMethod[0], callerClassMethod[1], message);
    } else {
      logger.logp(level, callerClassMethod[0], callerClassMethod[1], message, throwable);
    }
  }

  /**
   * Retrieves the class name and method name of the caller that initiated the logging request. This
   * method navigates the stack trace to find the first method outside the known logging methods,
   * providing the context from where the log was called. This is particularly useful for including
   * in log messages to identify the source of the log entry.
   *
   * <p>The method uses a two-step filtering process on the stack trace:
   *
   * <ol>
   *   <li>It first drops stack trace elements until it finds one whose method name is a known
   *       logging method (e.g., trace, debug, info, warn, error).
   *   <li>Then, it continues to drop elements until it finds the first method not in the set of
   *       logging methods, which is considered the caller.
   * </ol>
   */
  protected static String[] getCaller() {
    return Stream.of(Thread.currentThread().getStackTrace())
        .dropWhile(stackTrace -> !logMethods.contains(stackTrace.getMethodName()))
        .dropWhile(stackTrace -> logMethods.contains(stackTrace.getMethodName()))
        .findFirst()
        .map(stackTrace -> new String[] {stackTrace.getClassName(), stackTrace.getMethodName()})
        .orElse(
            new String[] {
              "unknownClass", "unknownMethod"
            }); // lost in the stack trace wonderland :)
  }

  /**
   * Generates the log file pattern based on the provided log directory. If the log directory is
   * specified as "STDOUT", logging will be directed to the console. Otherwise, it ensures the
   * directory exists and resolves the log file path within it.
   */
  protected static String getLogPattern(String logDir) {
    if (logDir.equalsIgnoreCase(STDOUT)) {
      return STDOUT;
    }

    Path dirPath = Paths.get(logDir);
    if (Files.notExists(dirPath)) {
      try {
        LOGGER.info("Creating log directory for JUL logging: " + dirPath);
        Files.createDirectories(dirPath);
      } catch (IOException e) {
        // If the directory cannot be created, log to the console instead
        LOGGER.info(
            "Error creating log directory " + dirPath + " for JUL logging." + e.getMessage());
        return STDOUT;
      }
    }

    return dirPath.resolve(DATABRICKS_LOG_FILE).toString();
  }

  private static String getPackagePrefix() {
    String prefix = System.getenv("JDBC_PACKAGE_PREFIX");
    if (prefix != null && !prefix.isEmpty()) {
      return prefix;
    }
    return DEFAULT_PACKAGE_PREFIX;
  }
}
