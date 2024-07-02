package com.databricks.jdbc.commons.util;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LoggingUtil {
  // TODO : make this thread safe.
  private static String LOGGER_NAME = "DATABRICKS-OSS-JDBC-LOGGER";
  private static Logger LOGGER = Logger.getLogger(LOGGER_NAME);
  private static FileHandler fileHandler;

  public static void setupLogger(String filePath, int fileSize, int fileCount, Level logLevel)
      throws IOException {
    // Set the logger level
    LOGGER.setLevel(logLevel);

    // Create a file handler with rolling
    // fileSize is in bytes, fileCount is the number of files to use in the rotation
    fileHandler = new FileHandler(filePath, fileSize, fileCount, true);
    fileHandler.setFormatter(new SimpleFormatter());

    // Add the file handler to the logger
    LOGGER.addHandler(fileHandler);
  }

  public static void log(Level level, String message) {
    LOGGER.log(level, message);
  }

  public static void closeLogger() {
    if (fileHandler != null) {
      fileHandler.close();
    }
  }
}
