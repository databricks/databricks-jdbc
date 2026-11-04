package com.databricks.jdbc.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/** Captures driver diagnostics, including exception causes, for secret-disclosure tests. */
public final class LogCapture implements AutoCloseable {
  private final Logger logger = Logger.getLogger("com.databricks.jdbc");
  private final Level previousLevel = logger.getLevel();
  private final StringWriter output = new StringWriter();
  private final Handler handler =
      new Handler() {
        @Override
        public void publish(LogRecord record) {
          output.append(record.getMessage()).append('\n');
          if (record.getThrown() != null) {
            record.getThrown().printStackTrace(new PrintWriter(output));
          }
        }

        @Override
        public void flush() {}

        @Override
        public void close() {}
      };

  public LogCapture() {
    handler.setLevel(Level.ALL);
    logger.setLevel(Level.ALL);
    logger.addHandler(handler);
  }

  public String getOutput() {
    return output.toString();
  }

  @Override
  public void close() {
    logger.removeHandler(handler);
    logger.setLevel(previousLevel);
  }
}
