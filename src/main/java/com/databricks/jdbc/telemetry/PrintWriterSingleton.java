package com.databricks.jdbc.telemetry;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class PrintWriterSingleton {
  private static PrintWriter printWriter = null;

  private PrintWriterSingleton() {
    // Private constructor to prevent instantiation
  }

  public static synchronized PrintWriter getPrintWriter() {
    if (printWriter == null) {
      try {
        FileWriter fileWriter = new FileWriter("metrics.txt", true);
        printWriter = new PrintWriter(fileWriter);
      } catch (IOException e) {
        System.out.println("Error creating printWriter");
        e.printStackTrace();
      }
    }
    return printWriter;
  }
}
