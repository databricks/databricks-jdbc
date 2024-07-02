package com.databricks.jdbc.core.types;

public enum CompressionType {
  NONE(0),
  LZ4_COMPRESSION(1);
  private final int compressionTypeVal;

  CompressionType(int value) {
    this.compressionTypeVal = value;
  }

  public static CompressionType parseCompressionType(String compressionType) {
    try {
      int value = Integer.parseInt(compressionType);
      for (CompressionType type : values()) {
        if (type.compressionTypeVal == value) {
          return type;
        }
      }
    } catch (NumberFormatException ignored) {
    }
    return NONE;
  }
}
