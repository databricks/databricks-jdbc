package com.databricks.jdbc.common;

public enum Nullable {
  NO_NULLS(0),
  NULLABLE(1),
  UNKNOWN(2);

  private final int nullableValue;

  Nullable(int value) {
    this.nullableValue = value;
  }

  public int getValue() {
    return this.nullableValue;
  }
}
