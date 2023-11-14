package com.databricks.jdbc.core.types;

public enum Nullable {
  NO_NULLS(0),
  NULLABLE(1),
  UNKNOWN(2);

  private int nullableValue;

  Nullable(int value) {
    // Todo throw error for invalid value
    this.nullableValue = value;
  }

  public int getValue() {
    return this.nullableValue;
  }
}
