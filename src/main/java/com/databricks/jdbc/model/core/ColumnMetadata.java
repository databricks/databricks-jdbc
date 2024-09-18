package com.databricks.jdbc.model.core;

public class ColumnMetadata {
  private final String name;
  private final String typeText;
  private final int typeInt;
  private final int precision;
  private final int scale;
  private final int nullable;

  private ColumnMetadata(Builder builder) {
    this.name = builder.name;
    this.typeText = builder.typeText;
    this.typeInt = builder.typeInt;
    this.precision = builder.precision;
    this.scale = builder.scale;
    this.nullable = builder.nullable;
  }

  public String getName() {
    return name;
  }

  public String getTypeText() {
    return typeText;
  }

  public int getTypeInt() {
    return typeInt;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  public int getNullable() {
    return nullable;
  }

  // Builder class for ColumnMetadata
  public static class Builder {
    private String name;
    private String typeText;
    private int typeInt;
    private int precision;
    private int scale;
    private int nullable;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder typeText(String typeText) {
      this.typeText = typeText;
      return this;
    }

    public Builder typeInt(int typeInt) {
      this.typeInt = typeInt;
      return this;
    }

    public Builder precision(int precision) {
      this.precision = precision;
      return this;
    }

    public Builder scale(int scale) {
      this.scale = scale;
      return this;
    }

    public Builder nullable(int nullable) {
      this.nullable = nullable;
      return this;
    }

    public ColumnMetadata build() {
      return new ColumnMetadata(this);
    }
  }
}
