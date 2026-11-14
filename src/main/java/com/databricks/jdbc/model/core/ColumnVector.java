package com.databricks.jdbc.model.core;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** One physical column of a SEA columnar result. Exactly one values arm is populated. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ColumnVector {
  /** Protobuf repeated-value wrapper used by each values arm. */
  public static class Values<T> {
    @JsonProperty("values")
    private List<T> values;

    public Values() {}

    public Values(List<T> values) {
      this.values = values;
    }

    public List<T> getValues() {
      return values;
    }

    public Values<T> setValues(List<T> values) {
      this.values = values;
      return this;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof Values
          && Arrays.deepEquals(
              values == null ? null : values.toArray(),
              ((Values<?>) other).values == null ? null : ((Values<?>) other).values.toArray());
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(values == null ? null : values.toArray());
    }
  }

  @JsonProperty("null_mask")
  private byte[] nullMask;

  @JsonProperty("bool_values")
  private Values<Boolean> boolValues;

  @JsonProperty("byte_values")
  private Values<Integer> byteValues;

  @JsonProperty("i16_values")
  private Values<Short> i16Values;

  @JsonProperty("i32_values")
  private Values<Integer> i32Values;

  @JsonProperty("i64_values")
  private Values<Long> i64Values;

  @JsonProperty("double_values")
  private Values<Double> doubleValues;

  @JsonProperty("string_values")
  private Values<String> stringValues;

  @JsonProperty("binary_values")
  private Values<byte[]> binaryValues;

  public byte[] getNullMask() {
    return nullMask;
  }

  public ColumnVector setNullMask(byte[] nullMask) {
    this.nullMask = nullMask;
    return this;
  }

  public Values<Boolean> getBoolValues() {
    return boolValues;
  }

  public ColumnVector setBoolValues(Values<Boolean> values) {
    boolValues = values;
    return this;
  }

  public Values<Integer> getByteValues() {
    return byteValues;
  }

  public ColumnVector setByteValues(Values<Integer> values) {
    byteValues = values;
    return this;
  }

  public Values<Short> getI16Values() {
    return i16Values;
  }

  public ColumnVector setI16Values(Values<Short> values) {
    i16Values = values;
    return this;
  }

  public Values<Integer> getI32Values() {
    return i32Values;
  }

  public ColumnVector setI32Values(Values<Integer> values) {
    i32Values = values;
    return this;
  }

  public Values<Long> getI64Values() {
    return i64Values;
  }

  public ColumnVector setI64Values(Values<Long> values) {
    i64Values = values;
    return this;
  }

  public Values<Double> getDoubleValues() {
    return doubleValues;
  }

  public ColumnVector setDoubleValues(Values<Double> values) {
    doubleValues = values;
    return this;
  }

  public Values<String> getStringValues() {
    return stringValues;
  }

  public ColumnVector setStringValues(Values<String> values) {
    stringValues = values;
    return this;
  }

  public Values<byte[]> getBinaryValues() {
    return binaryValues;
  }

  public ColumnVector setBinaryValues(Values<byte[]> values) {
    binaryValues = values;
    return this;
  }

  /** Returns the sole physical values arm, or null for an unsupported or missing arm. */
  @JsonIgnore
  public List<?> getValues() {
    if (boolValues != null) return boolValues.getValues();
    if (byteValues != null) return byteValues.getValues();
    if (i16Values != null) return i16Values.getValues();
    if (i32Values != null) return i32Values.getValues();
    if (i64Values != null) return i64Values.getValues();
    if (doubleValues != null) return doubleValues.getValues();
    if (stringValues != null) return stringValues.getValues();
    if (binaryValues != null) return binaryValues.getValues();
    return null;
  }

  public Object getValue(int row) {
    if (byteValues != null) {
      return byteValues.getValues().get(row).byteValue();
    }
    return getValues().get(row);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ColumnVector)) return false;
    ColumnVector that = (ColumnVector) other;
    return Arrays.equals(nullMask, that.nullMask)
        && Objects.equals(boolValues, that.boolValues)
        && Objects.equals(byteValues, that.byteValues)
        && Objects.equals(i16Values, that.i16Values)
        && Objects.equals(i32Values, that.i32Values)
        && Objects.equals(i64Values, that.i64Values)
        && Objects.equals(doubleValues, that.doubleValues)
        && Objects.equals(stringValues, that.stringValues)
        && Objects.equals(binaryValues, that.binaryValues);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        Arrays.hashCode(nullMask),
        boolValues,
        byteValues,
        i16Values,
        i32Values,
        i64Values,
        doubleValues,
        stringValues,
        binaryValues);
  }
}
