package com.databricks.jdbc.model.core;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Column info POJO
 *
 * <p>TODO: Replace this class with the corresponding SDK implementation once it becomes available
 */
public class ColumnInfo {
  @JsonProperty("name")
  private String name;

  @JsonProperty("position")
  private Long position;

  @JsonProperty("type_interval_type")
  private String typeIntervalType;

  @JsonProperty("type_name")
  private ColumnInfoTypeName typeName;

  @JsonProperty("type_precision")
  private Long typePrecision;

  @JsonProperty("type_scale")
  private Long typeScale;

  @JsonProperty("type_text")
  private String typeText;

  @JsonProperty("type_json")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String typeJson;

  public String getTypeJson() {
    return typeJson;
  }

  public ColumnInfo setTypeJson(String typeJson) {
    this.typeJson = typeJson;
    return this;
  }

  public ColumnInfo setName(String name) {
    this.name = name;
    return this;
  }

  public String getName() {
    return this.name;
  }

  public ColumnInfo setPosition(Long position) {
    this.position = position;
    return this;
  }

  public Long getPosition() {
    return this.position;
  }

  public ColumnInfo setTypeIntervalType(String typeIntervalType) {
    this.typeIntervalType = typeIntervalType;
    return this;
  }

  public String getTypeIntervalType() {
    return this.typeIntervalType;
  }

  public ColumnInfo setTypeName(ColumnInfoTypeName typeName) {
    this.typeName = typeName;
    return this;
  }

  public ColumnInfoTypeName getTypeName() {
    return this.typeName;
  }

  public ColumnInfo setTypePrecision(Long typePrecision) {
    this.typePrecision = typePrecision;
    return this;
  }

  public Long getTypePrecision() {
    return this.typePrecision;
  }

  public ColumnInfo setTypeScale(Long typeScale) {
    this.typeScale = typeScale;
    return this;
  }

  public Long getTypeScale() {
    return this.typeScale;
  }

  public ColumnInfo setTypeText(String typeText) {
    this.typeText = typeText;
    return this;
  }

  public String getTypeText() {
    return this.typeText;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o != null && this.getClass() == o.getClass()) {
      ColumnInfo that = (ColumnInfo) o;
      return Objects.equals(this.name, that.name)
          && Objects.equals(this.position, that.position)
          && Objects.equals(this.typeIntervalType, that.typeIntervalType)
          && Objects.equals(this.typeName, that.typeName)
          && Objects.equals(this.typePrecision, that.typePrecision)
          && Objects.equals(this.typeScale, that.typeScale)
          && Objects.equals(this.typeText, that.typeText)
          && Objects.equals(this.typeJson, that.typeJson);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(
        new Object[] {
          this.name,
          this.position,
          this.typeIntervalType,
          this.typeName,
          this.typePrecision,
          this.typeScale,
          this.typeText,
          this.typeJson
        });
  }

  public String toString() {
    return (new ToStringer(ColumnInfo.class))
        .add("name", this.name)
        .add("position", this.position)
        .add("typeIntervalType", this.typeIntervalType)
        .add("typeName", this.typeName)
        .add("typePrecision", this.typePrecision)
        .add("typeScale", this.typeScale)
        .add("typeText", this.typeText)
        .add("typeJson", this.typeJson)
        .toString();
  }
}
