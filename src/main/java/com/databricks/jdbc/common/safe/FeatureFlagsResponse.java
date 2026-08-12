package com.databricks.jdbc.common.safe;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FeatureFlagsResponse {
  @JsonProperty("flags")
  private List<FeatureFlagEntry> flags;

  @JsonProperty("ttl_seconds")
  private Integer ttlSeconds;

  public List<FeatureFlagEntry> getFlags() {
    return flags;
  }

  public Integer getTtlSeconds() {
    return ttlSeconds;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FeatureFlagsResponse{flags=[");
    if (flags != null) {
      for (int i = 0; i < flags.size(); i++) {
        if (i > 0) sb.append(", ");
        sb.append(flags.get(i));
      }
    }
    sb.append("], ttlSeconds=").append(ttlSeconds).append("}");
    return sb.toString();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class FeatureFlagEntry {
    @JsonProperty("name")
    private String name;

    @JsonProperty("value")
    private String value;

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }

    @Override
    public String toString() {
      return name + "=" + value;
    }
  }
}
