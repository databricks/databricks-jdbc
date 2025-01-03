package com.databricks.jdbc.common.util;

public class VolumeUtil {

  /** Enum to represent the Volume Operation Type */
  public enum VolumeOperationType {
    GET("get"),
    PUT("put"),
    REMOVE("remove"),
    OTHER("other");

    private final String stringValue;

    VolumeOperationType(String stringValue) {
      this.stringValue = stringValue;
    }

    public static VolumeOperationType fromString(String text) {
      for (VolumeOperationType type : VolumeOperationType.values()) {
        if (type.stringValue.equalsIgnoreCase(text)) {
          return type;
        }
      }
      return VolumeOperationType.OTHER;
    }
  }

  /** Enum to represent the state of the Volume Operation */
  public enum VolumeOperationStatus {
    PENDING,
    RUNNING,
    ABORTED,
    SUCCEEDED,
    FAILED
  }
}
