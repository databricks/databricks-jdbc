package com.databricks.jdbc.common.util;

public class VolumeUtil {

  /** Constants to represent the type of Volume Operation */
  public static final String VOLUME_OPERATION_TYPE_GET = "get";

  public static final String VOLUME_OPERATION_TYPE_PUT = "put";
  public static final String VOLUME_OPERATION_TYPE_REMOVE = "remove";

  /** Enum to represent the state of the Volume Operation */
  public enum VolumeOperationStatus {
    PENDING,
    RUNNING,
    ABORTED,
    SUCCEEDED,
    FAILED
  }
}
