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

    /*
    Function to construct the path for the listObjects API call
     */
    public static String constructListPath(
        String catalog, String schema, String volume, String path) {
      String folder = StringUtil.getFolderNameFromPath(path);
      return folder.isEmpty()
          ? StringUtil.getVolumePath(catalog, schema, volume)
          : StringUtil.getVolumePath(catalog, schema, volume + "/" + folder);
    }
  }
}
