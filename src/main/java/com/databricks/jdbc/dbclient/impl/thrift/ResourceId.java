package com.databricks.jdbc.dbclient.impl.thrift;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

/** Utility class to handle UUIDs used in Thrift identifiers. */
public class ResourceId {
  private final UUID uuid;

  ResourceId(UUID uuid) {
    this.uuid = uuid;
  }

  public static ResourceId fromBytes(byte[] bytes) {
    return new ResourceId(uuidFromBytes(bytes));
  }

  public static ResourceId fromBase64(String str) {
    return new ResourceId(uuidFromBase64(str));
  }

  @Override
  public String toString() {
    return uuidToBase64(uuid);
  }

  public byte[] toBytes() {
    return uuidToBytes(uuid);
  }

  private static byte[] uuidToBytes(UUID uuid) {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(uuid.getMostSignificantBits());
    buffer.putLong(uuid.getLeastSignificantBits());
    return buffer.array();
  }

  private static UUID uuidFromBytes(byte[] bytes) {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    return new UUID(buf.getLong(), buf.getLong());
  }

  private static String uuidToBase64(UUID uuid) {
    return Base64.getEncoder().withoutPadding().encodeToString(uuidToBytes(uuid));
  }

  private static UUID uuidFromBase64(String str) {
    return uuidFromBytes(Base64.getDecoder().decode(str));
  }
}
