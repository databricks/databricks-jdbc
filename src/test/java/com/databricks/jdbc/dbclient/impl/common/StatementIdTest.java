package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.impl.thrift.ResourceId;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import org.junit.jupiter.api.Test;

/** Unit tests for the StatementId class. */
public class StatementIdTest {

  private static final byte[] testGuidBytes =
      new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  private static final byte[] testSecretBytes =
      new byte[] {17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};

  /** Test the constructor with a SQL Exec statement ID string. */
  @Test
  public void testConstructorWithString() {
    String statementId = "test-statement-id";
    StatementId stmtId = new StatementId(statementId);
    assertEquals(DatabricksClientType.SQL_EXEC, stmtId.clientType);
    assertEquals(statementId, stmtId.guid);
    assertNull(stmtId.secret);
  }

  /** Test the constructor with a THandleIdentifier for Thrift client type. */
  @Test
  public void testConstructorWithTHandleIdentifier() {
    THandleIdentifier tHandleIdentifier =
        new THandleIdentifier().setGuid(testGuidBytes).setSecret(testSecretBytes);

    StatementId stmtId = new StatementId(tHandleIdentifier);

    String expectedGuid = ResourceId.fromBytes(testGuidBytes).toString();
    String expectedSecret = ResourceId.fromBytes(testSecretBytes).toString();

    assertEquals(DatabricksClientType.THRIFT, stmtId.clientType);
    assertEquals(expectedGuid, stmtId.guid);
    assertEquals(expectedSecret, stmtId.secret);
  }

  /** Test the deserialize method with a valid SQL Exec statement ID. */
  @Test
  public void testDeserializeSqlExec() {
    String serializedStatementId = "test-statement-id";
    StatementId stmtId = StatementId.deserialize(serializedStatementId);
    assertEquals(DatabricksClientType.SQL_EXEC, stmtId.clientType);
    assertEquals(serializedStatementId, stmtId.guid);
    assertNull(stmtId.secret);
  }

  /** Test the deserialize method with a valid Thrift statement ID. */
  @Test
  public void testDeserializeThrift() {
    String guid = "guid-base64-string";
    String secret = "secret-base64-string";
    String serializedStatementId = guid + "|" + secret;
    StatementId stmtId = StatementId.deserialize(serializedStatementId);
    assertEquals(DatabricksClientType.THRIFT, stmtId.clientType);
    assertEquals(guid, stmtId.guid);
    assertEquals(secret, stmtId.secret);
  }

  /** Test the deserialize method with an invalid input (more than one '|'). */
  @Test
  public void testDeserializeInvalid() {
    String invalidSerializedStatementId = "part1|part2|part3";
    assertThrows(
        IllegalArgumentException.class,
        () -> StatementId.deserialize(invalidSerializedStatementId));
  }

  /** Test the toString method for SQL Exec client type. */
  @Test
  public void testToStringSqlExec() {
    String statementId = "test-statement-id";
    StatementId stmtId = new StatementId(statementId);
    String result = stmtId.toString();
    assertEquals(statementId, result);
  }

  /** Test the toString method for Thrift client type. */
  @Test
  public void testToStringThrift() {
    String guid = "guid-base64-string";
    String secret = "secret-base64-string";
    String serializedStatementId = guid + "|" + secret;
    StatementId stmtId = StatementId.deserialize(serializedStatementId);
    String result = stmtId.toString();
    assertEquals(serializedStatementId, result);
  }

  /** Test the toOperationIdentifier method for SQL Exec client type (should return null). */
  @Test
  public void testToOperationIdentifierSqlExec() {
    String statementId = "test-statement-id";
    StatementId stmtId = new StatementId(statementId);
    THandleIdentifier opId = stmtId.toOperationIdentifier();
    assertNull(opId);
  }

  /** Test the toOperationIdentifier method for Thrift client type. */
  @Test
  public void testToOperationIdentifierThrift() {
    String guidBase64 = ResourceId.fromBytes(testGuidBytes).toString();
    String secretBase64 = ResourceId.fromBytes(testSecretBytes).toString();
    StatementId stmtId = new StatementId(DatabricksClientType.THRIFT, guidBase64, secretBase64);

    THandleIdentifier opId = stmtId.toOperationIdentifier();

    byte[] expectedGuidBytes = ResourceId.fromBase64(guidBase64).toBytes();
    byte[] expectedSecretBytes = ResourceId.fromBase64(secretBase64).toBytes();

    assertArrayEquals(expectedGuidBytes, opId.getGuid());
    assertArrayEquals(expectedSecretBytes, opId.getSecret());
  }

  /** Test the toSQLExecStatementId method. */
  @Test
  public void testToSQLExecStatementId() {
    String statementId = "test-statement-id";
    StatementId stmtId = new StatementId(statementId);
    String result = stmtId.toSQLExecStatementId();
    assertEquals(statementId, result);
  }

  /** Test the equals method for equal StatementId objects. */
  @Test
  public void testEquals() {
    StatementId stmtId1 = new StatementId("test-statement-id");
    StatementId stmtId2 = new StatementId("test-statement-id");
    assertEquals(stmtId1, stmtId2);
  }

  /** Test the equals method for StatementId objects with different GUIDs. */
  @Test
  public void testNotEqualsDifferentGuid() {
    StatementId stmtId1 = new StatementId("test-statement-id1");
    StatementId stmtId2 = new StatementId("test-statement-id2");
    assertNotEquals(stmtId1, stmtId2);
  }

  /** Test the equals method for StatementId objects with different client types. */
  @Test
  public void testNotEqualsDifferentClientType() {
    String guid = "guid-base64-string";
    String secret = "secret-base64-string";
    StatementId stmtId1 = new StatementId(DatabricksClientType.SQL_EXEC, guid, null);
    StatementId stmtId2 = new StatementId(DatabricksClientType.THRIFT, guid, secret);
    assertNotEquals(stmtId1, stmtId2);
  }

  /** Test the equals method when comparing to null. */
  @Test
  public void testNotEqualsNull() {
    StatementId stmtId1 = new StatementId("test-statement-id");
    assertNotNull(stmtId1);
  }

  /** Test the equals method when secrets are null. */
  @Test
  public void testEqualsNullSecret() {
    StatementId stmtId1 = new StatementId(DatabricksClientType.SQL_EXEC, "guid", null);
    StatementId stmtId2 = new StatementId(DatabricksClientType.SQL_EXEC, "guid", null);
    assertEquals(stmtId1, stmtId2);
  }

  /** Test the equals method when secrets are different. */
  @Test
  public void testNotEqualsDifferentSecret() {
    StatementId stmtId1 = new StatementId(DatabricksClientType.THRIFT, "guid", "secret1");
    StatementId stmtId2 = new StatementId(DatabricksClientType.THRIFT, "guid", "secret2");
    assertNotEquals(stmtId1, stmtId2);
  }

  /** Test that two StatementIds with the same GUID but different client types are not equal. */
  @Test
  public void testNotEqualsDifferentClientTypesSameGuid() {
    String guid = "test-guid";
    StatementId stmtId1 = new StatementId(DatabricksClientType.SQL_EXEC, guid, null);
    StatementId stmtId2 = new StatementId(DatabricksClientType.THRIFT, guid, "secret");
    assertNotEquals(stmtId1, stmtId2);
  }

  /** Test the toOperationIdentifier method with invalid base64 strings. */
  @Test
  public void testToOperationIdentifierInvalidBase64() {
    String invalidBase64Guid = "invalid-guid";
    String invalidBase64Secret = "invalid-secret";
    StatementId stmtId =
        new StatementId(DatabricksClientType.THRIFT, invalidBase64Guid, invalidBase64Secret);

    assertThrows(IllegalArgumentException.class, stmtId::toOperationIdentifier);
  }

  /** Test the constructor with an empty string. */
  @Test
  public void testConstructorWithEmptyString() {
    String statementId = "";
    StatementId stmtId = new StatementId(statementId);
    assertEquals(DatabricksClientType.SQL_EXEC, stmtId.clientType);
    assertEquals(statementId, stmtId.guid);
    assertNull(stmtId.secret);
  }

  /** Test the constructor with a THandleIdentifier having null GUID and Secret. */
  @Test
  public void testConstructorWithTHandleIdentifierNullFields() {
    THandleIdentifier tHandleIdentifier = new THandleIdentifier();

    assertThrows(NullPointerException.class, () -> new StatementId(tHandleIdentifier));
  }

  /** Test the equals method when both StatementIds have null fields. */
  @Test
  public void testEqualsBothNullFields() {
    StatementId stmtId1 = new StatementId(DatabricksClientType.SQL_EXEC, null, null);
    StatementId stmtId2 = new StatementId(DatabricksClientType.SQL_EXEC, null, null);
    assertEquals(stmtId1, stmtId2);
  }

  /** Test the toString method when GUID and Secret are null. */
  @Test
  public void testToStringNullFields() {
    StatementId stmtId = new StatementId(DatabricksClientType.SQL_EXEC, null, null);
    String result = stmtId.toString();
    assertNull(result);
  }
}
