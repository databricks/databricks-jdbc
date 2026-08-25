package com.databricks.jdbc.comparator.suite;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.config.ConnectionFactory;
import com.databricks.jdbc.comparator.config.Endpoint;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ApcExtendedProviderTest {

  @Test
  void createsRouteAppropriateMissingStatementHandles() {
    StatementId thrift = ApcExtendedProvider.missingStatementId(true);
    THandleIdentifier thriftIdentifier = thrift.toOperationIdentifier();
    assertArrayEquals(
        uuidBytes("00000000-0000-4000-8000-000000000003"), thriftIdentifier.getGuid());
    assertArrayEquals(
        uuidBytes("00000000-0000-4000-8000-000000000004"), thriftIdentifier.getSecret());

    StatementId sea = ApcExtendedProvider.missingStatementId(false);
    assertEquals("00000000-0000-4000-8000-000000000003", sea.toSQLExecStatementId());
  }

  @Test
  void enablesBoundedSeaForApcCandidateOnly() {
    String baseUrl = "jdbc:databricks://host:443/default";
    String apcPath = "/sql/protocolv1/o/123/cluster";

    assertTrue(
        new Endpoint(apcPath, Endpoint.SEA, "RIGHT")
            .toUrl(baseUrl)
            .endsWith("useThriftClient=0;UseBoundedSeaApi=1"));
    assertTrue(
        new Endpoint(apcPath, Endpoint.THRIFT, "LEFT")
            .toUrl(baseUrl)
            .endsWith("useThriftClient=1"));
    assertFalse(
        new Endpoint("/sql/1.0/warehouses/warehouse", Endpoint.SEA, "RIGHT")
            .toUrl(baseUrl)
            .contains("UseBoundedSeaApi"));
  }

  @Test
  void exposesSixStableCases() {
    List<TestCase> cases = new ApcExtendedProvider().getTestCases();

    assertEquals(6, cases.size());
    assertEquals("External-link result retrieval", cases.get(0).getDescription());
    assertEquals("Multi-page result retrieval", cases.get(1).getDescription());
    assertEquals("Mid-flight cancellation and connection recovery", cases.get(2).getDescription());
    assertEquals("Invalid SQL error surface", cases.get(3).getDescription());
    assertEquals("Invalid cluster ID error surface", cases.get(4).getDescription());
    assertEquals("Invalid session and statement handle errors", cases.get(5).getDescription());
  }

  @Test
  void invalidClusterUrlPreservesWorkspaceAndOtherParameters() {
    String url =
        "jdbc:databricks://host:443/default;httpPath=/sql/protocolv1/o/123/real-cluster;"
            + "useThriftClient=1";

    String invalid = ApcExtendedProvider.invalidClusterUrl(url);

    assertTrue(invalid.contains("/o/123/0000-000000-nosuchcl"));
    assertTrue(invalid.endsWith("useThriftClient=1"));
    assertFalse(invalid.contains("real-cluster"));
  }

  @Test
  void matchingInvalidSqlErrorsPassStrictComparison() throws Exception {
    Connection left = mock(Connection.class);
    Connection right = mock(Connection.class);
    Statement leftStatement = mock(Statement.class);
    Statement rightStatement = mock(Statement.class);
    when(left.createStatement()).thenReturn(leftStatement);
    when(right.createStatement()).thenReturn(rightStatement);
    when(leftStatement.executeQuery(anyString()))
        .thenThrow(
            new SQLException(
                "verbose wrapper: [PARSE_SYNTAX_ERROR] Syntax error at or near 'SELET'. "
                    + "SQLSTATE: 42601",
                "42601",
                7));
    when(rightStatement.executeQuery(anyString()))
        .thenThrow(
            new SQLException(
                "bounded display: [PARSE_SYNTAX_ERROR] Syntax error at or near 'SELET'. "
                    + "SQLSTATE: 42601",
                "42601",
                7));
    ApcExtendedProvider provider = new ApcExtendedProvider();
    TestCase invalidSql = provider.getTestCases().get(3);

    ComparisonResult result =
        provider.execute(left, right, mock(ConnectionFactory.class), invalidSql, "extended");

    assertFalse(result.hasDifferences());
  }

  @Test
  void cancellationMessageNormalizationIgnoresOnlyUuidValues() {
    assertEquals(
        "Statement [<uuid>] was cancelled",
        ApcExtendedProvider.normalizeUuids(
            "Statement [0578076d-e962-4580-9f93-fd199eb4740b] was cancelled"));
    assertEquals(
        "Statement [<uuid>] failed",
        ApcExtendedProvider.normalizeUuids(
            "Statement [29c80b40-05f2-4fff-ac8b-91a5d2459782] failed"));
  }

  @Test
  void consumingLazyResultSurfacesFailure() throws Exception {
    java.sql.ResultSet result = mock(java.sql.ResultSet.class);
    when(result.next()).thenThrow(new SQLException("missing statement"));

    try {
      ApcExtendedProvider.consume(result);
    } catch (SQLException error) {
      assertEquals("missing statement", error.getMessage());
      return;
    }
    throw new AssertionError("Expected lazy ResultSet failure");
  }

  private static byte[] uuidBytes(String value) {
    UUID uuid = UUID.fromString(value);
    return ByteBuffer.allocate(16)
        .putLong(uuid.getMostSignificantBits())
        .putLong(uuid.getLeastSignificantBits())
        .array();
  }
}
