package com.databricks.jdbc.comparator.suite;

import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksResultSetMetaData;
import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.comparator.ComparisonResult;
import com.databricks.jdbc.comparator.ResultSetComparator;
import com.databricks.jdbc.comparator.config.ConnectionFactory;
import com.databricks.jdbc.comparator.error.CapturedOutcome;
import com.databricks.jdbc.comparator.error.Captures;
import com.databricks.jdbc.comparator.error.ErrorFacts;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.model.client.thrift.generated.THandleIdentifier;
import com.databricks.jdbc.model.client.thrift.generated.TSessionHandle;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/** Higher-cost APC parity cases for results, cancellation, and stable error surfaces. */
public class ApcExtendedProvider implements SuiteProvider {

  private static final String EXTERNAL_LINK = "EXTERNAL_LINK_RESULTS";
  private static final String PAGINATION = "MULTI_PAGE_RESULTS";
  private static final String CANCELLATION = "MID_FLIGHT_CANCELLATION";
  private static final String INVALID_SQL = "INVALID_SQL";
  private static final String INVALID_CLUSTER = "INVALID_CLUSTER";
  private static final String INVALID_HANDLES = "INVALID_HANDLES";

  private static final String EXTERNAL_LINK_QUERY =
      "SELECT id, sha2(CAST(id AS STRING), 512) AS payload " + "FROM range(0, 100000) ORDER BY id";
  private static final String PAGINATION_QUERY =
      "SELECT id, sha2(CAST(id AS STRING), 512) AS payload " + "FROM range(0, 300000) ORDER BY id";
  private static final String SLOW_QUERY =
      "SELECT COUNT(*) FROM range(0, 100000000) a CROSS JOIN range(0, 20) b";
  private static final String BAD_SQL = "SELET 1";
  private static final String MISSING_CLUSTER_ID = "0000-000000-nosuchcl";
  private static final Pattern UUID_PATTERN =
      Pattern.compile("(?i)\\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\b");
  private static final String INVALID_SQL_MESSAGE =
      "[PARSE_SYNTAX_ERROR] Syntax error at or near 'SELET'. SQLSTATE: 42601";
  private static final String INVALID_CLUSTER_MESSAGE =
      "RESOURCE_DOES_NOT_EXIST: No cluster found matching: " + MISSING_CLUSTER_ID;
  private static final UUID MISSING_SESSION_GUID =
      UUID.fromString("00000000-0000-4000-8000-000000000001");
  private static final UUID MISSING_SESSION_SECRET =
      UUID.fromString("00000000-0000-4000-8000-000000000002");
  private static final UUID MISSING_STATEMENT_GUID =
      UUID.fromString("00000000-0000-4000-8000-000000000003");
  private static final UUID MISSING_STATEMENT_SECRET =
      UUID.fromString("00000000-0000-4000-8000-000000000004");

  @Override
  public List<TestCase> getTestCases() {
    return List.of(
        new TestCase(EXTERNAL_LINK, "External-link result retrieval"),
        new TestCase(PAGINATION, "Multi-page result retrieval"),
        new TestCase(CANCELLATION, "Mid-flight cancellation and connection recovery"),
        new TestCase(INVALID_SQL, "Invalid SQL error surface"),
        new TestCase(INVALID_CLUSTER, "Invalid cluster ID error surface"),
        new TestCase(INVALID_HANDLES, "Invalid session and statement handle errors"));
  }

  @Override
  public ComparisonResult execute(
      Connection conn1, Connection conn2, TestCase testCase, String label) {
    throw new UnsupportedOperationException("APC_EXTENDED requires the ConnectionFactory overload");
  }

  @Override
  public ComparisonResult execute(
      Connection conn1,
      Connection conn2,
      ConnectionFactory factory,
      TestCase testCase,
      String label)
      throws Exception {
    switch (testCase.getIdentifier()) {
      case EXTERNAL_LINK:
        return compareLargeResult(conn1, conn2, EXTERNAL_LINK_QUERY, testCase, label, false);
      case PAGINATION:
        return compareLargeResult(conn1, conn2, PAGINATION_QUERY, testCase, label, true);
      case CANCELLATION:
        return compareCancellation(factory, testCase, label);
      case INVALID_SQL:
        return compareInvalidSql(conn1, conn2, testCase, label);
      case INVALID_CLUSTER:
        return compareInvalidCluster(factory, testCase, label);
      case INVALID_HANDLES:
        return compareInvalidHandles(factory, testCase, label);
      default:
        throw new IllegalArgumentException(
            "Unknown APC extended case: " + testCase.getIdentifier());
    }
  }

  private ComparisonResult compareLargeResult(
      Connection leftConnection,
      Connection rightConnection,
      String query,
      TestCase testCase,
      String label,
      boolean requireMultipleChunks)
      throws Exception {
    try (Statement leftStatement = leftConnection.createStatement();
        Statement rightStatement = rightConnection.createStatement();
        ResultSet left = leftStatement.executeQuery(query);
        ResultSet right = rightStatement.executeQuery(query)) {
      assertCloudFetch(left, "LEFT");
      assertCloudFetch(right, "RIGHT");
      if (requireMultipleChunks) {
        assertMultipleChunks(left, "LEFT");
        assertMultipleChunks(right, "RIGHT");
      }
      return ResultSetComparator.compare(label, query, testCase.getArgs(), left, right);
    }
  }

  private static void assertCloudFetch(ResultSet resultSet, String side) throws SQLException {
    if (!(resultSet.getMetaData() instanceof DatabricksResultSetMetaData)) {
      throw new AssertionError(side + " did not return Databricks result metadata");
    }
    DatabricksResultSetMetaData metadata = (DatabricksResultSetMetaData) resultSet.getMetaData();
    if (!metadata.getIsCloudFetchUsed()) {
      throw new AssertionError(side + " did not use CloudFetch/external links");
    }
  }

  private static void assertMultipleChunks(ResultSet resultSet, String side) throws SQLException {
    DatabricksResultSetMetaData metadata = (DatabricksResultSetMetaData) resultSet.getMetaData();
    Long chunkCount = metadata.getChunkCount();
    if (chunkCount == null || chunkCount <= 1) {
      throw new AssertionError(
          side + " did not expose multiple result chunks; chunkCount=" + chunkCount);
    }
  }

  private ComparisonResult compareCancellation(
      ConnectionFactory factory, TestCase testCase, String label) throws Exception {
    Connection left = factory.openFresh("LEFT");
    Connection right = factory.openFresh("RIGHT");
    try {
      CancellationObservation leftObservation = cancelAndRecover(left);
      CancellationObservation rightObservation = cancelAndRecover(right);
      requireBothThrew(
          leftObservation.cancellation, rightObservation.cancellation, "mid-flight cancellation");
      ComparisonResult result = new ComparisonResult(label, CANCELLATION, testCase.getArgs());
      compareErrorFacts(
          result,
          leftObservation.cancellation,
          rightObservation.cancellation,
          "cancellation: ",
          null,
          true);
      if (!leftObservation.recovery.equals(rightObservation.recovery)) {
        result.dataDifferences.add(
            "Recovery result mismatch: "
                + leftObservation.recovery
                + " vs "
                + rightObservation.recovery);
      }
      return result;
    } finally {
      closeQuietly(left);
      closeQuietly(right);
    }
  }

  private static CancellationObservation cancelAndRecover(Connection connection) throws Exception {
    CapturedOutcome cancellation;
    try (Statement rawStatement = connection.createStatement()) {
      IDatabricksStatement statement = (IDatabricksStatement) rawStatement;
      ResultSet pending = statement.executeAsync(SLOW_QUERY);
      try {
        Thread.sleep(250);
        statement.cancel();
        cancellation =
            Captures.capture(
                () -> {
                  try (ResultSet result = statement.getExecutionResult()) {
                    while (result.next()) {
                      // A cancelled operation must not return rows.
                    }
                    return "completed";
                  }
                });
      } finally {
        pending.close();
      }
    }
    return new CancellationObservation(cancellation, querySingleLong(connection, "SELECT 1"));
  }

  private ComparisonResult compareInvalidSql(
      Connection left, Connection right, TestCase testCase, String label) {
    CapturedOutcome leftOutcome = Captures.capture(() -> executeInvalidSql(left));
    CapturedOutcome rightOutcome = Captures.capture(() -> executeInvalidSql(right));
    requireBothThrew(leftOutcome, rightOutcome, "invalid SQL");
    ComparisonResult result = new ComparisonResult(label, BAD_SQL, testCase.getArgs());
    compareErrorFacts(result, leftOutcome, rightOutcome, "", INVALID_SQL_MESSAGE, false);
    return result;
  }

  private static Object executeInvalidSql(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(BAD_SQL)) {
      return resultSet.next();
    }
  }

  private ComparisonResult compareInvalidCluster(
      ConnectionFactory factory, TestCase testCase, String label) {
    CapturedOutcome left =
        Captures.capture(
            () -> factory.open(invalidClusterUrl(factory.urlFor("LEFT")), factory.token()));
    CapturedOutcome right =
        Captures.capture(
            () -> factory.open(invalidClusterUrl(factory.urlFor("RIGHT")), factory.token()));
    try {
      requireBothThrew(left, right, "invalid cluster ID");
      ComparisonResult result = new ComparisonResult(label, INVALID_CLUSTER, testCase.getArgs());
      compareErrorFacts(result, left, right, "", INVALID_CLUSTER_MESSAGE, false);
      return result;
    } finally {
      closeReturnedConnection(left);
      closeReturnedConnection(right);
    }
  }

  static String invalidClusterUrl(String url) {
    return url.replaceFirst("(httpPath=/sql/protocolv1/o/[^/;]+/)[^;]+", "$1" + MISSING_CLUSTER_ID);
  }

  private ComparisonResult compareInvalidHandles(
      ConnectionFactory factory, TestCase testCase, String label) throws Exception {
    Connection left = factory.openFresh("LEFT");
    Connection right = factory.openFresh("RIGHT");
    try {
      HandleObservations leftObservations = exerciseInvalidHandles(left);
      HandleObservations rightObservations = exerciseInvalidHandles(right);
      requireBothThrew(
          leftObservations.statement, rightObservations.statement, "invalid statement handle");
      requireBothThrew(
          leftObservations.session, rightObservations.session, "invalid session handle");
      ComparisonResult result = new ComparisonResult(label, INVALID_HANDLES, testCase.getArgs());
      compareErrorFacts(
          result,
          leftObservations.statement,
          rightObservations.statement,
          "statement: ",
          null,
          true);
      compareErrorFacts(
          result, leftObservations.session, rightObservations.session, "session: ", null, true);
      return result;
    } finally {
      closeQuietly(left);
      closeQuietly(right);
    }
  }

  private static HandleObservations exerciseInvalidHandles(Connection connection) throws Exception {
    DatabricksConnection databricksConnection = connection.unwrap(DatabricksConnection.class);
    IDatabricksSession realSession = databricksConnection.getSession();
    boolean thrift = realSession.getSessionInfo().sessionHandle() != null;

    String statementId =
        thrift
            ? MISSING_STATEMENT_GUID + "|" + MISSING_STATEMENT_SECRET
            : MISSING_STATEMENT_GUID.toString();
    CapturedOutcome statement =
        Captures.capture(
            () -> {
              try (ResultSet result =
                  ((IDatabricksStatement) databricksConnection.getStatement(statementId))
                      .getExecutionResult()) {
                consume(result);
                return "completed";
              }
            });

    IDatabricksSession missingSession = missingSession(realSession, thrift);
    IDatabricksClient client = realSession.getDatabricksClient();
    CapturedOutcome session =
        Captures.capture(
            () ->
                client.executeStatement(
                    "SELECT 1",
                    realSession.getComputeResource(),
                    Collections.emptyMap(),
                    StatementType.SQL,
                    missingSession,
                    null,
                    null));
    Captures.closeIfResultSet(session.value());
    return new HandleObservations(statement, session);
  }

  static void consume(ResultSet result) throws SQLException {
    while (result.next()) {
      // Force lazy SEA failures to surface before the outcome is compared.
    }
  }

  private static void compareErrorFacts(
      ComparisonResult result,
      CapturedOutcome left,
      CapturedOutcome right,
      String prefix,
      String requiredMessage,
      boolean normalizeUuids) {
    ErrorFacts leftError = left.error();
    ErrorFacts rightError = right.error();
    if (!leftError.exceptionClass.equals(rightError.exceptionClass)) {
      result.dataDifferences.add(
          prefix
              + "Error class mismatch: "
              + leftError.simpleClassName()
              + " vs "
              + rightError.simpleClassName());
    }
    if (!equalsNullSafe(leftError.sqlState, rightError.sqlState)) {
      result.dataDifferences.add(
          prefix + "Error SQLState mismatch: " + leftError.sqlState + " vs " + rightError.sqlState);
    }
    if (leftError.vendorCode != rightError.vendorCode) {
      result.dataDifferences.add(
          prefix + "Error code mismatch: " + leftError.vendorCode + " vs " + rightError.vendorCode);
    }

    if (requiredMessage != null) {
      requireMessage(result, leftError.message, "LEFT", prefix, requiredMessage);
      requireMessage(result, rightError.message, "RIGHT", prefix, requiredMessage);
    } else {
      String leftMessage = normalizeUuids(leftError.message);
      String rightMessage = normalizeUuids(rightError.message);
      if (!equalsNullSafe(leftMessage, rightMessage)) {
        result.dataDifferences.add(prefix + "Error semantic message mismatch");
      }
    }
  }

  private static void requireMessage(
      ComparisonResult result, String actual, String side, String prefix, String required) {
    if (actual == null || !actual.contains(required)) {
      result.dataDifferences.add(prefix + side + " error is missing expected semantic message");
    }
  }

  static String normalizeUuids(String value) {
    return value == null ? null : UUID_PATTERN.matcher(value).replaceAll("<uuid>");
  }

  private static boolean equalsNullSafe(Object left, Object right) {
    return left == null ? right == null : left.equals(right);
  }

  private static IDatabricksSession missingSession(IDatabricksSession real, boolean thrift)
      throws Exception {
    ImmutableSessionInfo.Builder info =
        ImmutableSessionInfo.builder()
            .sessionId(MISSING_SESSION_GUID.toString())
            .computeResource(real.getComputeResource());
    if (thrift) {
      info.sessionHandle(
          new TSessionHandle(
              new THandleIdentifier()
                  .setGuid(uuidBytes(MISSING_SESSION_GUID))
                  .setSecret(uuidBytes(MISSING_SESSION_SECRET))));
    }
    ImmutableSessionInfo missingInfo = info.build();
    return (IDatabricksSession)
        Proxy.newProxyInstance(
            IDatabricksSession.class.getClassLoader(),
            new Class<?>[] {IDatabricksSession.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getSessionId")) {
                return MISSING_SESSION_GUID.toString();
              }
              if (method.getName().equals("getSessionInfo")) {
                return missingInfo;
              }
              try {
                return method.invoke(real, args);
              } catch (InvocationTargetException error) {
                throw error.getCause();
              }
            });
  }

  private static byte[] uuidBytes(UUID value) {
    return ByteBuffer.allocate(16)
        .putLong(value.getMostSignificantBits())
        .putLong(value.getLeastSignificantBits())
        .array();
  }

  private static long querySingleLong(Connection connection, String query) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query)) {
      if (!result.next()) {
        throw new AssertionError("Recovery query returned no rows");
      }
      return result.getLong(1);
    }
  }

  private static void requireBothThrew(
      CapturedOutcome left, CapturedOutcome right, String operation) {
    if (!left.threw() || !right.threw()) {
      throw new AssertionError(
          operation + " must fail on both routes; left=" + left.kind() + " right=" + right.kind());
    }
  }

  private static void closeReturnedConnection(CapturedOutcome outcome) {
    if (!outcome.threw() && outcome.value() instanceof Connection) {
      closeQuietly((Connection) outcome.value());
    }
  }

  private static void closeQuietly(Connection connection) {
    if (connection == null) {
      return;
    }
    try {
      connection.close();
    } catch (SQLException ignored) {
      // Best-effort cleanup after a deliberately invalid request.
    }
  }

  private static final class CancellationObservation {
    private final CapturedOutcome cancellation;
    private final Long recovery;

    private CancellationObservation(CapturedOutcome cancellation, Long recovery) {
      this.cancellation = cancellation;
      this.recovery = recovery;
    }
  }

  private static final class HandleObservations {
    private final CapturedOutcome statement;
    private final CapturedOutcome session;

    private HandleObservations(CapturedOutcome statement, CapturedOutcome session) {
      this.statement = statement;
      this.session = session;
    }
  }
}
