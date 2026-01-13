package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionInternal;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class DatabricksDatabaseMetaDataTest {

  private IDatabricksConnectionInternal connection;
  private IDatabricksSession session;
  private DatabricksDatabaseMetaData metaData;
  private IDatabricksMetadataClient metadataClient;

  @BeforeEach
  public void setup() throws SQLException {
    connection = Mockito.mock(IDatabricksConnectionInternal.class);
    session = Mockito.mock(IDatabricksSession.class);
    when(connection.getSession()).thenReturn(session);
    metaData = new DatabricksDatabaseMetaData(connection);
    metadataClient = Mockito.mock(IDatabricksMetadataClient.class);
    when(session.getDatabricksMetadataClient()).thenReturn(metadataClient);
    when(metadataClient.listTables(any(), any(), any(), any(), any()))
        .thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(metadataClient.listPrimaryKeys(any(), any(), any(), any()))
        .thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(metadataClient.listSchemas(any(), any(), any()))
        .thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(session.getConnectionContext())
        .thenReturn(DatabricksConnectionContext.parse(WAREHOUSE_JDBC_URL, new Properties()));
    when(metadataClient.listCatalogs(any())).thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(metadataClient.listTableTypes(any())).thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(metadataClient.listTypeInfo(any())).thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(metadataClient.listFunctions(any(), any(), any(), any()))
        .thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(metadataClient.listColumns(any(), any(), any(), any(), any()))
        .thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(metadataClient.listImportedKeys(any(), any(), any(), any()))
        .thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(metadataClient.listExportedKeys(any(), any(), any(), any()))
        .thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(metadataClient.listCrossReferences(any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Mockito.mock(DatabricksResultSet.class));
    when(connection.getConnection()).thenReturn(Mockito.mock(Connection.class));
    when(session.isOpen()).thenReturn(true);
  }

  @Test
  public void getDatabaseProductName_returnsCorrectProductName() throws Exception {
    String productName = metaData.getDatabaseProductName();
    assertEquals("SparkSQL", productName);
  }

  @Test
  public void getDatabaseProductName_throwsExceptionWhenConnectionIsClosed() throws Exception {
    when(connection.getSession().isOpen()).thenReturn(false);
    try {
      metaData.getDatabaseProductName();
    } catch (SQLException e) {
      assertEquals("Connection closed!", e.getMessage());
    }
  }

  @Test
  public void supportsBatchUpdates_returnsFalse() throws Exception {
    boolean supportsBatchUpdates = metaData.supportsBatchUpdates();
    assertFalse(supportsBatchUpdates);
  }

  @Test
  public void getDatabaseMajorVersion_returnsCorrectVersion() throws Exception {
    int majorVersion = metaData.getDatabaseMajorVersion();
    assertEquals(3, majorVersion);
  }

  @Test
  public void getDatabaseMinorVersion_returnsCorrectVersion() throws Exception {
    int minorVersion = metaData.getDatabaseMinorVersion();
    assertEquals(1, minorVersion);
  }

  @Test
  public void getJDBCMajorVersion_returnsCorrectVersion() throws Exception {
    int majorVersion = metaData.getJDBCMajorVersion();
    assertEquals(4, majorVersion);
  }

  @Test
  public void getJDBCMinorVersion_returnsCorrectVersion() throws Exception {
    int minorVersion = metaData.getJDBCMinorVersion();
    assertEquals(3, minorVersion);
  }

  @Test
  public void getSQLStateType_returnsCorrectType() throws Exception {
    int sqlStateType = metaData.getSQLStateType();
    assertEquals(DatabaseMetaData.sqlStateSQL, sqlStateType);
  }

  @Test
  public void locatorsUpdateCopy_returnsFalse() throws Exception {
    boolean locatorsUpdateCopy = metaData.locatorsUpdateCopy();
    assertFalse(locatorsUpdateCopy);
  }

  @Test
  public void supportsStatementPooling_returnsFalse() throws Exception {
    boolean supportsStatementPooling = metaData.supportsStatementPooling();
    assertFalse(supportsStatementPooling);
  }

  @Test
  public void getRowIdLifetime_returnsUnknown() throws Exception {
    RowIdLifetime rowIdLifetime = metaData.getRowIdLifetime();
    assertEquals(RowIdLifetime.ROWID_UNSUPPORTED, rowIdLifetime);
  }

  @Test
  public void supportsStoredFunctionsUsingCallSyntax_returnsFalse() throws Exception {
    boolean supportsStoredFunctionsUsingCallSyntax =
        metaData.supportsStoredFunctionsUsingCallSyntax();
    assertFalse(supportsStoredFunctionsUsingCallSyntax);
  }

  @Test
  public void autoCommitFailureClosesAllResultSets_returnsTrue() throws Exception {
    boolean autoCommitFailureClosesAllResultSets = metaData.autoCommitFailureClosesAllResultSets();
    assertTrue(autoCommitFailureClosesAllResultSets);
  }

  @Test
  public void generatedKeyAlwaysReturned_returnsFalse() throws Exception {
    boolean generatedKeyAlwaysReturned = metaData.generatedKeyAlwaysReturned();
    assertFalse(generatedKeyAlwaysReturned);
  }

  @Test
  public void supportsSchemasInDataManipulation_returnsTrue() throws Exception {
    boolean supportsSchemasInDataManipulation = metaData.supportsSchemasInDataManipulation();
    assertTrue(supportsSchemasInDataManipulation);
  }

  @Test
  public void supportsSchemasInProcedureCalls_returnsFalse() throws Exception {
    boolean supportsSchemasInProcedureCalls = metaData.supportsSchemasInProcedureCalls();
    assertFalse(supportsSchemasInProcedureCalls);
  }

  @Test
  public void supportsSchemasInTableDefinitions_returnsTrue() throws Exception {
    boolean supportsSchemasInTableDefinitions = metaData.supportsSchemasInTableDefinitions();
    assertTrue(supportsSchemasInTableDefinitions);
  }

  @Test
  public void supportsSchemasInIndexDefinitions_returnsTrue() throws Exception {
    boolean supportsSchemasInIndexDefinitions = metaData.supportsSchemasInIndexDefinitions();
    assertTrue(supportsSchemasInIndexDefinitions);
  }

  @Test
  public void supportsSchemasInPrivilegeDefinitions_returnsTrue() throws Exception {
    boolean supportsSchemasInPrivilegeDefinitions =
        metaData.supportsSchemasInPrivilegeDefinitions();
    assertTrue(supportsSchemasInPrivilegeDefinitions);
  }

  @Test
  public void supportsCatalogsInDataManipulation_returnsTrue() throws Exception {
    boolean supportsCatalogsInDataManipulation = metaData.supportsCatalogsInDataManipulation();
    assertTrue(supportsCatalogsInDataManipulation);
  }

  @Test
  public void supportsCatalogsInProcedureCalls_returnsTrue() throws Exception {
    boolean supportsCatalogsInProcedureCalls = metaData.supportsCatalogsInProcedureCalls();
    assertTrue(supportsCatalogsInProcedureCalls);
  }

  @Test
  public void supportsCatalogsInTableDefinitions_returnsTrue() throws Exception {
    boolean supportsCatalogsInTableDefinitions = metaData.supportsCatalogsInTableDefinitions();
    assertTrue(supportsCatalogsInTableDefinitions);
  }

  @Test
  public void supportsCatalogsInIndexDefinitions_returnsTrue() throws Exception {
    boolean supportsCatalogsInIndexDefinitions = metaData.supportsCatalogsInIndexDefinitions();
    assertTrue(supportsCatalogsInIndexDefinitions);
  }

  @Test
  public void supportsCatalogsInPrivilegeDefinitions_returnsTrue() throws Exception {
    boolean supportsCatalogsInPrivilegeDefinitions =
        metaData.supportsCatalogsInPrivilegeDefinitions();
    assertTrue(supportsCatalogsInPrivilegeDefinitions);
  }

  @Test
  public void supportsPositionedDelete_returnsFalse() throws Exception {
    boolean supportsPositionedDelete = metaData.supportsPositionedDelete();
    assertFalse(supportsPositionedDelete);
  }

  @Test
  public void supportsPositionedUpdate_returnsFalse() throws Exception {
    boolean supportsPositionedUpdate = metaData.supportsPositionedUpdate();
    assertFalse(supportsPositionedUpdate);
  }

  @Test
  public void supportsSelectForUpdate_returnsFalse() throws Exception {
    boolean supportsSelectForUpdate = metaData.supportsSelectForUpdate();
    assertFalse(supportsSelectForUpdate);
  }

  @Test
  public void supportsStoredProcedures_returnsTrue() throws Exception {
    boolean supportsStoredProcedures = metaData.supportsStoredProcedures();
    assertTrue(supportsStoredProcedures);
  }

  @Test
  public void supportsSubqueriesInComparisons_returnsTrue() throws Exception {
    boolean supportsSubqueriesInComparisons = metaData.supportsSubqueriesInComparisons();
    assertTrue(supportsSubqueriesInComparisons);
  }

  @Test
  public void supportsSubqueriesInExists_returnsTrue() throws Exception {
    boolean supportsSubqueriesInExists = metaData.supportsSubqueriesInExists();
    assertTrue(supportsSubqueriesInExists);
  }

  @Test
  public void supportsSubqueriesInIns_returnsTrue() throws Exception {
    boolean supportsSubqueriesInIns = metaData.supportsSubqueriesInIns();
    assertTrue(supportsSubqueriesInIns);
  }

  @Test
  public void supportsSubqueriesInQuantifieds_returnsTrue() throws Exception {
    boolean supportsSubqueriesInQuantifieds = metaData.supportsSubqueriesInQuantifieds();
    assertTrue(supportsSubqueriesInQuantifieds);
  }

  @Test
  public void supportsCorrelatedSubqueries_returnsTrue() throws Exception {
    boolean supportsCorrelatedSubqueries = metaData.supportsCorrelatedSubqueries();
    assertTrue(supportsCorrelatedSubqueries);
  }

  @Test
  public void supportsUnion_returnsTrue() throws Exception {
    boolean supportsUnion = metaData.supportsUnion();
    assertTrue(supportsUnion);
  }

  @Test
  public void supportsUnionAll_returnsTrue() throws Exception {
    boolean supportsUnionAll = metaData.supportsUnionAll();
    assertTrue(supportsUnionAll);
  }

  @Test
  public void supportsOpenCursorsAcrossCommit_returnsFalse() throws Exception {
    boolean supportsOpenCursorsAcrossCommit = metaData.supportsOpenCursorsAcrossCommit();
    assertFalse(supportsOpenCursorsAcrossCommit);
  }

  @Test
  public void supportsOpenCursorsAcrossRollback_returnsFalse() throws Exception {
    boolean supportsOpenCursorsAcrossRollback = metaData.supportsOpenCursorsAcrossRollback();
    assertFalse(supportsOpenCursorsAcrossRollback);
  }

  @Test
  public void supportsOpenStatementsAcrossCommit_returnsTrue() throws Exception {
    boolean supportsOpenStatementsAcrossCommit = metaData.supportsOpenStatementsAcrossCommit();
    assertTrue(supportsOpenStatementsAcrossCommit);
  }

  @Test
  public void supportsOpenStatementsAcrossRollback_returnsTrue() throws Exception {
    boolean supportsOpenStatementsAcrossRollback = metaData.supportsOpenStatementsAcrossRollback();
    assertTrue(supportsOpenStatementsAcrossRollback);
  }

  @Test
  public void getMaxBinaryLiteralLength_returnsExpectedLength() throws Exception {
    int maxBinaryLiteralLength = metaData.getMaxBinaryLiteralLength();
    assertEquals(0, maxBinaryLiteralLength);
  }

  @Test
  public void getMaxCharLiteralLength_returnsExpectedLength() throws Exception {
    int maxCharLiteralLength = metaData.getMaxCharLiteralLength();
    assertEquals(0, maxCharLiteralLength);
  }

  @Test
  public void getMaxColumnNameLength_returnsExpectedLength() throws Exception {
    int maxColumnNameLength = metaData.getMaxColumnNameLength();
    assertEquals(128, maxColumnNameLength);
  }

  @Test
  public void getMaxColumnsInGroupBy_returnsExpectedCount() throws Exception {
    int maxColumnsInGroupBy = metaData.getMaxColumnsInGroupBy();
    assertEquals(0, maxColumnsInGroupBy);
  }

  @Test
  public void getMaxColumnsInIndex_returnsExpectedCount() throws Exception {
    int maxColumnsInIndex = metaData.getMaxColumnsInIndex();
    assertEquals(0, maxColumnsInIndex);
  }

  @Test
  public void getMaxColumnsInOrderBy_returnsExpectedCount() throws Exception {
    int maxColumnsInOrderBy = metaData.getMaxColumnsInOrderBy();
    assertEquals(0, maxColumnsInOrderBy);
  }

  @Test
  public void getMaxColumnsInSelect_returnsExpectedCount() throws Exception {
    int maxColumnsInSelect = metaData.getMaxColumnsInSelect();
    assertEquals(0, maxColumnsInSelect);
  }

  @Test
  public void getMaxColumnsInTable_returnsExpectedCount() throws Exception {
    int maxColumnsInTable = metaData.getMaxColumnsInTable();
    assertEquals(0, maxColumnsInTable);
  }

  @Test
  public void getMaxConnections_returnsExpectedCount() throws Exception {
    int maxConnections = metaData.getMaxConnections();
    assertEquals(0, maxConnections);
  }

  @Test
  public void getMaxCursorNameLength_returnsExpectedLength() throws Exception {
    int maxCursorNameLength = metaData.getMaxCursorNameLength();
    assertEquals(0, maxCursorNameLength);
  }

  @Test
  public void getMaxIndexLength_returnsExpectedLength() throws Exception {
    int maxIndexLength = metaData.getMaxIndexLength();
    assertEquals(0, maxIndexLength);
  }

  @Test
  public void getMaxSchemaNameLength_returnsExpectedLength() throws Exception {
    int maxSchemaNameLength = metaData.getMaxSchemaNameLength();
    assertEquals(128, maxSchemaNameLength);
  }

  @Test
  public void getMaxProcedureNameLength_returnsExpectedLength() throws Exception {
    int maxProcedureNameLength = metaData.getMaxProcedureNameLength();
    assertEquals(0, maxProcedureNameLength);
  }

  @Test
  public void getMaxCatalogNameLength_returnsExpectedLength() throws Exception {
    int maxCatalogNameLength = metaData.getMaxCatalogNameLength();
    assertEquals(128, maxCatalogNameLength);
  }

  @Test
  public void getMaxRowSize_returnsExpectedSize() throws Exception {
    int maxRowSize = metaData.getMaxRowSize();
    assertEquals(0, maxRowSize);
  }

  @Test
  public void doesMaxRowSizeIncludeBlobs_returnsFalse() throws Exception {
    boolean doesMaxRowSizeIncludeBlobs = metaData.doesMaxRowSizeIncludeBlobs();
    assertFalse(doesMaxRowSizeIncludeBlobs);
  }

  @Test
  public void getMaxStatementLength_returnsExpectedLength() throws Exception {
    int maxStatementLength = metaData.getMaxStatementLength();
    assertEquals(0, maxStatementLength);
  }

  @Test
  public void getMaxStatements_returnsExpectedCount() throws Exception {
    int maxStatements = metaData.getMaxStatements();
    assertEquals(0, maxStatements);
  }

  @Test
  public void getMaxTableNameLength_returnsExpectedLength() throws Exception {
    int maxTableNameLength = metaData.getMaxTableNameLength();
    assertEquals(128, maxTableNameLength);
  }

  @Test
  public void getMaxTablesInSelect_returnsExpectedCount() throws Exception {
    int maxTablesInSelect = metaData.getMaxTablesInSelect();
    assertEquals(0, maxTablesInSelect);
  }

  @Test
  public void getMaxUserNameLength_returnsExpectedLength() throws Exception {
    int maxUserNameLength = metaData.getMaxUserNameLength();
    assertEquals(0, maxUserNameLength);
  }

  @Test
  public void getDefaultTransactionIsolation_returnsExpectedIsolationLevel() throws Exception {
    int defaultTransactionIsolation = metaData.getDefaultTransactionIsolation();
    assertEquals(Connection.TRANSACTION_REPEATABLE_READ, defaultTransactionIsolation);
  }

  @Test
  public void supportsTransactions_returnsFalseByDefault() throws Exception {
    // Default IgnoreTransactions=1 means transactions are ignored, so supportsTransactions returns
    // false
    boolean supportsTransactions = metaData.supportsTransactions();
    assertFalse(supportsTransactions);
  }

  @Test
  public void supportsTransactions_returnsTrueWhenTransactionsEnabled() throws Exception {
    // When IgnoreTransactions=0, transactions are enabled, so supportsTransactions returns true
    String urlWithTransactionsEnabled = WAREHOUSE_JDBC_URL + ";IgnoreTransactions=0";
    when(session.getConnectionContext())
        .thenReturn(
            DatabricksConnectionContext.parse(urlWithTransactionsEnabled, new Properties()));
    boolean supportsTransactions = metaData.supportsTransactions();
    assertTrue(supportsTransactions);
  }

  @Test
  public void supportsDataDefinitionAndDataManipulationTransactions_returnsFalse()
      throws Exception {
    boolean supportsDataDefinitionAndDataManipulationTransactions =
        metaData.supportsDataDefinitionAndDataManipulationTransactions();
    assertFalse(supportsDataDefinitionAndDataManipulationTransactions);
  }

  @Test
  public void supportsDataManipulationTransactionsOnly_returnsFalse() throws Exception {
    boolean supportsDataManipulationTransactionsOnly =
        metaData.supportsDataManipulationTransactionsOnly();
    assertFalse(supportsDataManipulationTransactionsOnly);
  }

  @Test
  public void dataDefinitionCausesTransactionCommit_returnsFalse() throws Exception {
    boolean dataDefinitionCausesTransactionCommit =
        metaData.dataDefinitionCausesTransactionCommit();
    assertFalse(dataDefinitionCausesTransactionCommit);
  }

  @Test
  public void dataDefinitionIgnoredInTransactions_returnsFalse() throws Exception {
    boolean dataDefinitionIgnoredInTransactions = metaData.dataDefinitionIgnoredInTransactions();
    assertFalse(dataDefinitionIgnoredInTransactions);
  }

  @Test
  public void getUDTs_returnsEmptyResultSet() throws Exception {
    ResultSet resultSet = metaData.getUDTs(null, null, null, null);
    assertFalse(resultSet.next());
  }

  @Test
  public void supportsSavepoints_returnsFalse() throws Exception {
    boolean supportsSavepoints = metaData.supportsSavepoints();
    assertFalse(supportsSavepoints);
  }

  @Test
  public void supportsNamedParameters_returnsFalse() throws Exception {
    boolean supportsNamedParameters = metaData.supportsNamedParameters();
    assertFalse(supportsNamedParameters);
  }

  @Test
  public void supportsMultipleOpenResults_returnsFalse() throws Exception {
    boolean supportsMultipleOpenResults = metaData.supportsMultipleOpenResults();
    assertFalse(supportsMultipleOpenResults);
  }

  @Test
  public void supportsGetGeneratedKeys_returnsFalse() throws Exception {
    boolean supportsGetGeneratedKeys = metaData.supportsGetGeneratedKeys();
    assertFalse(supportsGetGeneratedKeys);
  }

  @Test
  public void supportsResultSetType_returnsTrueForForwardOnly() throws Exception {
    boolean supportsResultSetType = metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY);
    assertTrue(supportsResultSetType);
  }

  @Test
  public void supportsResultSetConcurrency_returnsTrueForReadOnly() throws Exception {
    boolean supportsResultSetConcurrency =
        metaData.supportsResultSetConcurrency(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    assertTrue(supportsResultSetConcurrency);
  }

  @Test
  public void ownUpdatesAreVisible_returnsFalse() throws Exception {
    boolean ownUpdatesAreVisible = metaData.ownUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY);
    assertFalse(ownUpdatesAreVisible);
  }

  @Test
  public void ownDeletesAreVisible_returnsFalse() throws Exception {
    boolean ownDeletesAreVisible = metaData.ownDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY);
    assertFalse(ownDeletesAreVisible);
  }

  @Test
  public void ownInsertsAreVisible_returnsFalse() throws Exception {
    boolean ownInsertsAreVisible = metaData.ownInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY);
    assertFalse(ownInsertsAreVisible);
  }

  @Test
  public void othersUpdatesAreVisible_returnsFalse() throws Exception {
    boolean othersUpdatesAreVisible = metaData.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY);
    assertFalse(othersUpdatesAreVisible);
  }

  @Test
  public void othersDeletesAreVisible_returnsFalse() throws Exception {
    boolean othersDeletesAreVisible = metaData.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY);
    assertFalse(othersDeletesAreVisible);
  }

  @Test
  public void othersInsertsAreVisible_returnsFalse() throws Exception {
    boolean othersInsertsAreVisible = metaData.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY);
    assertFalse(othersInsertsAreVisible);
  }

  @Test
  public void updatesAreDetected_returnsFalse() throws Exception {
    boolean updatesAreDetected = metaData.updatesAreDetected(ResultSet.TYPE_FORWARD_ONLY);
    assertFalse(updatesAreDetected);
  }

  @Test
  public void insertsAreDetected_returnsFalse() throws Exception {
    boolean insertsAreDetected = metaData.insertsAreDetected(ResultSet.TYPE_FORWARD_ONLY);
    assertFalse(insertsAreDetected);
  }

  @Test
  public void getFunctions_returnsEmptyResultSet() throws Exception {
    ResultSet resultSet = metaData.getFunctions(null, null, null);
    assertNotNull(resultSet);
  }

  @Test
  public void getFunctions_returnsEmptyResultSetWhenExceptionIsThrown() throws Exception {
    when(metadataClient.listFunctions(session, null, "%", "%")).thenThrow(new SQLException());
    ResultSet resultSet = metaData.getFunctions(null, "%", "%");
    assertNotNull(resultSet);
    assertFalse(resultSet.next());
  }

  @Test
  public void supportsGroupBy_returnsTrue() throws Exception {
    assertTrue(metaData.supportsGroupBy());
  }

  @Test
  public void supportsGroupByUnrelated_returnsFalse() throws Exception {
    assertFalse(metaData.supportsGroupByUnrelated());
  }

  @Test
  public void supportsGroupByBeyondSelect_returnsTrue() throws Exception {
    assertTrue(metaData.supportsGroupByBeyondSelect());
  }

  @Test
  public void supportsLikeEscapeClause_returnsTrue() throws Exception {
    assertTrue(metaData.supportsLikeEscapeClause());
  }

  @Test
  public void supportsMultipleResultSets_returnsFalse() throws Exception {
    assertFalse(metaData.supportsMultipleResultSets());
  }

  @Test
  public void supportsMultipleTransactions_returnsTrue() throws Exception {
    assertTrue(metaData.supportsMultipleTransactions());
  }

  @Test
  public void supportsNonNullableColumns_returnsFalse() throws Exception {
    assertFalse(metaData.supportsNonNullableColumns());
  }

  @Test
  public void supportsMinimumSQLGrammar_returnsTrue() throws Exception {
    assertTrue(metaData.supportsMinimumSQLGrammar());
  }

  @Test
  public void supportsCoreSQLGrammar_returnsTrue() throws Exception {
    assertTrue(metaData.supportsCoreSQLGrammar());
  }

  @Test
  public void supportsExtendedSQLGrammar_returnsFalse() throws Exception {
    assertFalse(metaData.supportsExtendedSQLGrammar());
  }

  @Test
  public void supportsANSI92EntryLevelSQL_returnsTrue() throws Exception {
    assertTrue(metaData.supportsANSI92EntryLevelSQL());
  }

  @Test
  public void supportsANSI92IntermediateSQL_returnsFalse() throws Exception {
    assertFalse(metaData.supportsANSI92IntermediateSQL());
  }

  @Test
  public void supportsANSI92FullSQL_returnsFalse() throws Exception {
    assertFalse(metaData.supportsANSI92FullSQL());
  }

  @Test
  public void supportsIntegrityEnhancementFacility_returnsFalse() throws Exception {
    assertFalse(metaData.supportsIntegrityEnhancementFacility());
  }

  @Test
  public void supportsOuterJoins_returnsFalse() throws Exception {
    assertFalse(metaData.supportsOuterJoins());
  }

  @Test
  public void supportsFullOuterJoins_returnsTrue() throws Exception {
    assertTrue(metaData.supportsFullOuterJoins());
  }

  @Test
  public void supportsLimitedOuterJoins_returnsFalse() throws Exception {
    assertFalse(metaData.supportsLimitedOuterJoins());
  }

  @Test
  public void testGetProcedures() throws SQLException {
    ResultSet resultSet = metaData.getProcedures(null, null, null);
    assertNotNull(resultSet);
  }

  @Test
  public void testGetTables() throws SQLException {
    ResultSet resultSet = metaData.getTables(null, null, null, null);
    assertNotNull(resultSet);
  }

  @Test
  public void testGetTables_tableNamePatternLowercaseConversion() throws SQLException {
    ArgumentCaptor<String> patternCaptor = ArgumentCaptor.forClass(String.class);

    metaData.getTables(null, null, "MY\\_TABLE", null);
    metaData.getTables(null, null, "my\\_table", null);

    verify(metadataClient, times(2))
        .listTables(any(), any(), any(), patternCaptor.capture(), any());
    List<String> patterns = patternCaptor.getAllValues();

    // Verify both uppercase and lowercase inputs are converted to the same lowercase pattern
    assertEquals(
        patterns.get(0), patterns.get(1), "Both patterns should be converted to lowercase");
  }

  @Test
  public void testGetColumns() throws SQLException {
    ResultSet resultSet = metaData.getColumns(null, null, null, null);
    assertNotNull(resultSet);
  }

  @Test
  public void testGetSchemas_SqlExec() throws SQLException {
    when(session.getConnectionContext())
        .thenReturn(
            DatabricksConnectionContext.parse(WAREHOUSE_JDBC_URL_WITH_SEA, new Properties()));
    ResultSet resultSet = metaData.getSchemas();
    assertNotNull(resultSet);
  }

  @Test
  public void testGetSchemas_Thrift() throws SQLException {
    when(session.getConnectionContext())
        .thenReturn(
            DatabricksConnectionContext.parse(WAREHOUSE_JDBC_URL_WITH_THRIFT, new Properties()));
    ResultSet resultSet = metaData.getSchemas(null, null);
    assertNotNull(resultSet);
  }

  @Test
  public void testGetPrimaryKeys() throws SQLException {
    ResultSet resultSet = metaData.getPrimaryKeys(null, null, null);
    assertNotNull(resultSet);
  }

  @Test
  public void testSupportsResultSetHoldability() throws SQLException {
    boolean result = metaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
    assertFalse(result);
  }

  @Test
  public void testGetCatalogs() throws SQLException {
    ResultSet resultSet = metaData.getCatalogs();
    assertNotNull(resultSet);
  }

  @Test
  public void testTableTypes() throws SQLException {
    ResultSet resultSet = metaData.getTableTypes();
    assertNotNull(resultSet);
  }

  @Test
  public void testGetSchemasWithoutInput() throws SQLException {
    ResultSet resultSet = metaData.getSchemas();
    assertNotNull(resultSet);
  }

  @Test
  public void testTypeInfo() throws SQLException {
    ResultSet resultSet = metaData.getTypeInfo();
    assertNotNull(resultSet);
  }

  @Test
  public void testAllProceduresAreCallable() throws SQLException {
    boolean result = metaData.allProceduresAreCallable();
    assertTrue(result);
  }

  @Test
  public void testAllTablesAreSelectable() throws SQLException {
    boolean result = metaData.allTablesAreSelectable();
    assertTrue(result);
  }

  @Test
  public void testGetUserName() throws SQLException {
    String result = metaData.getUserName();
    assertEquals("User", result);
  }

  @Test
  public void testIsReadOnly() throws SQLException {
    boolean result = metaData.isReadOnly();
    assertFalse(result);
  }

  @Test
  public void testNullsAreSortedHigh() throws SQLException {
    boolean result = metaData.nullsAreSortedHigh();
    assertFalse(result);
  }

  @Test
  public void testNullsAreSortedLow() throws SQLException {
    boolean result = metaData.nullsAreSortedLow();
    assertTrue(result);
  }

  @Test
  public void testNullsAreSortedAtStart() throws SQLException {
    boolean result = metaData.nullsAreSortedAtStart();
    assertFalse(result);
  }

  @Test
  public void testNullsAreSortedAtEnd() throws SQLException {
    boolean result = metaData.nullsAreSortedAtEnd();
    assertFalse(result);
  }

  @Test
  public void testGetDatabaseProductVersion() throws SQLException {
    String result = metaData.getDatabaseProductVersion();
    assertEquals("3.1.1", result);
  }

  @Test
  public void testGetDriverName() throws SQLException {
    String result = metaData.getDriverName();
    assertEquals("DatabricksJDBC", result);
  }

  @Test
  public void testGetDriverVersion() throws SQLException {
    String result = metaData.getDriverVersion();
    assertEquals("3.1.1", result);
  }

  @Test
  public void testGetDriverMajorVersion() {
    int result = metaData.getDriverMajorVersion();
    assertEquals(3, result);
  }

  @Test
  public void testGetDriverMinorVersion() {
    int result = metaData.getDriverMinorVersion();
    assertEquals(1, result);
  }

  @Test
  public void testUsesLocalFiles() throws SQLException {
    boolean result = metaData.usesLocalFiles();
    assertFalse(result);
  }

  @Test
  public void testUsesLocalFilePerTable() throws SQLException {
    boolean result = metaData.usesLocalFilePerTable();
    assertFalse(result);
  }

  @Test
  public void testSupportsMixedCaseIdentifiers() throws SQLException {
    boolean result = metaData.supportsMixedCaseIdentifiers();
    assertFalse(result);
  }

  @Test
  public void testStoresUpperCaseIdentifiers() throws SQLException {
    boolean result = metaData.storesUpperCaseIdentifiers();
    assertFalse(result);
  }

  @Test
  public void testStoresLowerCaseIdentifiers() throws SQLException {
    boolean result = metaData.storesLowerCaseIdentifiers();
    assertFalse(result);
  }

  @Test
  public void testStoresMixedCaseIdentifiers() throws SQLException {
    boolean result = metaData.storesMixedCaseIdentifiers();
    assertTrue(result);
  }

  @Test
  public void testSupportsMixedCaseQuotedIdentifiers() throws SQLException {
    boolean result = metaData.supportsMixedCaseQuotedIdentifiers();
    assertTrue(result);
  }

  @Test
  public void testStoresUpperCaseQuotedIdentifiers() throws SQLException {
    boolean result = metaData.storesUpperCaseQuotedIdentifiers();
    assertFalse(result);
  }

  @Test
  public void testStoresLowerCaseQuotedIdentifiers() throws SQLException {
    boolean result = metaData.storesLowerCaseQuotedIdentifiers();
    assertFalse(result);
  }

  @Test
  public void testStoresMixedCaseQuotedIdentifiers() throws SQLException {
    boolean result = metaData.storesMixedCaseQuotedIdentifiers();
    assertFalse(result);
  }

  @Test
  public void testGetIdentifierQuoteString() throws SQLException {
    String result = metaData.getIdentifierQuoteString();
    assertEquals("`", result);
  }

  @Test
  public void testGetSQLKeywords() throws SQLException {
    String result = metaData.getSQLKeywords();
    assertEquals("", result);
  }

  @Test
  public void testGetNumericFunctions() throws SQLException {
    String result = metaData.getNumericFunctions();
    assertEquals(
        "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE",
        result);
  }

  @Test
  public void testGetStringFunctions() throws SQLException {
    String result = metaData.getStringFunctions();
    assertEquals(
        "ASCII,CHAR,CHAR_LENGTH,CHARACTER_LENGTH,CONCAT,INSERT,LCASE,LEFT,LENGTH,LOCATE,LOCATE2,LTRIM,OCTET_LENGTH,POSITION,REPEAT,REPLACE,RIGHT,RTRIM,SOUNDEX,SPACE,SUBSTRING,UCASE",
        result);
  }

  @Test
  public void testGetSystemFunctions() throws SQLException {
    String result = metaData.getSystemFunctions();
    assertEquals("DATABASE,IFNULL,USER", result);
  }

  @Test
  public void testGetTimeDateFunctions() throws SQLException {
    String result = metaData.getTimeDateFunctions();
    assertEquals(
        "CURDATE,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURTIME,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,HOUR,MINUTE,MONTH,MONTHNAME,NOW,QUARTER,SECOND,TIMESTAMPADD,TIMESTAMPDIFF,WEEK,YEAR",
        result);
  }

  @Test
  public void testGetExtraNameCharacters() throws SQLException {
    String result = metaData.getExtraNameCharacters();
    assertEquals("", result);
  }

  @Test
  public void testSupportsAlterTableWithAddColumn() throws SQLException {
    boolean result = metaData.supportsAlterTableWithAddColumn();
    assertFalse(result);
  }

  @Test
  public void testGetUrl() throws SQLException {
    assertEquals(metaData.getURL(), WAREHOUSE_JDBC_URL);
  }

  @Test
  public void testSupportsAlterTableWithDropColumn() throws SQLException {
    boolean result = metaData.supportsAlterTableWithDropColumn();
    assertFalse(result);
  }

  @Test
  public void testSupportsColumnAliasing() throws SQLException {
    boolean result = metaData.supportsColumnAliasing();
    assertTrue(result);
  }

  @Test
  public void testNullPlusNonNullIsNull() throws SQLException {
    boolean result = metaData.nullPlusNonNullIsNull();
    assertTrue(result);
  }

  @Test
  public void testSupportsConvert() throws SQLException {
    boolean result = metaData.supportsConvert();
    assertTrue(result);
    assertTrue(metaData.supportsConvert(Types.INTEGER, Types.INTEGER));
  }

  @Test
  public void testSupportsTableCorrelationNames() throws SQLException {
    boolean result = metaData.supportsTableCorrelationNames();
    assertTrue(result);
  }

  @Test
  public void testSupportsDifferentTableCorrelationNames() throws SQLException {
    boolean result = metaData.supportsDifferentTableCorrelationNames();
    assertFalse(result);
  }

  @Test
  public void testSupportsExpressionsInOrderBy() throws SQLException {
    boolean result = metaData.supportsExpressionsInOrderBy();
    assertTrue(result);
  }

  @Test
  public void testSupportsOrderByUnrelated() throws SQLException {
    boolean result = metaData.supportsOrderByUnrelated();
    assertFalse(result);
  }

  @Test
  public void testGetSchemaTerm() throws SQLException {
    String result = metaData.getSchemaTerm();
    assertEquals("schema", result);
  }

  @Test
  public void testGetProcedureTerm() throws SQLException {
    String result = metaData.getProcedureTerm();
    assertEquals("procedure", result);
  }

  @Test
  public void testGetCatalogTerm() throws SQLException {
    String result = metaData.getCatalogTerm();
    assertEquals("catalog", result);
  }

  @Test
  public void testIsCatalogAtStart() throws SQLException {
    boolean result = metaData.isCatalogAtStart();
    assertTrue(result);
  }

  @Test
  public void testGetCatalogSeparator() throws SQLException {
    String result = metaData.getCatalogSeparator();
    assertEquals(".", result);
  }

  @Test
  public void testGetConnection() throws SQLException {
    Connection result = metaData.getConnection();
    assertNotNull(result);
  }

  @Test
  public void testGetResultSetHoldability() throws SQLException {
    int result = metaData.getResultSetHoldability();
    assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, result);
  }

  @Test
  public void testGetSearchStringEscape() throws SQLException {
    String result = metaData.getSearchStringEscape();
    assertEquals(DatabricksJdbcConstants.BACKWARD_SLASH, result);
  }

  @ParameterizedTest
  @MethodSource("resultSetTypes")
  public void testDeletesAreDetected(int resultSetType, String typeName) {
    assertFalse(metaData.deletesAreDetected(resultSetType));
  }

  @ParameterizedTest
  @MethodSource("provideAttributeParameters")
  public void testGetAttributes(
      String catalog,
      String schemaPattern,
      String typeNamePattern,
      String attributeNamePattern,
      String testDescription)
      throws SQLException {
    ResultSet resultSet =
        metaData.getAttributes(catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    assertNotNull(resultSet);

    assertEquals(21, resultSet.getMetaData().getColumnCount());
    assertSame("TYPE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("TYPE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("TYPE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("ATTR_NAME", resultSet.getMetaData().getColumnName(4));
    assertEquals("DATA_TYPE", resultSet.getMetaData().getColumnName(5));
    assertEquals("ATTR_TYPE_NAME", resultSet.getMetaData().getColumnName(6));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @ParameterizedTest
  @MethodSource("provideGetBestRowIdentifierParameters")
  public void testGetBestRowIdentifier(
      String catalog,
      String schema,
      String table,
      int scope,
      boolean nullable,
      String testDescription)
      throws SQLException {
    ResultSet resultSet = metaData.getBestRowIdentifier(catalog, schema, table, scope, nullable);
    assertNotNull(resultSet);

    assertEquals(8, resultSet.getMetaData().getColumnCount());
    assertSame("SCOPE", resultSet.getMetaData().getColumnName(1));
    assertSame("COLUMN_NAME", resultSet.getMetaData().getColumnName(2));
    assertSame("DATA_TYPE", resultSet.getMetaData().getColumnName(3));
    assertSame("TYPE_NAME", resultSet.getMetaData().getColumnName(4));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @Test
  public void testGetBestRowIdentifier_throwsExceptionWhenInvalidScope() throws SQLException {
    assertThrows(
        DatabricksSQLException.class,
        () -> metaData.getBestRowIdentifier(null, null, null, 3, false));
  }

  @Test
  public void testGetColumnPrivileges() throws SQLException {
    ResultSet resultSet =
        metaData.getColumnPrivileges("sample_catalog", "sample_schema", "sample_table", "%");
    assertNotNull(resultSet);

    assertEquals(8, resultSet.getMetaData().getColumnCount());
    assertSame("TABLE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("TABLE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("TABLE_NAME", resultSet.getMetaData().getColumnName(3));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @Test
  public void testGetClientInfoProperties() throws SQLException {
    ResultSet resultSet = metaData.getClientInfoProperties();
    assertNotNull(resultSet);

    assertEquals(4, resultSet.getMetaData().getColumnCount());
    assertEquals("NAME", resultSet.getMetaData().getColumnName(1));
    assertEquals("MAX_LEN", resultSet.getMetaData().getColumnName(2));
    assertEquals("DEFAULT_VALUE", resultSet.getMetaData().getColumnName(3));
    assertEquals("DESCRIPTION", resultSet.getMetaData().getColumnName(4));

    // Verify the first row
    assertTrue(resultSet.next());
    assertEquals("APPLICATIONNAME", resultSet.getString(1));
    assertEquals(25, resultSet.getInt(2));
    assertNull(resultSet.getString(3));

    assertTrue(resultSet.next());
    assertTrue(resultSet.next());

    // No more than 3 rows
    assertFalse(resultSet.next());
  }

  @Test
  public void testGetCrossReference() throws SQLException {
    ResultSet resultSet =
        metaData.getCrossReference(
            "primary_catalog",
            "primary_schema",
            "primary_table",
            "foreign_catalog",
            "foreign_schema",
            "foreign_table");
    assertNotNull(resultSet);
  }

  @Test
  public void testGetCrossReferenceThrowsExceptionWhenPrimaryForeignTableNull() {
    assertThrows(
        DatabricksSQLException.class,
        () ->
            metaData.getCrossReference(
                "primary_catalog",
                "primary_schema",
                null,
                "primary_catalog",
                "primary_schema",
                null));
  }

  @ParameterizedTest
  @MethodSource("provideCatalogSchemaTableParameters")
  public void testGetTablePrivileges(
      String catalog, String schemaPattern, String tableNamePattern, String testDesc)
      throws SQLException {
    ResultSet resultSet = metaData.getTablePrivileges(catalog, schemaPattern, tableNamePattern);
    assertNotNull(resultSet);

    assertEquals(7, resultSet.getMetaData().getColumnCount());
    assertSame("TABLE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("TABLE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("TABLE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("GRANTOR", resultSet.getMetaData().getColumnName(4));
    assertEquals("GRANTEE", resultSet.getMetaData().getColumnName(5));
    assertEquals("PRIVILEGE", resultSet.getMetaData().getColumnName(6));
    assertEquals("IS_GRANTABLE", resultSet.getMetaData().getColumnName(7));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @ParameterizedTest
  @MethodSource("provideCatalogSchemaTableParameters")
  public void testGetVersionColumns(
      String catalog, String schemaPattern, String tableNamePattern, String testDesc)
      throws SQLException {
    ResultSet resultSet = metaData.getVersionColumns(catalog, schemaPattern, tableNamePattern);
    assertNotNull(resultSet);

    assertEquals(8, resultSet.getMetaData().getColumnCount());
    assertSame("SCOPE", resultSet.getMetaData().getColumnName(1));
    assertSame("COLUMN_NAME", resultSet.getMetaData().getColumnName(2));
    assertSame("DATA_TYPE", resultSet.getMetaData().getColumnName(3));
    assertSame("TYPE_NAME", resultSet.getMetaData().getColumnName(4));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @ParameterizedTest
  @MethodSource("provideCatalogSchemaTableParameters")
  public void testGetSuperTypes(
      String catalog, String schemaPattern, String typeNamePattern, String testDesc)
      throws SQLException {
    ResultSet resultSet = metaData.getSuperTypes(catalog, schemaPattern, typeNamePattern);
    assertNotNull(resultSet);

    assertEquals(6, resultSet.getMetaData().getColumnCount());
    assertSame("TYPE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("TYPE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("TYPE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("SUPERTYPE_CAT", resultSet.getMetaData().getColumnName(4));
    assertEquals("SUPERTYPE_SCHEM", resultSet.getMetaData().getColumnName(5));
    assertEquals("SUPERTYPE_NAME", resultSet.getMetaData().getColumnName(6));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @ParameterizedTest
  @MethodSource("provideCatalogSchemaEntityPatternParams")
  public void testGetProcedureColumns(
      String catalog,
      String schemaPattern,
      String procedureNamePattern,
      String columnNamePattern,
      String testDesc)
      throws SQLException {
    ResultSet resultSet =
        metaData.getProcedureColumns(
            catalog, schemaPattern, procedureNamePattern, columnNamePattern);
    assertNotNull(resultSet);

    assertEquals(20, resultSet.getMetaData().getColumnCount());
    assertSame("PROCEDURE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("PROCEDURE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("PROCEDURE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("COLUMN_NAME", resultSet.getMetaData().getColumnName(4));
    assertEquals("COLUMN_TYPE", resultSet.getMetaData().getColumnName(5));
    assertEquals("DATA_TYPE", resultSet.getMetaData().getColumnName(6));
    assertEquals("TYPE_NAME", resultSet.getMetaData().getColumnName(7));
    assertEquals("PRECISION", resultSet.getMetaData().getColumnName(8));
    assertEquals("LENGTH", resultSet.getMetaData().getColumnName(9));
    assertEquals("SCALE", resultSet.getMetaData().getColumnName(10));
    assertEquals("RADIX", resultSet.getMetaData().getColumnName(11));
    assertEquals("NULLABLE", resultSet.getMetaData().getColumnName(12));
    assertEquals("REMARKS", resultSet.getMetaData().getColumnName(13));

    assertEquals(1, resultSet.getMetaData().isNullable(1));
    assertEquals(1, resultSet.getMetaData().isNullable(2));
    assertEquals(0, resultSet.getMetaData().isNullable(3));
    assertEquals(0, resultSet.getMetaData().isNullable(4));
    assertEquals(0, resultSet.getMetaData().isNullable(5));
    assertEquals(0, resultSet.getMetaData().isNullable(6));
    assertEquals(0, resultSet.getMetaData().isNullable(7));
    assertEquals(1, resultSet.getMetaData().isNullable(8));
    assertEquals(1, resultSet.getMetaData().isNullable(9));
    assertEquals(1, resultSet.getMetaData().isNullable(10));
    assertEquals(1, resultSet.getMetaData().isNullable(11));
    assertEquals(0, resultSet.getMetaData().isNullable(12));
    assertEquals(1, resultSet.getMetaData().isNullable(13));
    assertEquals(1, resultSet.getMetaData().isNullable(14));
    assertEquals(0, resultSet.getMetaData().isNullable(15));
    assertEquals(1, resultSet.getMetaData().isNullable(16));
    assertEquals(1, resultSet.getMetaData().isNullable(17));
    assertEquals(0, resultSet.getMetaData().isNullable(18));
    assertEquals(0, resultSet.getMetaData().isNullable(19));
    assertEquals(0, resultSet.getMetaData().isNullable(20));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @ParameterizedTest
  @MethodSource("provideGetIndexInfoParameters")
  public void testGetIndexInfo(
      String catalog,
      String schema,
      String table,
      boolean unique,
      boolean approximate,
      String testDesc)
      throws SQLException {
    ResultSet resultSet = metaData.getIndexInfo(catalog, schema, table, unique, approximate);
    assertNotNull(resultSet);

    assertEquals(13, resultSet.getMetaData().getColumnCount());
    assertSame("TABLE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("TABLE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("TABLE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("NON_UNIQUE", resultSet.getMetaData().getColumnName(4));
    assertEquals("INDEX_QUALIFIER", resultSet.getMetaData().getColumnName(5));
    assertEquals("INDEX_NAME", resultSet.getMetaData().getColumnName(6));
    assertEquals("TYPE", resultSet.getMetaData().getColumnName(7));
    assertEquals("ORDINAL_POSITION", resultSet.getMetaData().getColumnName(8));
    assertEquals("COLUMN_NAME", resultSet.getMetaData().getColumnName(9));
    assertEquals("ASC_OR_DESC", resultSet.getMetaData().getColumnName(10));
    assertEquals("CARDINALITY", resultSet.getMetaData().getColumnName(11));
    assertEquals("PAGES", resultSet.getMetaData().getColumnName(12));
    assertEquals("FILTER_CONDITION", resultSet.getMetaData().getColumnName(13));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @ParameterizedTest
  @MethodSource("provideCatalogSchemaTableParameters")
  public void testGetSuperTables(
      String catalog, String schemaPattern, String tableNamePattern, String testDesc)
      throws SQLException {
    ResultSet resultSet = metaData.getSuperTables(catalog, schemaPattern, tableNamePattern);
    assertNotNull(resultSet);

    assertEquals(4, resultSet.getMetaData().getColumnCount());
    assertSame("TABLE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("TABLE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("TABLE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("SUPERTABLE_NAME", resultSet.getMetaData().getColumnName(4));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @ParameterizedTest
  @MethodSource("provideCatalogSchemaEntityPatternParams")
  public void testGetFunctionColumns(
      String catalog,
      String schemaPattern,
      String functionNamePattern,
      String columnNamePattern,
      String testDesc)
      throws SQLException {
    ResultSet resultSet =
        metaData.getFunctionColumns(catalog, schemaPattern, functionNamePattern, columnNamePattern);
    assertNotNull(resultSet);

    assertEquals(17, resultSet.getMetaData().getColumnCount());
    assertSame("FUNCTION_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("FUNCTION_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("FUNCTION_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("COLUMN_NAME", resultSet.getMetaData().getColumnName(4));
    assertEquals("COLUMN_TYPE", resultSet.getMetaData().getColumnName(5));
    assertEquals("DATA_TYPE", resultSet.getMetaData().getColumnName(6));
    assertEquals("TYPE_NAME", resultSet.getMetaData().getColumnName(7));
    assertEquals("PRECISION", resultSet.getMetaData().getColumnName(8));
    assertEquals("LENGTH", resultSet.getMetaData().getColumnName(9));
    assertEquals("SCALE", resultSet.getMetaData().getColumnName(10));
    assertEquals("RADIX", resultSet.getMetaData().getColumnName(11));
    assertEquals("NULLABLE", resultSet.getMetaData().getColumnName(12));
    assertEquals("REMARKS", resultSet.getMetaData().getColumnName(13));
    assertEquals("CHAR_OCTET_LENGTH", resultSet.getMetaData().getColumnName(14));
    assertEquals("ORDINAL_POSITION", resultSet.getMetaData().getColumnName(15));
    assertEquals("IS_NULLABLE", resultSet.getMetaData().getColumnName(16));
    assertEquals("SPECIFIC_NAME", resultSet.getMetaData().getColumnName(17));

    assertEquals(1, resultSet.getMetaData().isNullable(1));
    assertEquals(1, resultSet.getMetaData().isNullable(2));
    assertEquals(0, resultSet.getMetaData().isNullable(3));
    assertEquals(0, resultSet.getMetaData().isNullable(4));
    assertEquals(0, resultSet.getMetaData().isNullable(5));
    assertEquals(0, resultSet.getMetaData().isNullable(6));
    assertEquals(0, resultSet.getMetaData().isNullable(7));
    assertEquals(1, resultSet.getMetaData().isNullable(8));
    assertEquals(1, resultSet.getMetaData().isNullable(9));
    assertEquals(1, resultSet.getMetaData().isNullable(10));
    assertEquals(1, resultSet.getMetaData().isNullable(11));
    assertEquals(0, resultSet.getMetaData().isNullable(12));
    assertEquals(1, resultSet.getMetaData().isNullable(13));
    assertEquals(1, resultSet.getMetaData().isNullable(14));
    assertEquals(0, resultSet.getMetaData().isNullable(15));
    assertEquals(0, resultSet.getMetaData().isNullable(16));
    assertEquals(0, resultSet.getMetaData().isNullable(17));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @ParameterizedTest
  @MethodSource("provideCatalogSchemaEntityPatternParams")
  public void testGetPseudoColumns(
      String catalog,
      String schemaPattern,
      String tableNamePattern,
      String columnNamePattern,
      String testDesc)
      throws SQLException {
    ResultSet resultSet =
        metaData.getPseudoColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
    assertNotNull(resultSet);

    assertEquals(12, resultSet.getMetaData().getColumnCount());
    assertSame("TABLE_CAT", resultSet.getMetaData().getColumnName(1));
    assertSame("TABLE_SCHEM", resultSet.getMetaData().getColumnName(2));
    assertEquals("TABLE_NAME", resultSet.getMetaData().getColumnName(3));
    assertEquals("COLUMN_NAME", resultSet.getMetaData().getColumnName(4));
    assertEquals("DATA_TYPE", resultSet.getMetaData().getColumnName(5));
    assertEquals("COLUMN_SIZE", resultSet.getMetaData().getColumnName(6));
    assertEquals("DECIMAL_DIGITS", resultSet.getMetaData().getColumnName(7));
    assertEquals("NUM_PREC_RADIX", resultSet.getMetaData().getColumnName(8));
    assertEquals("COLUMN_USAGE", resultSet.getMetaData().getColumnName(9));
    assertEquals("REMARKS", resultSet.getMetaData().getColumnName(10));
    assertEquals("CHAR_OCTET_LENGTH", resultSet.getMetaData().getColumnName(11));
    assertEquals("IS_NULLABLE", resultSet.getMetaData().getColumnName(12));

    assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(1));
    assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(2));
    assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(3));
    assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(4));
    assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(5));
    assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(6));
    assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(7));
    assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(8));
    assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(9));
    assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(10));
    assertEquals(Types.INTEGER, resultSet.getMetaData().getColumnType(11));
    assertEquals(Types.VARCHAR, resultSet.getMetaData().getColumnType(12));

    assertEquals(1, resultSet.getMetaData().isNullable(1));
    assertEquals(1, resultSet.getMetaData().isNullable(2));
    assertEquals(0, resultSet.getMetaData().isNullable(3));
    assertEquals(0, resultSet.getMetaData().isNullable(4));
    assertEquals(0, resultSet.getMetaData().isNullable(5));
    assertEquals(1, resultSet.getMetaData().isNullable(6));
    assertEquals(1, resultSet.getMetaData().isNullable(7));
    assertEquals(1, resultSet.getMetaData().isNullable(8));
    assertEquals(0, resultSet.getMetaData().isNullable(9));
    assertEquals(1, resultSet.getMetaData().isNullable(10));
    assertEquals(1, resultSet.getMetaData().isNullable(11));
    assertEquals(0, resultSet.getMetaData().isNullable(12));

    // Result set is empty
    assertFalse(resultSet.next());
  }

  @Test
  public void testGetImportedKeys() throws SQLException {
    ResultSet resultSet = metaData.getImportedKeys("catalog", "schema", "table");
    assertNotNull(resultSet);
  }

  @Test
  public void testGetExportedKeys() throws SQLException {
    ResultSet resultSet = metaData.getExportedKeys("catalog", "schema", "table");
    assertNotNull(resultSet);
  }

  @Test
  public void testSupportsTransactionIsolationLevel() throws SQLException {
    assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE));
    assertFalse(
        metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
    assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
    assertTrue(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
    assertFalse(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
  }

  @Test
  public void testIsWrapperFor_ReturnsTrueForDatabaseMetaData() throws SQLException {
    // Test that it returns true for DatabaseMetaData interface
    assertTrue(metaData.isWrapperFor(DatabaseMetaData.class));
  }

  @Test
  public void testIsWrapperFor_ReturnsTrueForSameClass() throws SQLException {
    // Test that it returns true for its own class
    assertTrue(metaData.isWrapperFor(DatabricksDatabaseMetaData.class));
  }

  @Test
  public void testIsWrapperFor_ReturnsFalseForUnrelatedInterface() throws SQLException {
    // Test that it returns false for unrelated interfaces
    assertFalse(metaData.isWrapperFor(Runnable.class));
  }

  @Test
  public void testUnwrap_SuccessfullyUnwrapsDatabaseMetaData() throws SQLException {
    // Test unwrapping to DatabaseMetaData interface
    DatabaseMetaData unwrapped = metaData.unwrap(DatabaseMetaData.class);
    assertNotNull(unwrapped);
    assertSame(metaData, unwrapped);
  }

  @Test
  public void testUnwrap_SuccessfullyUnwrapsSameClass() throws SQLException {
    // Test unwrapping to its own class
    DatabricksDatabaseMetaData unwrapped = metaData.unwrap(DatabricksDatabaseMetaData.class);
    assertNotNull(unwrapped);
    assertSame(metaData, unwrapped);
  }

  @Test
  public void testUnwrap_ThrowsExceptionForUnrelatedInterface() throws SQLException {
    // Test that unwrapping to an unrelated interface throws exception
    assertThrows(DatabricksSQLException.class, () -> metaData.unwrap(Runnable.class));
  }

  private static Stream<Arguments> provideAttributeParameters() {
    return Stream.of(
        // Test case 1: All nulls (should return empty result set)
        Arguments.of(null, null, null, null, "All parameters null"),

        // Test case 2: Valid catalog, others null
        Arguments.of("test_catalog", null, null, null, "Only catalog specified"),

        // Test case 3: Valid schema pattern, others null
        Arguments.of(null, "test_schema%", null, null, "Only schema pattern specified"),

        // Test case 4: Valid type name pattern, others null
        Arguments.of(null, null, "TEST_TYPE%", null, "Only type name pattern specified"),

        // Test case 5: Valid attribute name pattern, others null
        Arguments.of(null, null, null, "attr%", "Only attribute name pattern specified"),

        // Test case 6: All parameters specified with wildcards
        Arguments.of(
            "test_catalog",
            "test_schema%",
            "TEST_TYPE%",
            "attr%",
            "All parameters specified with wildcards"),

        // Test case 7: Exact match patterns
        Arguments.of(
            "test_catalog", "test_schema", "TEST_TYPE", "attribute_name", "Exact match patterns"),

        // Test case 8: Invalid patterns (should return empty result set)
        Arguments.of(
            "invalid_catalog",
            "invalid_schema",
            "INVALID_TYPE",
            "invalid_attr",
            "Invalid patterns"),

        // Test case 9: Special characters in patterns
        Arguments.of(null, "_test%", "%TYPE_", "_attr%", "Special characters in patterns"));
  }

  private static Stream<Arguments> provideGetBestRowIdentifierParameters() {
    return Stream.of(
        // Test case 1: All nulls (should return empty result set)
        Arguments.of(null, null, null, 0, false, "All parameters null"),

        // Test case 2: Valid catalog, others null
        Arguments.of("test_catalog", null, null, 0, false, "Only catalog specified"),

        // Test case 3: Valid schema, others null
        Arguments.of(null, "test_schema", null, 0, false, "Only schema specified"),

        // Test case 4: Valid table, others null
        Arguments.of(null, null, "test_table", 0, false, "Only table specified"),

        // Test case 5: Valid scope, others null
        Arguments.of(null, null, null, 1, false, "Only scope specified"),

        // Test case 6: Valid nullable, others null
        Arguments.of(null, null, null, 0, true, "Only nullable specified"),

        // Test case 7: All parameters specified
        Arguments.of(
            "test_catalog", "test_schema", "test_table", 1, true, "All parameters specified"));
  }

  private static Stream<Arguments> resultSetTypes() {
    return Stream.of(
        Arguments.of(ResultSet.TYPE_FORWARD_ONLY, "TYPE_FORWARD_ONLY"),
        Arguments.of(ResultSet.TYPE_SCROLL_INSENSITIVE, "TYPE_SCROLL_INSENSITIVE"),
        Arguments.of(ResultSet.TYPE_SCROLL_SENSITIVE, "TYPE_SCROLL_SENSITIVE"));
  }

  private static Stream<Arguments> provideCatalogSchemaTableParameters() {
    return Stream.of(
        // Test case 1: All nulls (should return empty result set)
        Arguments.of(null, null, null, "All parameters null"),

        // Test case 2: Valid catalog, others null
        Arguments.of("test_catalog", null, null, "Only catalog specified"),

        // Test case 3: Valid schema, others null
        Arguments.of(null, "test_schema", null, "Only schema specified"),

        // Test case 4: Valid table, others null
        Arguments.of(null, null, "test_table", "Only table specified"),

        // Test case 5: All parameters specified
        Arguments.of("test_catalog", "test_schema", "test_table", "All parameters specified"));
  }

  private static Stream<Arguments> provideGetIndexInfoParameters() {
    return Stream.of(
        // Test case 1: All nulls (should return empty result set)
        Arguments.of(null, null, null, false, false, "All parameters null"),

        // Test case 2: Valid catalog, others null
        Arguments.of("test_catalog", null, null, false, false, "Only catalog specified"),

        // Test case 3: Valid schema, others null
        Arguments.of(null, "test_schema", null, false, false, "Only schema specified"),

        // Test case 4: Valid table, others null
        Arguments.of(null, null, "test_table", false, false, "Only table specified"),

        // Test case 5: All parameters specified
        Arguments.of(
            "test_catalog", "test_schema", "test_table", false, true, "All parameters specified"));
  }

  /**
   * Provides parameters for testing {@link DatabaseMetaData} methods with catalog, schema pattern,
   * entity pattern, and column name pattern. Entity could be a table, function, procedure, etc.
   */
  private static Stream<Arguments> provideCatalogSchemaEntityPatternParams() {
    return Stream.of(
        // Test case 1: All nulls (should return empty result set)
        Arguments.of(null, null, null, null, "All parameters null"),

        // Test case 2: Valid catalog, others null
        Arguments.of("test_catalog", null, null, null, "Only catalog specified"),

        // Test case 3: Valid schema pattern, others null
        Arguments.of(null, "test_schema%", null, null, "Only schema pattern specified"),

        // Test case 4: Valid entity name pattern, others null
        Arguments.of(null, null, "TEST_ENTITY%", null, "Only function name pattern specified"),

        // Test case 5: Valid column name, others null
        Arguments.of(null, null, null, "column_name", "Only column name specified"),

        // Test case 6: All parameters specified with wildcards
        Arguments.of(
            "test_catalog",
            "test_schema%",
            "TEST_ENTITY%",
            "column_name",
            "All parameters specified with wildcards"),

        // Test case 7: Exact match patterns
        Arguments.of(
            "test_catalog", "test_schema", "TEST_ENTITY", "column_name", "Exact match patterns"),

        // Test case 8: Invalid patterns (should return empty result set)
        Arguments.of(
            "invalid_catalog",
            "invalid_schema",
            "INVALID_ENTITY",
            "invalid_column",
            "Invalid patterns"),

        // Test case 9: Special characters in patterns
        Arguments.of(null, "_test%", "%ENTITY_", "_column%", "Special characters in patterns"));
  }
}
