package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.TestConstants.WAREHOUSE_JDBC_URL;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class DatabricksDatabaseMetaDataTest {

  private IDatabricksConnection connection;
  private IDatabricksSession session;
  private DatabricksDatabaseMetaData metaData;

  private IDatabricksMetadataClient metadataClient;

  @Mock private DatabricksResultSet resultSet;

  @BeforeEach
  public void setup() throws SQLException {
    connection = Mockito.mock(IDatabricksConnection.class);
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
    assertEquals(0, majorVersion);
  }

  @Test
  public void getJDBCMinorVersion_returnsCorrectVersion() throws Exception {
    int minorVersion = metaData.getJDBCMinorVersion();
    assertEquals(9, minorVersion);
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
    assertEquals(Connection.TRANSACTION_READ_COMMITTED, defaultTransactionIsolation);
  }

  @Test
  public void supportsTransactions_returnsFalse() throws Exception {
    boolean supportsTransactions = metaData.supportsTransactions();
    assertFalse(supportsTransactions);
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
  public void testGetColumns() throws SQLException {
    ResultSet resultSet = metaData.getColumns(null, null, null, null);
    assertNotNull(resultSet);
  }

  @Test
  public void testGetSchemas() throws SQLException {
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
    assertEquals("0.9.4-oss", result);
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
  public void testUnsupportedOperations() {
    List<Callable<Object>> tasks =
        Arrays.asList(
            () -> metaData.supportsTransactionIsolationLevel(0),
            () -> metaData.getDriverMajorVersion(),
            () -> metaData.getDriverMinorVersion(),
            () -> metaData.getSearchStringEscape(),
            () -> metaData.getProcedureColumns(null, null, null, null),
            () -> metaData.getBestRowIdentifier(null, null, null, 0, false),
            () -> metaData.getVersionColumns(null, null, null),
            () -> metaData.getCrossReference(null, null, null, null, null, null),
            () -> metaData.getIndexInfo(null, null, null, false, false),
            () -> metaData.supportsConvert(0, 0),
            () -> metaData.getProcedureColumns(null, null, null, null),
            () -> metaData.getColumnPrivileges(null, null, null, null),
            () -> metaData.getTablePrivileges(null, null, null),
            () -> metaData.getSuperTypes(null, null, null),
            () -> metaData.getSuperTables(null, null, null),
            () -> metaData.getAttributes(null, null, null, null),
            () -> metaData.getClientInfoProperties(),
            () -> metaData.getFunctionColumns(null, null, null, null),
            () -> metaData.getPseudoColumns(null, null, null, null),
            () -> metaData.deletesAreDetected(ResultSet.TYPE_FORWARD_ONLY),
            () -> metaData.isWrapperFor(DatabricksDatabaseMetaData.class),
            () -> metaData.unwrap(DatabricksDatabaseMetaData.class));

    for (Callable<Object> task : tasks) {
      try {
        task.call();
        fail("Expected UnsupportedOperationException for " + task);
      } catch (UnsupportedOperationException e) {
        // This is expected
      } catch (Exception e) {
        fail("Unexpected exception type thrown: " + e);
      }
    }
  }
}
