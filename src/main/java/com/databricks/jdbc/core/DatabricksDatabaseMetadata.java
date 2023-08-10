package com.databricks.jdbc.core;

import com.databricks.jdbc.driver.DatabricksJdbcConstants;

import java.sql.*;

public class DatabricksDatabaseMetadata implements DatabaseMetaData {

  private final IDatabricksConnection connection;

  private final IDatabricksSession session;

  public DatabricksDatabaseMetadata(IDatabricksConnection connection) {
      this.connection = connection;
      this.session = connection.getSession();
  }
  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getUserName() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.USER_NAME;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.PRODUCT_NAME;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.DATABASE_MAJOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DatabricksJdbcConstants.DATABASE_MINOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DatabricksJdbcConstants.DATABASE_PATCH_VERSION;
  }

  @Override
  public String getDriverName() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.JDBC_MAJOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DatabricksJdbcConstants.JDBC_MINOR_VERSION + DatabricksJdbcConstants.FULL_STOP
            + DatabricksJdbcConstants.JDBC_PATCH_VERSION;
  }

  @Override
  public int getDriverMajorVersion() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getDriverMinorVersion() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.IDENTIFIER_QUOTE_STRING;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getStringFunctions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.EMPTY_STRING;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.SCHEMA;
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.PROCEDURE;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.CATALOG;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.FULL_STOP;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
    throwExceptionIfConnectionIsClosed();

    // TODO: Handle pattern for schema, assuming schema is not a regex for now

    if (catalog == null) {
      catalog = session.getCatalog();
    }
    if (catalog == null) {
      // TODO: return an empty result set
    }
    if (schemaPattern == null) {
      schemaPattern = session.getSchema();
    }
    if (schemaPattern == null) {
      // TODO: return an empty result set
    }

    String showTablesSQL = "show tables from " + catalog + "." + schemaPattern + " like '" + tableNamePattern + "'";
    return session.getDatabricksClient().executeStatement(
        showTablesSQL, session.getWarehouseId(), true, session);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null /* catalog */, null /* schema pattern */);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    throwExceptionIfConnectionIsClosed();

    String showCatalogsSQL = "show catalogs";

    return session.getDatabricksClient().executeStatement(
        showCatalogsSQL, session.getWarehouseId(), true, session);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    throwExceptionIfConnectionIsClosed();

    // TODO: Handle null catalog, schema, table behaviour

    String showSchemaSQL = "show columns in " + catalog + "." + schemaPattern + "." + tableNamePattern;
    ResultSet resultSet = session.getDatabricksClient().executeStatement(showSchemaSQL, session.getSessionId(), session.getWarehouseId(), true);

    // TODO: Handle post result set generation filtering based on result set implementation

    return resultSet;
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Connection getConnection() throws SQLException {
    return connection.getConnection();
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return DatabricksJdbcConstants.DATABASE_MAJOR_VERSION;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return DatabricksJdbcConstants.DATABASE_MINOR_VERSION;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    return DatabricksJdbcConstants.JDBC_MAJOR_VERSION;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    return DatabricksJdbcConstants.JDBC_MINOR_VERSION;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return DatabaseMetaData.sqlStateSQL;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    throwExceptionIfConnectionIsClosed();

    if (catalog == null) {
        catalog = session.getCatalog();
    }

    if (catalog == null) {
      // TODO: Return an empty result set
    }

    String showSchemaSQL = "show schemas in " + catalog + " like \'" + schemaPattern + "\'";
    return session.getDatabricksClient().executeStatement(
        showSchemaSQL, session.getWarehouseId(), true, session);
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
    throwExceptionIfConnectionIsClosed();

    // TODO: Handle null catalog, schema, function behaviour

    String showSchemaSQL = "show functions in " + catalog + "." + schemaPattern + " like '" + functionNamePattern + "'";
    return session.getDatabricksClient().executeStatement(showSchemaSQL, session.getSessionId(), session.getWarehouseId(), true);
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  private void throwExceptionIfConnectionIsClosed() throws SQLException {
    if (!connection.getSession().isOpen()) {
      throw new DatabricksSQLException("Connection closed!");
    }
  }
}
