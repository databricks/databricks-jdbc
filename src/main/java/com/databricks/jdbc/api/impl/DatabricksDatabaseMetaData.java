package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.sql.*;
import java.util.*;

public class DatabricksDatabaseMetaData implements DatabaseMetaData {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksDatabaseMetaData.class);
  public static final String DRIVER_NAME = "DatabricksJDBC";
  public static final String PRODUCT_NAME = "SparkSQL";
  public static final int DATABASE_MAJOR_VERSION = 3;
  public static final int DATABASE_MINOR_VERSION = 1;
  public static final int DATABASE_PATCH_VERSION = 1;
  public static final Integer MAX_NAME_LENGTH = 128;
  public static final String NUMERIC_FUNCTIONS =
      "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,PI,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN,TRUNCATE";
  public static final String STRING_FUNCTIONS =
      "ASCII,CHAR,CHAR_LENGTH,CHARACTER_LENGTH,CONCAT,INSERT,LCASE,LEFT,LENGTH,LOCATE,LOCATE2,LTRIM,OCTET_LENGTH,POSITION,REPEAT,REPLACE,RIGHT,RTRIM,SOUNDEX,SPACE,SUBSTRING,UCASE";
  public static final String SYSTEM_FUNCTIONS = "DATABASE,IFNULL,USER";
  public static final String TIME_DATE_FUNCTIONS =
      "CURDATE,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURTIME,DAYNAME,DAYOFMONTH,DAYOFWEEK,DAYOFYEAR,HOUR,MINUTE,MONTH,MONTHNAME,NOW,QUARTER,SECOND,TIMESTAMPADD,TIMESTAMPDIFF,WEEK,YEAR";
  private final IDatabricksConnection connection;
  private final IDatabricksSession session;

  public DatabricksDatabaseMetaData(IDatabricksConnection connection) {
    this.connection = connection;
    this.session = connection.getSession();
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    LOGGER.debug("public boolean allProceduresAreCallable()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    LOGGER.debug("public boolean allTablesAreSelectable()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    LOGGER.debug("public String getURL()");
    return this.session.getConnectionContext().getConnectionURL();
  }

  @Override
  public String getUserName() throws SQLException {
    LOGGER.debug("public String getUserName()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.USER_NAME;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    LOGGER.debug("public boolean isReadOnly()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    LOGGER.debug("public boolean nullsAreSortedHigh()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    LOGGER.debug("public boolean nullsAreSortedLow()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    LOGGER.debug("public boolean nullsAreSortedAtStart()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    LOGGER.debug("public boolean nullsAreSortedAtEnd()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    LOGGER.debug("public String getDatabaseProductName()");
    throwExceptionIfConnectionIsClosed();
    return PRODUCT_NAME;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    LOGGER.debug("public String getDatabaseProductVersion()");
    throwExceptionIfConnectionIsClosed();
    return DATABASE_MAJOR_VERSION
        + DatabricksJdbcConstants.FULL_STOP
        + DATABASE_MINOR_VERSION
        + DatabricksJdbcConstants.FULL_STOP
        + DATABASE_PATCH_VERSION;
  }

  @Override
  public String getDriverName() throws SQLException {
    LOGGER.debug("public String getDriverName()");
    throwExceptionIfConnectionIsClosed();
    return DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    LOGGER.debug("public String getDriverVersion()");
    throwExceptionIfConnectionIsClosed();
    return DriverUtil.getVersion();
  }

  @Override
  public int getDriverMajorVersion() {
    return DriverUtil.getMajorVersion();
  }

  @Override
  public int getDriverMinorVersion() {
    return DriverUtil.getMinorVersion();
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    LOGGER.debug("public boolean usesLocalFiles()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    LOGGER.debug("public boolean usesLocalFilePerTable()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    LOGGER.debug("public boolean supportsMixedCaseIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    LOGGER.debug("public boolean storesUpperCaseIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    LOGGER.debug("public boolean storesLowerCaseIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    LOGGER.debug("public boolean storesMixedCaseIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    LOGGER.debug("public boolean supportsMixedCaseQuotedIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    LOGGER.debug("public boolean storesUpperCaseQuotedIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    LOGGER.debug("public boolean storesLowerCaseQuotedIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    LOGGER.debug("public boolean storesMixedCaseQuotedIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    LOGGER.debug("public String getIdentifierQuoteString()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.IDENTIFIER_QUOTE_STRING;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    LOGGER.debug("public String getSQLKeywords()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.EMPTY_STRING;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    LOGGER.debug("public String getNumericFunctions()");
    throwExceptionIfConnectionIsClosed();
    return NUMERIC_FUNCTIONS;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    LOGGER.debug("public String getStringFunctions()");
    throwExceptionIfConnectionIsClosed();
    return STRING_FUNCTIONS;
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    LOGGER.debug("public String getSystemFunctions()");
    throwExceptionIfConnectionIsClosed();
    return SYSTEM_FUNCTIONS;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    LOGGER.debug("public String getTimeDateFunctions()");
    throwExceptionIfConnectionIsClosed();
    return TIME_DATE_FUNCTIONS;
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    LOGGER.debug("public String getSearchStringEscape()");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getSearchStringEscape()");
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    LOGGER.debug("public String getExtraNameCharacters()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.EMPTY_STRING;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    LOGGER.debug("public boolean supportsAlterTableWithAddColumn()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    LOGGER.debug("public boolean supportsAlterTableWithDropColumn()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    LOGGER.debug("public boolean supportsColumnAliasing()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    LOGGER.debug("public boolean nullPlusNonNullIsNull()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    LOGGER.debug("public boolean supportsConvert()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - supportsConvert(int fromType, int toType)");
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    LOGGER.debug("public boolean supportsTableCorrelationNames()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    LOGGER.debug("public boolean supportsDifferentTableCorrelationNames()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    LOGGER.debug("public boolean supportsExpressionsInOrderBy()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    LOGGER.debug("public boolean supportsOrderByUnrelated()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    LOGGER.debug("public boolean supportsGroupBy()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    LOGGER.debug("public boolean supportsGroupByUnrelated()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    LOGGER.debug("public boolean supportsGroupByBeyondSelect()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    LOGGER.debug("public boolean supportsLikeEscapeClause()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    LOGGER.debug("public boolean supportsMultipleResultSets()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    LOGGER.debug("public boolean supportsMultipleTransactions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    LOGGER.debug("public boolean supportsNonNullableColumns()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    LOGGER.debug("public boolean supportsMinimumSQLGrammar()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    LOGGER.debug("public boolean supportsCoreSQLGrammar()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    LOGGER.debug("public boolean supportsExtendedSQLGrammar()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    LOGGER.debug("public boolean supportsANSI92EntryLevelSQL()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    LOGGER.debug("public boolean supportsANSI92IntermediateSQL()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    LOGGER.debug("public boolean supportsANSI92FullSQL()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    LOGGER.debug("public boolean supportsIntegrityEnhancementFacility()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    LOGGER.debug("public boolean supportsOuterJoins()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    LOGGER.debug("public boolean supportsFullOuterJoins()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    LOGGER.debug("public boolean supportsLimitedOuterJoins()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    LOGGER.debug("public String getSchemaTerm()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.SCHEMA;
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    LOGGER.debug("public String getProcedureTerm()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.PROCEDURE;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    LOGGER.debug("public String getCatalogTerm()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.CATALOG;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    LOGGER.debug("public boolean isCatalogAtStart()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    LOGGER.debug("public String getCatalogSeparator()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.FULL_STOP;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    LOGGER.debug("public boolean supportsSchemasInDataManipulation()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    LOGGER.debug("public boolean supportsSchemasInProcedureCalls()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    LOGGER.debug("public boolean supportsSchemasInTableDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    LOGGER.debug("public boolean supportsSchemasInIndexDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    LOGGER.debug("public boolean supportsSchemasInPrivilegeDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    LOGGER.debug("public boolean supportsCatalogsInDataManipulation()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    LOGGER.debug("public boolean supportsCatalogsInProcedureCalls()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    LOGGER.debug("public boolean supportsCatalogsInTableDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    LOGGER.debug("public boolean supportsCatalogsInIndexDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    LOGGER.debug("public boolean supportsCatalogsInPrivilegeDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    LOGGER.debug("public boolean supportsPositionedDelete()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    LOGGER.debug("public boolean supportsPositionedUpdate()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    LOGGER.debug("public boolean supportsSelectForUpdate()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    LOGGER.debug("public boolean supportsStoredProcedures()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    LOGGER.debug("public boolean supportsSubqueriesInComparisons()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    LOGGER.debug("public boolean supportsSubqueriesInExists()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    LOGGER.debug("public boolean supportsSubqueriesInIns()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    LOGGER.debug("public boolean supportsSubqueriesInQuantifieds()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    LOGGER.debug("public boolean supportsCorrelatedSubqueries()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    LOGGER.debug("public boolean supportsUnion()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    LOGGER.debug("public boolean supportsUnionAll()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    LOGGER.debug("public boolean supportsOpenCursorsAcrossCommit()");
    throwExceptionIfConnectionIsClosed();
    // Open cursors are not supported, however open statements are.
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    LOGGER.debug("public boolean supportsOpenCursorsAcrossRollback()");
    throwExceptionIfConnectionIsClosed();
    // Open cursors are not supported, however open statements are.
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    LOGGER.debug("public boolean supportsOpenStatementsAcrossCommit()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    LOGGER.debug("public boolean supportsOpenStatementsAcrossRollback()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    LOGGER.debug("public int getMaxBinaryLiteralLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    LOGGER.debug("public int getMaxCharLiteralLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    LOGGER.debug("public int getMaxColumnNameLength()");
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    LOGGER.debug("public int getMaxColumnsInGroupBy()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    LOGGER.debug("public int getMaxColumnsInIndex()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    LOGGER.debug("public int getMaxColumnsInOrderBy()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    LOGGER.debug("public int getMaxColumnsInSelect()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    LOGGER.debug("public int getMaxColumnsInTable()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    LOGGER.debug("public int getMaxConnections()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    LOGGER.debug("public int getMaxCursorNameLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    LOGGER.debug("public int getMaxIndexLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    LOGGER.debug("public int getMaxSchemaNameLength()");
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    LOGGER.debug("public int getMaxProcedureNameLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    LOGGER.debug("public int getMaxCatalogNameLength()");
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    LOGGER.debug("public int getMaxRowSize()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    LOGGER.debug("public boolean doesMaxRowSizeIncludeBlobs()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    LOGGER.debug("public int getMaxStatementLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    LOGGER.debug("public int getMaxStatements()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    LOGGER.debug("public int getMaxTableNameLength()");
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    LOGGER.debug("public int getMaxTablesInSelect()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    LOGGER.debug("public int getMaxUserNameLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    LOGGER.debug("public int getDefaultTransactionIsolation()");
    throwExceptionIfConnectionIsClosed();
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    LOGGER.debug("public boolean supportsTransactions()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    LOGGER.debug(
        String.format("public boolean supportsTransactionIsolationLevel(int level = {%s})", level));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - supportsTransactionIsolationLevel(int level)");
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    LOGGER.debug("public boolean supportsDataDefinitionAndDataManipulationTransactions()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    LOGGER.debug("public boolean supportsDataManipulationTransactionsOnly()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    LOGGER.debug("public boolean dataDefinitionCausesTransactionCommit()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    LOGGER.debug("public boolean dataDefinitionIgnoredInTransactions()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getProcedures(String catalog = {%s}, String schemaPattern = {%s}, String procedureNamePattern = {%s})",
            catalog, schemaPattern, procedureNamePattern));
    throwExceptionIfConnectionIsClosed();
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "getprocedures-metadata",
        Arrays.asList(
            "PROCEDURE_CAT",
            "PROCEDURE_SCHEM",
            "PROCEDURE_NAME",
            "NUM_INPUT_PARAMS",
            "NUM_OUTPUT_PARAMS",
            "NUM_RESULT_SETS",
            "REMARKS",
            "PROCEDURE_TYPE",
            "SPECIFIC_NAME"),
        Arrays.asList(
            "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR",
            "VARCHAR"),
        Arrays.asList(
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR),
        Arrays.asList(128, 128, 128, 128, 128, 128, 128, 128, 128),
        new Object[0][0],
        StatementType.METADATA);
  }

  @Override
  public ResultSet getProcedureColumns(
      String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getProcedureColumns(String catalog = {%s}, String schemaPattern = {%s}, String procedureNamePattern = {%s}, String columnNamePattern = {%s})",
            catalog, schemaPattern, procedureNamePattern, columnNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)");
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getTables(String catalog = {%s}, String schemaPattern = {%s}, String tableNamePattern = {%s}, String[] types = {%s})",
            catalog, schemaPattern, tableNamePattern, types));
    throwExceptionIfConnectionIsClosed();
    return session
        .getDatabricksMetadataClient()
        .listTables(session, catalog, schemaPattern, tableNamePattern, types);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    LOGGER.debug("public ResultSet getSchemas()");
    if (session.getConnectionContext().getClientType() == DatabricksClientType.SQL_EXEC) {
      return MetadataResultSetBuilder.getSchemasResult(null);
    }
    return getSchemas(null /* catalog */, null /* schema pattern */);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    LOGGER.debug("public ResultSet getCatalogs()");
    throwExceptionIfConnectionIsClosed();
    return session.getDatabricksMetadataClient().listCatalogs(session);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    LOGGER.debug("public ResultSet getTableTypes()");
    throwExceptionIfConnectionIsClosed();
    return session.getDatabricksMetadataClient().listTableTypes(session);
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getColumns(String catalog = {%s}, String schemaPattern = {%s}, String tableNamePattern = {%s}, String columnNamePattern = {%s})",
            catalog, schemaPattern, tableNamePattern, columnNamePattern));
    throwExceptionIfConnectionIsClosed();

    return session
        .getDatabricksMetadataClient()
        .listColumns(session, catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  @Override
  public ResultSet getColumnPrivileges(
      String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getColumnPrivileges(String catalog = {%s}, String schema = {%s}, String table = {%s}, String columnNamePattern = {%s})",
            catalog, schema, table, columnNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)");
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getTablePrivileges(String catalog = {%s}, String schemaPattern = {%s}, String tableNamePattern = {%s})",
            catalog, schemaPattern, tableNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)");
  }

  @Override
  public ResultSet getBestRowIdentifier(
      String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getBestRowIdentifier(String catalog = {%s}, String schema = {%s}, String table = {%s}, int scope = {%s}, boolean nullable = {%s})",
            catalog, schema, table, scope, nullable));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)");
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getVersionColumns(String catalog = {}, String schema = {}, String table = {})",
            catalog,
            schema,
            table));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getVersionColumns(String catalog, String schema, String table)");
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getPrimaryKeys(String catalog = {}, String schema = {}, String table = {})",
            catalog,
            schema,
            table));
    throwExceptionIfConnectionIsClosed();
    return session.getDatabricksMetadataClient().listPrimaryKeys(session, catalog, schema, table);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getImportedKeys(String catalog = {}, String schema = {}, String table = {})",
            catalog,
            schema,
            table));
    // TODO (PECO-1696): Implement getImportedKeys
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getExportedKeys(String catalog = {}, String schema = {}, String table = {})",
            catalog,
            schema,
            table));
    // TODO (PECO-1696): Implement getExportedKeys
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getCrossReference(String parentCatalog = {}, String parentSchema = {}, String parentTable = {}, String foreignCatalog = {}, String foreignSchema = {}, String foreignTable = {})",
            parentCatalog,
            parentSchema,
            parentTable,
            foreignCatalog,
            foreignSchema,
            foreignTable));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable)");
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    LOGGER.debug("public ResultSet getTypeInfo()");
    throwExceptionIfConnectionIsClosed();
    return this.session.getDatabricksMetadataClient().listTypeInfo(session);
  }

  @Override
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate) {
    LOGGER.debug(
        String.format(
            "public ResultSet getIndexInfo(String catalog = {}, String schema = {}, String table = {}, boolean unique = {}, boolean approximate = {})",
            catalog,
            schema,
            table,
            unique,
            approximate));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)");
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    LOGGER.debug(String.format("public boolean supportsResultSetType(int type = {%s})", type));
    throwExceptionIfConnectionIsClosed();
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    LOGGER.debug(
        String.format(
            "public boolean supportsResultSetConcurrency(int type = {}, int concurrency = {})",
            type,
            concurrency));
    throwExceptionIfConnectionIsClosed();
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    LOGGER.debug(String.format("public boolean ownUpdatesAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    LOGGER.debug(String.format("public boolean ownDeletesAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    LOGGER.debug(String.format("public boolean ownInsertsAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    LOGGER.debug(String.format("public boolean othersUpdatesAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    LOGGER.debug(String.format("public boolean othersDeletesAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    LOGGER.debug(String.format("public boolean updatesAreDetected(int type = {%s})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    LOGGER.debug(String.format("public boolean deletesAreDetected(int type = {%s})", type));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - deletesAreDetected(int type)");
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    LOGGER.debug(String.format("public boolean insertsAreDetected(int type = {%s})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    LOGGER.debug("public boolean supportsBatchUpdates()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getUDTs(String catalog = {%s}, String schemaPattern = {%s}, String typeNamePattern = {%s}, int[] types = {%s})",
            catalog, schemaPattern, typeNamePattern, Arrays.toString(types)));
    throwExceptionIfConnectionIsClosed();
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "getudts-metadata",
        Arrays.asList(
            "TYPE_CAT",
            "TYPE_SCHEM",
            "TYPE_NAME",
            "CLASS_NAME",
            "DATA_TYPE",
            "REMAKRS",
            "BASE_TYPE"),
        Arrays.asList("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
        Arrays.asList(
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR),
        Arrays.asList(128, 128, 128, 128, 128, 128, 128),
        new String[0][0],
        StatementType.METADATA);
  }

  @Override
  public Connection getConnection() throws SQLException {
    LOGGER.debug("public Connection getConnection()");
    return connection.getConnection();
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    LOGGER.debug("public boolean supportsSavepoints()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    LOGGER.debug("public boolean supportsNamedParameters()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    LOGGER.debug("public boolean supportsMultipleOpenResults()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    LOGGER.debug("public boolean supportsGetGeneratedKeys()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getSuperTypes(String catalog = {%s}, String schemaPattern = {%s}, String typeNamePattern = {%s})",
            catalog, schemaPattern, typeNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)");
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getSuperTables(String catalog = {%s}, String schemaPattern = {%s}, String tableNamePattern = {%s})",
            catalog, schemaPattern, tableNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getSuperTables(String catalog, String schemaPattern, String tableNamePattern)");
  }

  @Override
  public ResultSet getAttributes(
      String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getAttributes(String catalog = {%s}, String schemaPattern = {%s}, String typeNamePattern = {%s}, String attributeNamePattern = {%s})",
            catalog, schemaPattern, typeNamePattern, attributeNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)");
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    LOGGER.debug(
        String.format(
            "public boolean supportsResultSetHoldability(int holdability = {%s})", holdability));
    throwExceptionIfConnectionIsClosed();
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    LOGGER.debug("public int getResultSetHoldability()");
    throwExceptionIfConnectionIsClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    LOGGER.debug("public int getDatabaseMajorVersion()");
    return DATABASE_MAJOR_VERSION;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    LOGGER.debug("public int getDatabaseMinorVersion()");
    return DATABASE_MINOR_VERSION;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    LOGGER.debug("public int getJDBCMajorVersion()");
    return DriverUtil.getMajorVersion();
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    LOGGER.debug("public int getJDBCMinorVersion()");
    return DriverUtil.getMinorVersion();
  }

  @Override
  public int getSQLStateType() throws SQLException {
    LOGGER.debug("public int getSQLStateType()");
    throwExceptionIfConnectionIsClosed();
    return DatabaseMetaData.sqlStateSQL;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    LOGGER.debug("public boolean locatorsUpdateCopy()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    LOGGER.debug("public boolean supportsStatementPooling()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    LOGGER.debug("public RowIdLifetime getRowIdLifetime()");
    throwExceptionIfConnectionIsClosed();
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getSchemas(String catalog = {%s}, String schemaPattern = {%s})",
            catalog, schemaPattern));
    throwExceptionIfConnectionIsClosed();
    return session.getDatabricksMetadataClient().listSchemas(session, catalog, schemaPattern);
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    LOGGER.debug("public boolean supportsStoredFunctionsUsingCallSyntax()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    LOGGER.debug("public boolean autoCommitFailureClosesAllResultSets()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    LOGGER.debug("public ResultSet getClientInfoProperties()");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getClientInfoProperties()");
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    throwExceptionIfConnectionIsClosed();
    return session
        .getDatabricksMetadataClient()
        .listFunctions(session, catalog, schemaPattern, functionNamePattern);
  }

  @Override
  public ResultSet getFunctionColumns(
      String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public ResultSet getFunctionColumns(String catalog = {%s}, String schemaPattern = {%s}, String functionNamePattern = {%s}, String columnNamePattern = {%s})",
            catalog, schemaPattern, functionNamePattern, columnNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)");
  }

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
    LOGGER.debug(
        String.format(
            "public ResultSet getPseudoColumns(String catalog = {%s}, String schemaPattern = {%s}, String tableNamePattern = {%s}, String columnNamePattern = {%s})",
            catalog, schemaPattern, tableNamePattern, columnNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)");
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    LOGGER.debug("public boolean generatedKeyAlwaysReturned()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LOGGER.debug(String.format("public <T> T unwrap(Class<T> iface = {%s})", iface));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - unwrap(Class<T> iface)");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LOGGER.debug(String.format("public boolean isWrapperFor(Class<?> iface = {%s})", iface));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - isWrapperFor(Class<?> iface)");
  }

  private void throwExceptionIfConnectionIsClosed() throws SQLException {
    LOGGER.debug("private void throwExceptionIfConnectionIsClosed()");
    if (!connection.getSession().isOpen()) {
      throw new DatabricksSQLException("Connection closed!");
    }
  }
}
