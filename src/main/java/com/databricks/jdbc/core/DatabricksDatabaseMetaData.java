package com.databricks.jdbc.core;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.commons.util.WildcardUtil;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksDatabaseMetaData implements DatabaseMetaData {
  public static final String DRIVER_NAME = "DatabricksJDBC";
  public static final String PRODUCT_NAME = "SparkSQL";
  public static final int DATABASE_MAJOR_VERSION = 3;
  public static final int DATABASE_MINOR_VERSION = 1;
  public static final int DATABASE_PATCH_VERSION = 1;
  public static final int JDBC_MAJOR_VERSION = 0;
  public static final int JDBC_MINOR_VERSION = 0;
  public static final int JDBC_PATCH_VERSION = 0;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksDatabaseMetaData.class);

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
    throw new UnsupportedOperationException("Not implemented");
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
    return JDBC_MAJOR_VERSION
        + DatabricksJdbcConstants.FULL_STOP
        + JDBC_MINOR_VERSION
        + DatabricksJdbcConstants.FULL_STOP
        + JDBC_PATCH_VERSION;
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
    throw new UnsupportedOperationException("Not implemented");
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
    LOGGER.debug(
        "public boolean supportsConvert(int fromType = {}, int toType = {})", fromType, toType);
    throw new UnsupportedOperationException("Not implemented");
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
    LOGGER.debug("public boolean supportsTransactionIsolationLevel(int level = {})", level);
    throw new UnsupportedOperationException("Not implemented");
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
        "public ResultSet getProcedures(String catalog = {}, String schemaPattern = {}, String procedureNamePattern = {})",
        catalog,
        schemaPattern,
        procedureNamePattern);
    // TODO: check once, simba returns empty result set as well
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
        "public ResultSet getProcedureColumns(String catalog = {}, String schemaPattern = {}, String procedureNamePattern = {}, String columnNamePattern = {})",
        catalog,
        schemaPattern,
        procedureNamePattern,
        columnNamePattern);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getTables(String catalog = {}, String schemaPattern = {}, String tableNamePattern = {}, String[] types = {})",
        catalog,
        schemaPattern,
        tableNamePattern,
        types);
    throwExceptionIfConnectionIsClosed();
    Map.Entry<String, String> pair = applyContext(catalog, schemaPattern);
    String catalogWithContext = pair.getKey();
    String schemaWithContext = pair.getValue();
    return session
        .getDatabricksMetadataClient()
        .listTables(session, catalogWithContext, schemaWithContext, tableNamePattern);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    LOGGER.debug("public ResultSet getSchemas()");
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
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "tabletype-metadata",
        Collections.singletonList("TABLE_TYPE"),
        Collections.singletonList("VARCHAR"),
        Collections.singletonList(Types.VARCHAR),
        Collections.singletonList(128),
        new String[][] {{"SYSTEM TABLE"}, {"TABLE"}, {"VIEW"}},
        StatementType.METADATA);
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getColumns(String catalog = {}, String schemaPattern = {}, String tableNamePattern = {}, String columnNamePattern = {})",
        catalog,
        schemaPattern,
        tableNamePattern,
        columnNamePattern);
    throwExceptionIfConnectionIsClosed();

    return session
        .getDatabricksMetadataClient()
        .listColumns(session, catalog, schemaPattern, tableNamePattern, columnNamePattern);
  }

  @Override
  public ResultSet getColumnPrivileges(
      String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    LOGGER.debug(
        "public ResultSet getColumnPrivileges(String catalog = {}, String schema = {}, String table = {}, String columnNamePattern = {})",
        catalog,
        schema,
        table,
        columnNamePattern);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getTablePrivileges(String catalog = {}, String schemaPattern = {}, String tableNamePattern = {})",
        catalog,
        schemaPattern,
        tableNamePattern);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getBestRowIdentifier(
      String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getBestRowIdentifier(String catalog = {}, String schema = {}, String table = {}, int scope = {}, boolean nullable = {})",
        catalog,
        schema,
        table,
        scope,
        nullable);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getVersionColumns(String catalog = {}, String schema = {}, String table = {})",
        catalog,
        schema,
        table);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    LOGGER.debug(
        "public ResultSet getPrimaryKeys(String catalog = {}, String schema = {}, String table = {})",
        catalog,
        schema,
        table);
    return session.getDatabricksMetadataClient().listPrimaryKeys(session, catalog, schema, table);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getImportedKeys(String catalog = {}, String schema = {}, String table = {})",
        catalog,
        schema,
        table);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getExportedKeys(String catalog = {}, String schema = {}, String table = {})",
        catalog,
        schema,
        table);
    throw new UnsupportedOperationException("Not implemented");
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
        "public ResultSet getCrossReference(String parentCatalog = {}, String parentSchema = {}, String parentTable = {}, String foreignCatalog = {}, String foreignSchema = {}, String foreignTable = {})",
        parentCatalog,
        parentSchema,
        parentTable,
        foreignCatalog,
        foreignSchema,
        foreignTable);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getTypeInfo() throws SQLException {
    LOGGER.debug("public ResultSet getTypeInfo()");
    throwExceptionIfConnectionIsClosed();

    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "typeinfo-metadata",
        Arrays.asList(
            "TYPE_NAME",
            "DATA_TYPE",
            "PRECISION",
            "LITERAL_PREFIX",
            "LITERAL_SUFFIX",
            "CREATE_PARAMS",
            "NULLABLE",
            "CASE_SENSITIVE",
            "SEARCHABLE",
            "UNSIGNED_ATTRIBUTE",
            "FIXED_PREC_SCALE",
            "AUTO_INCREMENT",
            "LOCAL_TYPE_NAME",
            "MINIMUM_SCALE",
            "MAXIMUM_SCALE",
            "SQL_DATA_TYPE",
            "SQL_DATETIME_SUB",
            "NUM_PREC_RADIX"),
        Arrays.asList(
            "VARCHAR",
            "INTEGER",
            "INTEGER",
            "VARCHAR",
            "VARCHAR",
            "VARCHAR",
            "SMALLINT",
            "BIT",
            "SMALLINT",
            "BIT",
            "BIT",
            "BIT",
            "VARCHAR",
            "SMALLINT",
            "SMALLINT",
            "INTEGER",
            "INTEGER",
            "INTEGER"),
        Arrays.asList(
            Types.VARCHAR,
            Types.INTEGER,
            Types.INTEGER,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.SMALLINT,
            Types.BOOLEAN,
            Types.SMALLINT,
            Types.BOOLEAN,
            Types.BOOLEAN,
            Types.BOOLEAN,
            Types.VARCHAR,
            Types.SMALLINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.INTEGER,
            Types.INTEGER),
        Arrays.asList(128, 10, 10, 128, 128, 128, 5, 1, 5, 1, 1, 1, 128, 5, 5, 10, 10, 10),
        new Object[][] {
          {
            "TINYINT",
            Types.TINYINT,
            3,
            null,
            null,
            null,
            typeNullable,
            false,
            typePredBasic,
            false,
            false,
            null,
            "TINYINT",
            0,
            0,
            Types.TINYINT,
            null,
            10
          },
          {
            "BIGINT",
            Types.BIGINT,
            19,
            null,
            null,
            null,
            typeNullable,
            false,
            typePredBasic,
            false,
            false,
            null,
            "BIGINT",
            0,
            0,
            Types.BIGINT,
            null,
            10
          },
          {
            "BINARY",
            Types.BINARY,
            32767,
            "0x",
            null,
            "LENGTH",
            typeNullable,
            false,
            typePredNone,
            null,
            false,
            null,
            "BINARY",
            null,
            null,
            Types.BINARY,
            null,
            null
          },
          {
            "CHAR",
            Types.CHAR,
            255,
            "'",
            "'",
            "LENGTH",
            typeNullable,
            true,
            typeSearchable,
            null,
            false,
            null,
            "CHAR",
            null,
            null,
            Types.CHAR,
            null,
            null
          },
          {
            "DECIMAL",
            Types.DECIMAL,
            38,
            null,
            null,
            null,
            typeNullable,
            false,
            typePredBasic,
            false,
            false,
            null,
            "DECIMAL",
            0,
            0,
            Types.DECIMAL,
            null,
            10
          },
          {
            "INT",
            Types.INTEGER,
            10,
            null,
            null,
            null,
            typeNullable,
            false,
            typePredBasic,
            false,
            false,
            null,
            "INT",
            0,
            0,
            Types.INTEGER,
            null,
            10
          },
          {
            "SMALLINT",
            Types.SMALLINT,
            5,
            null,
            null,
            null,
            typeNullable,
            false,
            typePredBasic,
            false,
            false,
            null,
            "SMALLINT",
            0,
            0,
            Types.SMALLINT,
            null,
            10
          },
          {
            "FLOAT",
            Types.FLOAT,
            7,
            null,
            null,
            null,
            typeNullable,
            false,
            typePredBasic,
            false,
            false,
            null,
            "FLOAT",
            null,
            null,
            Types.FLOAT,
            null,
            2
          },
          {
            "DOUBLE",
            Types.DOUBLE,
            15,
            null,
            null,
            null,
            typeNullable,
            false,
            typePredBasic,
            false,
            false,
            null,
            "DOUBLE",
            null,
            null,
            Types.DOUBLE,
            null,
            2
          },
          {
            "ARRAY",
            Types.VARCHAR,
            32767,
            "'",
            "'",
            "Type",
            typeNullable,
            false,
            typeSearchable,
            null,
            false,
            null,
            "ARRAY",
            null,
            null,
            Types.VARCHAR,
            null,
            null
          },
          {
            "MAP",
            Types.VARCHAR,
            32767,
            "'",
            "'",
            "Key,Value",
            typeNullable,
            false,
            typeSearchable,
            null,
            false,
            null,
            "MAP",
            null,
            null,
            Types.VARCHAR,
            null,
            null
          },
          {
            "STRING",
            Types.VARCHAR,
            510,
            "'",
            "'",
            "max length",
            typeNullable,
            true,
            typeSearchable,
            null,
            false,
            null,
            "STRING",
            null,
            null,
            Types.VARCHAR,
            null,
            null
          },
          {
            "STRUCT",
            Types.VARCHAR,
            32767,
            "'",
            "'",
            "Column Type, ...",
            typeNullable,
            false,
            typeSearchable,
            null,
            false,
            null,
            "STRUCT",
            null,
            null,
            Types.VARCHAR,
            null,
            null
          },
          {
            "VARCHAR",
            Types.VARCHAR,
            510,
            "'",
            "'",
            "max length",
            typeNullable,
            true,
            typeSearchable,
            null,
            false,
            null,
            "VARCHAR",
            null,
            null,
            Types.VARCHAR,
            null,
            null
          },
          {
            "BOOLEAN",
            Types.BOOLEAN,
            1,
            null,
            null,
            null,
            typeNullable,
            true,
            typePredBasic,
            null,
            false,
            null,
            "BOOLEAN",
            null,
            null,
            Types.BOOLEAN,
            null,
            null
          },
          {
            "DATE",
            Types.DATE,
            10,
            "'",
            "'",
            null,
            typeNullable,
            false,
            typeSearchable,
            null,
            false,
            null,
            "DATE",
            null,
            null,
            Types.DATE,
            null,
            null
          },
          {
            "TIMESTAMP",
            Types.TIMESTAMP,
            29,
            "'",
            "'",
            null,
            typeNullable,
            false,
            typeSearchable,
            null,
            false,
            null,
            "TIMESTAMP",
            0,
            0,
            Types.DATE,
            null,
            null
          }
        },
        StatementType.METADATA);
  }

  @Override
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getIndexInfo(String catalog = {}, String schema = {}, String table = {}, boolean unique = {}, boolean approximate = {})",
        catalog,
        schema,
        table,
        unique,
        approximate);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsResultSetType(int type) throws SQLException {
    LOGGER.debug("public boolean supportsResultSetType(int type = {})", type);
    throwExceptionIfConnectionIsClosed();
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    LOGGER.debug(
        "public boolean supportsResultSetConcurrency(int type = {}, int concurrency = {})",
        type,
        concurrency);
    throwExceptionIfConnectionIsClosed();
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    LOGGER.debug("public boolean ownUpdatesAreVisible(int type = {})", type);
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    LOGGER.debug("public boolean ownDeletesAreVisible(int type = {})", type);
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    LOGGER.debug("public boolean ownInsertsAreVisible(int type = {})", type);
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    LOGGER.debug("public boolean othersUpdatesAreVisible(int type = {})", type);
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    LOGGER.debug("public boolean othersDeletesAreVisible(int type = {})", type);
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    LOGGER.debug("public boolean othersInsertsAreVisible(int type = {})", type);
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    LOGGER.debug("public boolean updatesAreDetected(int type = {})", type);
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    LOGGER.debug("public boolean deletesAreDetected(int type = {})", type);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    LOGGER.debug("public boolean insertsAreDetected(int type = {})", type);
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
        "public ResultSet getUDTs(String catalog = {}, String schemaPattern = {}, String typeNamePattern = {}, int[] types = {})",
        catalog,
        schemaPattern,
        typeNamePattern,
        types);
    // TODO: implement, returning only empty set for now
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
        "public ResultSet getSuperTypes(String catalog = {}, String schemaPattern = {}, String typeNamePattern = {})",
        catalog,
        schemaPattern,
        typeNamePattern);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getSuperTables(String catalog = {}, String schemaPattern = {}, String tableNamePattern = {})",
        catalog,
        schemaPattern,
        tableNamePattern);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getAttributes(
      String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getAttributes(String catalog = {}, String schemaPattern = {}, String typeNamePattern = {}, String attributeNamePattern = {})",
        catalog,
        schemaPattern,
        typeNamePattern,
        attributeNamePattern);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    LOGGER.debug("public boolean supportsResultSetHoldability(int holdability = {})", holdability);
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
    return JDBC_MAJOR_VERSION;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    LOGGER.debug("public int getJDBCMinorVersion()");
    return JDBC_MINOR_VERSION;
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
        "public ResultSet getSchemas(String catalog = {}, String schemaPattern = {})",
        catalog,
        schemaPattern);
    throwExceptionIfConnectionIsClosed();
    Map.Entry<String, String> pair = applyContext(catalog, schemaPattern);
    String catalogWithContext = pair.getKey();
    String schemaWithContext = pair.getValue();

    return session
        .getDatabricksMetadataClient()
        .listSchemas(session, catalogWithContext, schemaWithContext);
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
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getFunctions(String catalog = {}, String schemaPattern = {}, String functionNamePattern = {})",
        catalog,
        schemaPattern,
        functionNamePattern);
    throwExceptionIfConnectionIsClosed();
    // TODO: implement, returning only empty set for now
    throwExceptionIfConnectionIsClosed();
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "getfunctions-metadata",
        Arrays.asList(
            "FUNCTION_CAT",
            "FUNCTION_SCHEM",
            "FUNCTION_NAME",
            "REMARKS",
            "FUNCTION_TYPE",
            "SPECIFIC_NAME"),
        Arrays.asList("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
        Arrays.asList(
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR),
        Arrays.asList(128, 128, 128, 128, 128, 128),
        new Object[0][0],
        StatementType.METADATA);

    //    // TODO: Handle null catalog, schema, function behaviour
    //
    //    String showSchemaSQL = "show functions in " + catalog + "." + schemaPattern + " like '" +
    // functionNamePattern + "'";
    //    return session.getDatabricksClient().executeStatement(showSchemaSQL,
    // session.getWarehouseId(),
    //        new HashMap<Integer, ImmutableSqlParameter>(), StatementType.METADATA, session);
  }

  @Override
  public ResultSet getFunctionColumns(
      String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getFunctionColumns(String catalog = {}, String schemaPattern = {}, String functionNamePattern = {}, String columnNamePattern = {})",
        catalog,
        schemaPattern,
        functionNamePattern,
        columnNamePattern);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    LOGGER.debug(
        "public ResultSet getPseudoColumns(String catalog = {}, String schemaPattern = {}, String tableNamePattern = {}, String columnNamePattern = {})",
        catalog,
        schemaPattern,
        tableNamePattern,
        columnNamePattern);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    LOGGER.debug("public boolean generatedKeyAlwaysReturned()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LOGGER.debug("public <T> T unwrap(Class<T> iface = {})", iface);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LOGGER.debug("public boolean isWrapperFor(Class<?> iface = {})", iface);
    throw new UnsupportedOperationException("Not implemented");
  }

  private void throwExceptionIfConnectionIsClosed() throws SQLException {
    LOGGER.debug("private void throwExceptionIfConnectionIsClosed()");
    if (!connection.getSession().isOpen()) {
      throw new DatabricksSQLException("Connection closed!");
    }
  }

  private Map.Entry<String, String> applyContext(String catalog, String schema)
      throws SQLException {
    LOGGER.debug(
        "private Map.Entry<String, String> applyContext(String catalog = {}, String schema = {})",
        catalog,
        schema);
    if (catalog == null) {
      catalog = connection.getConnection().getCatalog();
    }
    // If catalog is not set in context, look at all catalogs
    if (catalog == null) {
      catalog = "*";
    }
    if (schema == null) {
      schema = connection.getConnection().getSchema();
    }
    // If schema is not set in context, look at all schemas
    if (schema == null) {
      schema = "*";
    }
    return Map.entry(catalog, schema);
  }
}
