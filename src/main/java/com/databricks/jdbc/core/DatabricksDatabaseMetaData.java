package com.databricks.jdbc.core;

import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.sql.*;
import java.util.*;

public class DatabricksDatabaseMetaData implements DatabaseMetaData {
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
    LoggingUtil.log(LogLevel.DEBUG, "public boolean allProceduresAreCallable()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean allTablesAreSelectable()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getURL() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getURL()");
    return this.session.getConnectionContext().getConnectionURL();
  }

  @Override
  public String getUserName() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getUserName()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.USER_NAME;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean isReadOnly()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean nullsAreSortedHigh()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean nullsAreSortedLow()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean nullsAreSortedAtStart()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean nullsAreSortedAtEnd()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getDatabaseProductName()");
    throwExceptionIfConnectionIsClosed();
    return PRODUCT_NAME;
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getDatabaseProductVersion()");
    throwExceptionIfConnectionIsClosed();
    return DATABASE_MAJOR_VERSION
        + DatabricksJdbcConstants.FULL_STOP
        + DATABASE_MINOR_VERSION
        + DatabricksJdbcConstants.FULL_STOP
        + DATABASE_PATCH_VERSION;
  }

  @Override
  public String getDriverName() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getDriverName()");
    throwExceptionIfConnectionIsClosed();
    return DRIVER_NAME;
  }

  @Override
  public String getDriverVersion() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getDriverVersion()");
    throwExceptionIfConnectionIsClosed();
    return DriverUtil.getVersion();
  }

  @Override
  public int getDriverMajorVersion() {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getDriverMajorVersion()");
  }

  @Override
  public int getDriverMinorVersion() {
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getDriverMinorVersion()");
  }

  @Override
  public boolean usesLocalFiles() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean usesLocalFiles()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean usesLocalFilePerTable()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsMixedCaseIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean storesUpperCaseIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean storesLowerCaseIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean storesMixedCaseIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsMixedCaseQuotedIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean storesUpperCaseQuotedIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean storesLowerCaseQuotedIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean storesMixedCaseQuotedIdentifiers()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getIdentifierQuoteString()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.IDENTIFIER_QUOTE_STRING;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getSQLKeywords()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.EMPTY_STRING;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getNumericFunctions()");
    throwExceptionIfConnectionIsClosed();
    return NUMERIC_FUNCTIONS;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getStringFunctions()");
    throwExceptionIfConnectionIsClosed();
    return STRING_FUNCTIONS;
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getSystemFunctions()");
    throwExceptionIfConnectionIsClosed();
    return SYSTEM_FUNCTIONS;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getTimeDateFunctions()");
    throwExceptionIfConnectionIsClosed();
    return TIME_DATE_FUNCTIONS;
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getSearchStringEscape()");
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getSearchStringEscape()");
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getExtraNameCharacters()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.EMPTY_STRING;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsAlterTableWithAddColumn()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsAlterTableWithDropColumn()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsColumnAliasing()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean nullPlusNonNullIsNull()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsConvert()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    // LoggingUtil.log(LogLevel.DEBUG,
    //     "public boolean supportsConvert(int fromType = {}, int toType = {})", fromType, toType);
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - supportsConvert(int fromType, int toType)");
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsTableCorrelationNames()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsDifferentTableCorrelationNames()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsExpressionsInOrderBy()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsOrderByUnrelated()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsGroupBy()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsGroupByUnrelated()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsGroupByBeyondSelect()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsLikeEscapeClause()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsMultipleResultSets()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsMultipleTransactions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsNonNullableColumns()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsMinimumSQLGrammar()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsCoreSQLGrammar()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsExtendedSQLGrammar()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsANSI92EntryLevelSQL()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsANSI92IntermediateSQL()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsANSI92FullSQL()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsIntegrityEnhancementFacility()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsOuterJoins()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsFullOuterJoins()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsLimitedOuterJoins()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getSchemaTerm()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.SCHEMA;
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getProcedureTerm()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.PROCEDURE;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getCatalogTerm()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.CATALOG;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean isCatalogAtStart()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public String getCatalogSeparator()");
    throwExceptionIfConnectionIsClosed();
    return DatabricksJdbcConstants.FULL_STOP;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSchemasInDataManipulation()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSchemasInProcedureCalls()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSchemasInTableDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSchemasInIndexDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSchemasInPrivilegeDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsCatalogsInDataManipulation()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsCatalogsInProcedureCalls()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsCatalogsInTableDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsCatalogsInIndexDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsCatalogsInPrivilegeDefinitions()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsPositionedDelete()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsPositionedUpdate()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSelectForUpdate()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsStoredProcedures()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSubqueriesInComparisons()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSubqueriesInExists()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSubqueriesInIns()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSubqueriesInQuantifieds()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsCorrelatedSubqueries()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsUnion()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsUnionAll()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsOpenCursorsAcrossCommit()");
    throwExceptionIfConnectionIsClosed();
    // Open cursors are not supported, however open statements are.
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsOpenCursorsAcrossRollback()");
    throwExceptionIfConnectionIsClosed();
    // Open cursors are not supported, however open statements are.
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsOpenStatementsAcrossCommit()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsOpenStatementsAcrossRollback()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxBinaryLiteralLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxCharLiteralLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxColumnNameLength()");
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxColumnsInGroupBy()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxColumnsInIndex()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxColumnsInOrderBy()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxColumnsInSelect()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxColumnsInTable()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxConnections()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxCursorNameLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxIndexLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxSchemaNameLength()");
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxProcedureNameLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxCatalogNameLength()");
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxRowSize()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean doesMaxRowSizeIncludeBlobs()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxStatementLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxStatements() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxStatements()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxTableNameLength()");
    throwExceptionIfConnectionIsClosed();
    return MAX_NAME_LENGTH;
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxTablesInSelect()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxUserNameLength()");
    throwExceptionIfConnectionIsClosed();
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getDefaultTransactionIsolation()");
    throwExceptionIfConnectionIsClosed();
    return Connection.TRANSACTION_READ_COMMITTED;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsTransactions()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public boolean supportsTransactionIsolationLevel(int level = {%s})", level));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - supportsTransactionIsolationLevel(int level)");
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, "public boolean supportsDataDefinitionAndDataManipulationTransactions()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsDataManipulationTransactionsOnly()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean dataDefinitionCausesTransactionCommit()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean dataDefinitionIgnoredInTransactions()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getProcedures(String catalog = {%s}, String schemaPattern = {%s}, String procedureNamePattern = {%s})",
            catalog, schemaPattern, procedureNamePattern));
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
    LoggingUtil.log(
        LogLevel.DEBUG,
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
    LoggingUtil.log(
        LogLevel.DEBUG,
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
    LoggingUtil.log(LogLevel.DEBUG, "public ResultSet getSchemas()");
    return getSchemas(null /* catalog */, null /* schema pattern */);
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public ResultSet getCatalogs()");
    throwExceptionIfConnectionIsClosed();
    return session.getDatabricksMetadataClient().listCatalogs(session);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public ResultSet getTableTypes()");
    throwExceptionIfConnectionIsClosed();
    return session.getDatabricksMetadataClient().listTableTypes(session);
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
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
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getColumnPrivileges(String catalog = {%s}, String schema = {%s}, String table = {%s}, String columnNamePattern = {%s})",
            catalog, schema, table, columnNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)");
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
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
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getBestRowIdentifier(String catalog = {%s}, String schema = {%s}, String table = {%s}, int scope = {%s}, boolean nullable = {%s})",
            catalog, schema, table, scope, nullable));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)");
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
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
    LoggingUtil.log(
        LogLevel.DEBUG,
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
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getImportedKeys(String catalog = {}, String schema = {}, String table = {})",
            catalog,
            schema,
            table));
    // TODO(PECO-1696): Implement this
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getExportedKeys(String catalog = {}, String schema = {}, String table = {})",
            catalog,
            schema,
            table));
    // TODO(PECO-1696): Implement this
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
    LoggingUtil.log(
        LogLevel.DEBUG,
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
    LoggingUtil.log(LogLevel.DEBUG, "public ResultSet getTypeInfo()");
    throwExceptionIfConnectionIsClosed();
    return this.session.getDatabricksMetadataClient().listTypeInfo(session);
  }

  @Override
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate) {
    LoggingUtil.log(
        LogLevel.DEBUG,
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
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public boolean supportsResultSetType(int type = {%s})", type));
    throwExceptionIfConnectionIsClosed();
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public boolean supportsResultSetConcurrency(int type = {}, int concurrency = {})",
            type,
            concurrency));
    throwExceptionIfConnectionIsClosed();
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public boolean ownUpdatesAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public boolean ownDeletesAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public boolean ownInsertsAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public boolean othersUpdatesAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public boolean othersDeletesAreVisible(int type = {})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    // LoggingUtil.log(LogLevel.DEBUG,"public boolean othersInsertsAreVisible(int type = {})",
    // type);
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public boolean updatesAreDetected(int type = {%s})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public boolean deletesAreDetected(int type = {%s})", type));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - deletesAreDetected(int type)");
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public boolean insertsAreDetected(int type = {%s})", type));
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsBatchUpdates()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getUDTs(String catalog = {%s}, String schemaPattern = {%s}, String typeNamePattern = {%s}, int[] types = {%s})",
            catalog, schemaPattern, typeNamePattern, types));
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
    LoggingUtil.log(LogLevel.DEBUG, "public Connection getConnection()");
    return connection.getConnection();
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsSavepoints()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsNamedParameters()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsMultipleOpenResults()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsGetGeneratedKeys()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getSuperTypes(String catalog = {%s}, String schemaPattern = {%s}, String typeNamePattern = {%s})",
            catalog, schemaPattern, typeNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)");
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
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
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getAttributes(String catalog = {%s}, String schemaPattern = {%s}, String typeNamePattern = {%s}, String attributeNamePattern = {%s})",
            catalog, schemaPattern, typeNamePattern, attributeNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)");
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public boolean supportsResultSetHoldability(int holdability = {%s})", holdability));
    throwExceptionIfConnectionIsClosed();
    return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getResultSetHoldability()");
    throwExceptionIfConnectionIsClosed();
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getDatabaseMajorVersion()");
    return DATABASE_MAJOR_VERSION;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getDatabaseMinorVersion()");
    return DATABASE_MINOR_VERSION;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getJDBCMajorVersion()");
    return DriverUtil.getMajorVersion();
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getJDBCMinorVersion()");
    return DriverUtil.getMinorVersion();
  }

  @Override
  public int getSQLStateType() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getSQLStateType()");
    throwExceptionIfConnectionIsClosed();
    return DatabaseMetaData.sqlStateSQL;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean locatorsUpdateCopy()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsStatementPooling()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public RowIdLifetime getRowIdLifetime()");
    throwExceptionIfConnectionIsClosed();
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getSchemas(String catalog = {%s}, String schemaPattern = {%s})",
            catalog, schemaPattern));
    throwExceptionIfConnectionIsClosed();
    return session.getDatabricksMetadataClient().listSchemas(session, catalog, schemaPattern);
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean supportsStoredFunctionsUsingCallSyntax()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean autoCommitFailureClosesAllResultSets()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public ResultSet getClientInfoProperties()");
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
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getFunctionColumns(String catalog = {%s}, String schemaPattern = {%s}, String functionNamePattern = {%s}, String columnNamePattern = {%s})",
            catalog, schemaPattern, functionNamePattern, columnNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)");
  }

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public ResultSet getPseudoColumns(String catalog = {%s}, String schemaPattern = {%s}, String tableNamePattern = {%s}, String columnNamePattern = {%s})",
            catalog, schemaPattern, tableNamePattern, columnNamePattern));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)");
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean generatedKeyAlwaysReturned()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public <T> T unwrap(Class<T> iface = {%s})", iface));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - unwrap(Class<T> iface)");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public boolean isWrapperFor(Class<?> iface = {%s})", iface));
    throw new UnsupportedOperationException(
        "Not implemented in DatabricksDatabaseMetaData - isWrapperFor(Class<?> iface)");
  }

  private void throwExceptionIfConnectionIsClosed() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "private void throwExceptionIfConnectionIsClosed()");
    if (!connection.getSession().isOpen()) {
      throw new DatabricksSQLException("Connection closed!");
    }
  }
}
