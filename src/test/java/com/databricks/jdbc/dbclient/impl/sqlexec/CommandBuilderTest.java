package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.util.WildcardUtil;
import com.databricks.jdbc.exception.DatabricksValidationException;
import java.sql.SQLException;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CommandBuilderTest {

  @Mock private IDatabricksSession mockSession;

  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_SCHEMA = "test_schema";
  private static final String TEST_TABLE = "test_table";
  private static final String TEST_SESSION_CONTEXT = "test_session_context";

  @BeforeEach
  void setUp() {
    when(mockSession.toString()).thenReturn(TEST_SESSION_CONTEXT);
  }

  @ParameterizedTest
  @MethodSource("sqlPatternLiterals")
  void shouldEscapePatternsAsSqlLiterals(
      CommandName command, String catalog, String prefix, String pattern, String literal)
      throws SQLException {
    CommandBuilder builder =
        new CommandBuilder(catalog, mockSession)
            .setSchemaPattern(pattern)
            .setTablePattern(pattern)
            .setColumnPattern(pattern)
            .setFunctionPattern(pattern);
    String clauses = "";
    if (literal != null) {
      switch (command) {
        case LIST_SCHEMAS:
          clauses = " LIKE '" + literal + "'";
          break;
        case LIST_TABLES:
        case LIST_FUNCTIONS:
          clauses = " SCHEMA LIKE '" + literal + "' LIKE '" + literal + "'";
          break;
        case LIST_COLUMNS:
          clauses =
              " SCHEMA LIKE '" + literal + "' TABLE LIKE '" + literal + "' LIKE '" + literal + "'";
          break;
        default:
          fail("Unexpected command: " + command);
      }
    }
    String expected = prefix + clauses;
    assertEquals(expected, builder.getSQLString(command));
    assertEquals(expected, builder.getSQLString(command));
  }

  static Stream<Arguments> sqlPatternLiterals() {
    Object[][] commands = {
      {CommandName.LIST_SCHEMAS, TEST_CATALOG, "SHOW SCHEMAS IN `test_catalog`"},
      {CommandName.LIST_TABLES, TEST_CATALOG, "SHOW TABLES IN CATALOG `test_catalog`"},
      {CommandName.LIST_COLUMNS, TEST_CATALOG, "SHOW COLUMNS IN CATALOG `test_catalog`"},
      {CommandName.LIST_FUNCTIONS, TEST_CATALOG, "SHOW FUNCTIONS IN CATALOG `test_catalog`"},
      {CommandName.LIST_SCHEMAS, null, "SHOW SCHEMAS IN ALL CATALOGS"},
      {CommandName.LIST_TABLES, null, "SHOW TABLES IN ALL CATALOGS"},
      {CommandName.LIST_COLUMNS, null, "SHOW COLUMNS IN ALL CATALOGS"}
    };
    String[][] patterns = {
      {"a\\\\b", "a\\\\\\\\b"},
      {"O'Brien", "O\\'Brien"},
      {"a\\\\'b", "a\\\\\\\\\\'b"},
      {"a\\\\b%_", "a\\\\\\\\b*."},
      {"a\\%b\\_", "a%b_"},
      {"", ""},
      {null, null}
    };
    return Stream.of(commands)
        .flatMap(
            command ->
                Stream.of(patterns)
                    .map(
                        pattern ->
                            Arguments.of(
                                command[0], command[1], command[2], pattern[0], pattern[1])));
  }

  @Test
  void shouldReplacePatternWithoutAccumulatingSqlEscapes() throws SQLException {
    CommandBuilder builder =
        new CommandBuilder(TEST_CATALOG, mockSession).setSchemaPattern("a\\\\b");
    assertEquals(
        "SHOW SCHEMAS IN `test_catalog` LIKE 'a\\\\\\\\b'",
        builder.getSQLString(CommandName.LIST_SCHEMAS));
    builder.setSchemaPattern("c\\\\d");
    assertEquals(
        "SHOW SCHEMAS IN `test_catalog` LIKE 'c\\\\\\\\d'",
        builder.getSQLString(CommandName.LIST_SCHEMAS));
  }

  @Nested
  @DisplayName("Tests for LIST_PRIMARY_KEYS command")
  class ListPrimaryKeysTests {

    @Test
    @DisplayName("Should generate correct SQL for fetching primary keys")
    void shouldGenerateCorrectSqlForPrimaryKeys() throws SQLException {
      CommandBuilder builder =
          new CommandBuilder(TEST_CATALOG, mockSession).setSchema(TEST_SCHEMA).setTable(TEST_TABLE);

      String sql = builder.getSQLString(CommandName.LIST_PRIMARY_KEYS);

      String expectedSql =
          String.format(SHOW_PRIMARY_KEYS_SQL, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
      assertEquals(expectedSql, sql);
    }

    @Test
    @DisplayName("Should escape backticks in identifiers for primary keys")
    void shouldEscapeBackticksInIdentifiersForPrimaryKeys() throws SQLException {
      CommandBuilder builder =
          new CommandBuilder("cat`alog", mockSession).setSchema("sch`ema").setTable("tab`le");

      String sql = builder.getSQLString(CommandName.LIST_PRIMARY_KEYS);

      assertEquals("SHOW KEYS IN CATALOG `cat``alog` IN SCHEMA `sch``ema` IN TABLE `tab``le`", sql);
    }

    @Test
    @DisplayName("Should throw SQLException when catalog is null for primary keys")
    void shouldThrowExceptionWhenCatalogIsNullForPrimaryKeys() {
      CommandBuilder builder =
          new CommandBuilder(null, mockSession).setSchema(TEST_SCHEMA).setTable(TEST_TABLE);

      assertThrows(SQLException.class, () -> builder.getSQLString(CommandName.LIST_PRIMARY_KEYS));
    }

    @Test
    @DisplayName("Should throw SQLException when schema is null for primary keys")
    void shouldThrowExceptionWhenSchemaIsNullForPrimaryKeys() {
      CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession).setTable(TEST_TABLE);

      assertThrows(SQLException.class, () -> builder.getSQLString(CommandName.LIST_PRIMARY_KEYS));
    }

    @Test
    @DisplayName("Should throw SQLException when table is null for primary keys")
    void shouldThrowExceptionWhenTableIsNullForPrimaryKeys() {
      CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession).setSchema(TEST_SCHEMA);

      assertThrows(SQLException.class, () -> builder.getSQLString(CommandName.LIST_PRIMARY_KEYS));
    }
  }

  @Nested
  @DisplayName("Tests for LIST_TABLES command")
  class ListTablesTests {

    @Test
    @DisplayName("Should generate correct SQL for fetching tables with catalog")
    void shouldGenerateCorrectSqlForTablesWithCatalog() throws SQLException {
      CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession);

      String sql = builder.getSQLString(CommandName.LIST_TABLES);

      String expectedSql = String.format(SHOW_TABLES_SQL, TEST_CATALOG);
      assertEquals(expectedSql, sql);
    }

    @Test
    @DisplayName("Should generate correct SQL for fetching tables with catalog and schema pattern")
    void shouldGenerateCorrectSqlForTablesWithCatalogAndSchemaPattern() throws SQLException {
      String schemaPattern = "test_schema%";
      String hiveSchemaPattern = WildcardUtil.jdbcPatternToHive(schemaPattern);

      CommandBuilder builder =
          new CommandBuilder(TEST_CATALOG, mockSession).setSchemaPattern(schemaPattern);

      String sql = builder.getSQLString(CommandName.LIST_TABLES);

      String expectedSql =
          String.format(SHOW_TABLES_SQL.concat(SCHEMA_LIKE_SQL), TEST_CATALOG, hiveSchemaPattern);
      assertEquals(expectedSql, sql);
    }

    @Test
    @DisplayName(
        "Should generate correct SQL for fetching tables with catalog, schema pattern, and table pattern")
    void shouldGenerateCorrectSqlForTablesWithCatalogSchemaAndTablePattern() throws SQLException {
      String schemaPattern = "test_schema%";
      String tablePattern = "test_table%";
      String hiveSchemaPattern = WildcardUtil.jdbcPatternToHive(schemaPattern);
      String hiveTablePattern = WildcardUtil.jdbcPatternToHive(tablePattern);

      CommandBuilder builder =
          new CommandBuilder(TEST_CATALOG, mockSession)
              .setSchemaPattern(schemaPattern)
              .setTablePattern(tablePattern);

      String sql = builder.getSQLString(CommandName.LIST_TABLES);

      String expectedSql =
          String.format(
              SHOW_TABLES_SQL.concat(SCHEMA_LIKE_SQL).concat(LIKE_SQL),
              TEST_CATALOG,
              hiveSchemaPattern,
              hiveTablePattern);
      assertEquals(expectedSql, sql);
    }

    @Test
    @DisplayName("Should generate correct SQL for fetching tables from all catalogs")
    void shouldGenerateCorrectSqlForTablesFromAllCatalogs() throws SQLException {
      CommandBuilder builder = new CommandBuilder(null, mockSession);

      String sql = builder.getSQLString(CommandName.LIST_TABLES);

      assertEquals(SHOW_TABLES_IN_ALL_CATALOGS_SQL, sql);
    }

    @Test
    @DisplayName("Should treat wildcard catalog as literal per JDBC spec (catalog is exact match)")
    void shouldTreatWildcardCatalogAsLiteral() throws SQLException {
      // Per JDBC spec, catalog is an exact match — '*' and '%' are literal catalog names,
      // not wildcards. Only null means "do not narrow the search".
      CommandBuilder builder1 = new CommandBuilder("*", mockSession);
      String sql1 = builder1.getSQLString(CommandName.LIST_TABLES);
      assertEquals(String.format(SHOW_TABLES_SQL, "*"), sql1);
    }

    @Test
    @DisplayName("Should escape backticks in catalog identifier for tables")
    void shouldEscapeBackticksInCatalogIdentifierForTables() throws SQLException {
      CommandBuilder builder = new CommandBuilder("cat`alog", mockSession);

      String sql = builder.getSQLString(CommandName.LIST_TABLES);

      assertEquals("SHOW TABLES IN CATALOG `cat``alog`", sql);
    }

    @Test
    @DisplayName("Should generate SCHEMA LIKE clause for empty string schema pattern")
    void shouldGenerateSchemaLikeClauseForEmptyStringSchemaPattern() throws SQLException {
      CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession).setSchemaPattern("");

      String sql = builder.getSQLString(CommandName.LIST_TABLES);

      String expectedSql = String.format(SHOW_TABLES_SQL.concat(SCHEMA_LIKE_SQL), TEST_CATALOG, "");
      assertEquals(expectedSql, sql);
    }

    @Test
    @DisplayName("Should NOT generate SCHEMA LIKE clause for null schema pattern")
    void shouldNotGenerateSchemaLikeClauseForNullSchemaPattern() throws SQLException {
      CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession).setSchemaPattern(null);

      String sql = builder.getSQLString(CommandName.LIST_TABLES);

      String expectedSql = String.format(SHOW_TABLES_SQL, TEST_CATALOG);
      assertEquals(expectedSql, sql);
    }

    @Test
    @DisplayName("Should generate LIKE clause for empty string table pattern")
    void shouldGenerateLikeClauseForEmptyStringTablePattern() throws SQLException {
      CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession).setTablePattern("");

      String sql = builder.getSQLString(CommandName.LIST_TABLES);

      String expectedSql = String.format(SHOW_TABLES_SQL.concat(LIKE_SQL), TEST_CATALOG, "");
      assertEquals(expectedSql, sql);
    }
  }

  @Nested
  @DisplayName("Tests for LIST_FOREIGN_KEYS command")
  class ListForeignKeysTests {

    @Test
    @DisplayName("Should generate correct SQL for fetching foreign keys")
    void shouldGenerateCorrectSqlForForeignKeys() throws SQLException {
      CommandBuilder builder =
          new CommandBuilder(TEST_CATALOG, mockSession).setSchema(TEST_SCHEMA).setTable(TEST_TABLE);

      String sql = builder.getSQLString(CommandName.LIST_FOREIGN_KEYS);

      String expectedSql =
          String.format(SHOW_FOREIGN_KEYS_SQL, TEST_CATALOG, TEST_SCHEMA, TEST_TABLE);
      assertEquals(expectedSql, sql);
    }

    @Test
    @DisplayName("Should escape backticks in identifiers for foreign keys")
    void shouldEscapeBackticksInIdentifiersForForeignKeys() throws SQLException {
      CommandBuilder builder =
          new CommandBuilder("cat`alog", mockSession).setSchema("sch`ema").setTable("tab`le");

      String sql = builder.getSQLString(CommandName.LIST_FOREIGN_KEYS);

      assertEquals(
          "SHOW FOREIGN KEYS IN CATALOG `cat``alog` IN SCHEMA `sch``ema` IN TABLE `tab``le`", sql);
    }

    @Test
    @DisplayName("Should throw SQLException when catalog is null for foreign keys")
    void shouldThrowExceptionWhenCatalogIsNullForForeignKeys() {
      CommandBuilder builder =
          new CommandBuilder(null, mockSession).setSchema(TEST_SCHEMA).setTable(TEST_TABLE);

      assertThrows(SQLException.class, () -> builder.getSQLString(CommandName.LIST_FOREIGN_KEYS));
    }

    @Test
    @DisplayName("Should throw SQLException when schema is null for foreign keys")
    void shouldThrowExceptionWhenSchemaIsNullForForeignKeys() {
      CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession).setTable(TEST_TABLE);

      assertThrows(SQLException.class, () -> builder.getSQLString(CommandName.LIST_FOREIGN_KEYS));
    }

    @Test
    @DisplayName("Should throw SQLException when table is null for foreign keys")
    void shouldThrowExceptionWhenTableIsNullForForeignKeys() {
      CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession).setSchema(TEST_SCHEMA);

      assertThrows(SQLException.class, () -> builder.getSQLString(CommandName.LIST_FOREIGN_KEYS));
    }
  }

  @Test
  @DisplayName("Should escape backticks in catalog identifiers for other metadata commands")
  void shouldEscapeBackticksInCatalogIdentifiersForOtherMetadataCommands() throws SQLException {
    CommandBuilder builder = new CommandBuilder("cat`alog", mockSession);

    assertEquals("SHOW SCHEMAS IN `cat``alog`", builder.getSQLString(CommandName.LIST_SCHEMAS));
    assertEquals(
        "SHOW COLUMNS IN CATALOG `cat``alog`", builder.getSQLString(CommandName.LIST_COLUMNS));
    assertEquals(
        "SHOW FUNCTIONS IN CATALOG `cat``alog`", builder.getSQLString(CommandName.LIST_FUNCTIONS));
  }

  @Test
  @DisplayName("Should generate SHOW COLUMNS IN ALL CATALOGS when catalog is null")
  void shouldGenerateCorrectSqlForColumnsFromAllCatalogs() throws SQLException {
    CommandBuilder builder = new CommandBuilder(null, mockSession);

    assertEquals(SHOW_COLUMNS_IN_ALL_CATALOGS_SQL, builder.getSQLString(CommandName.LIST_COLUMNS));
  }

  @Test
  @DisplayName(
      "Should append SCHEMA/TABLE/column LIKE clauses to SHOW COLUMNS IN ALL CATALOGS when catalog is null")
  void shouldGenerateCorrectSqlForColumnsFromAllCatalogsWithPatterns() throws SQLException {
    CommandBuilder builder =
        new CommandBuilder(null, mockSession)
            .setSchemaPattern("testSchema")
            .setTablePattern("testTable")
            .setColumnPattern("testColumn");

    assertEquals(
        "SHOW COLUMNS IN ALL CATALOGS SCHEMA LIKE 'testSchema' TABLE LIKE 'testTable' LIKE 'testColumn'",
        builder.getSQLString(CommandName.LIST_COLUMNS));
  }

  @Test
  @DisplayName("Should throw exception for unsupported command")
  void shouldThrowExceptionForUnsupportedCommand() {
    CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession);

    CommandName mockCommand = mock(CommandName.class);

    assertThrows(DatabricksValidationException.class, () -> builder.getSQLString(mockCommand));
  }
}
