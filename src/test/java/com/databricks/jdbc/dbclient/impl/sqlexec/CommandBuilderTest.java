package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.util.WildcardUtil;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
    @DisplayName("Should generate correct SQL for fetching tables with wildcard catalog")
    void shouldGenerateCorrectSqlForTablesWithWildcardCatalog() throws SQLException {
      // Test with '*' wildcard
      CommandBuilder builder1 = new CommandBuilder("*", mockSession);
      String sql1 = builder1.getSQLString(CommandName.LIST_TABLES);
      assertEquals(SHOW_TABLES_IN_ALL_CATALOGS_SQL, sql1);

      // Test with '%' wildcard
      CommandBuilder builder2 = new CommandBuilder("%", mockSession);
      String sql2 = builder2.getSQLString(CommandName.LIST_TABLES);
      assertEquals(SHOW_TABLES_IN_ALL_CATALOGS_SQL, sql2);
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
  @DisplayName("Should throw exception for unsupported command")
  void shouldThrowExceptionForUnsupportedCommand() {
    CommandBuilder builder = new CommandBuilder(TEST_CATALOG, mockSession);

    CommandName mockCommand = mock(CommandName.class);

    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class, () -> builder.getSQLString(mockCommand));
  }
}
