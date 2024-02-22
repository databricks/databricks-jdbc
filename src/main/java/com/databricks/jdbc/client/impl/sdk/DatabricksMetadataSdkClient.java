package com.databricks.jdbc.client.impl.sdk;

import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.commons.util.WildcardUtil;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.core.ImmutableSqlParameter;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation for DatabricksMetadataClient using SDK client */
public class DatabricksMetadataSdkClient implements DatabricksMetadataClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSdkClient.class);

  private final DatabricksSdkClient sdkClient;

  public DatabricksMetadataSdkClient(DatabricksSdkClient sdkClient) {
    this.sdkClient = sdkClient;
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) {
    return null;
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    String showCatalogsSQL = "show catalogs";
    LOGGER.debug("SQL command to fetch catalogs: {}", showCatalogsSQL);

    ResultSet rs =
        sdkClient.executeStatement(
            showCatalogsSQL,
            session.getComputeResource(),
            new HashMap<Integer, ImmutableSqlParameter>(),
            StatementType.METADATA,
            session,
            null /* parentStatement */);
    List<List<Object>> rows = new ArrayList<>();
    while (rs.next()) {
      rows.add(Collections.singletonList(rs.getString(1)));
    }
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "getcatalogs-metadata",
        Collections.singletonList("TABLE_CAT"),
        Collections.singletonList("VARCHAR"),
        Collections.singletonList(Types.VARCHAR),
        Collections.singletonList(128),
        rows,
        StatementType.METADATA);
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    // Since catalog must be an identifier or all catalogs (null), we need not care about catalog
    // regex
    Queue<String> catalogs = new ConcurrentLinkedQueue<>();
    if (WildcardUtil.isMatchAnything(catalog)) {
      ResultSet rs = listCatalogs(session);
      while (rs.next()) {
        catalogs.add(rs.getString(1));
      }
    } else {
      catalogs.add(catalog);
    }
    // TODO: Remove post demo
    while (catalogs.size() > 5) catalogs.poll();

    List<List<Object>> rows = new CopyOnWriteArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(150);
    for (int i = 0; i < 150; i++) {
      executorService.submit(
          () -> {
            while (!catalogs.isEmpty()) {
              String currentCatalog = catalogs.poll();
              // TODO: Emoji characters are not being handled correctly by SDK/SEA, hence, skipping
              // for now
              //          if (WildcardUtil.containsEmoji(currentCatalog))
              //            return;
              String showSchemaSQL = "show schemas in `" + currentCatalog + "`";
              if (!WildcardUtil.isMatchAnything(schemaNamePattern)) {
                showSchemaSQL += " like '" + schemaNamePattern + "'";
              }
              LOGGER.debug("SQL command to fetch schemas: {}", showSchemaSQL);
              try {
                ResultSet rs =
                    sdkClient.executeStatement(
                        showSchemaSQL,
                        session.getComputeResource(),
                        new HashMap<Integer, ImmutableSqlParameter>(),
                        StatementType.METADATA,
                        session,
                        null /* parentStatement */);
                while (rs.next()) {
                  rows.add(Arrays.asList(rs.getString(1), currentCatalog));
                }
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    }
    executorService.shutdown();
    while (!executorService.isTerminated()) {
      // wait
    }
    rows.sort(
        Comparator.comparing((List<Object> i) -> i.get(1).toString())
            .thenComparing(i -> i.get(0).toString()));
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "metadata-statement",
        Arrays.asList("TABLE_SCHEM", "TABLE_CATALOG"),
        Arrays.asList("VARCHAR", "VARCHAR"),
        Arrays.asList(Types.VARCHAR, Types.VARCHAR),
        Arrays.asList(128, 128),
        rows,
        StatementType.METADATA);
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session, String catalog, String schemaNamePattern, String tableNamePattern)
      throws SQLException {

    Queue<Map.Entry<String, String>> catalogSchemaPairs = new ConcurrentLinkedQueue<>();
    if (WildcardUtil.isWildcard(schemaNamePattern) || WildcardUtil.isMatchAnything(catalog)) {
      ResultSet resultSet = listSchemas(session, catalog, schemaNamePattern);
      while (resultSet.next()) {
        catalogSchemaPairs.add(Map.entry(resultSet.getString(2), resultSet.getString(1)));
      }
    } else {
      catalogSchemaPairs.add(Map.entry(catalog, schemaNamePattern));
    }
    // TODO: Limit to 15 pairs to run quickly, remove after demo/find workaround
    while (catalogSchemaPairs.size() > 15) catalogSchemaPairs.poll();
    String tableWithContext = tableNamePattern == null ? "*" : tableNamePattern;

    List<List<Object>> rows = new CopyOnWriteArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(150);
    for (int i = 0; i < 150; i++) {
      executorService.submit(
          () -> {
            while (!catalogSchemaPairs.isEmpty()) {
              Map.Entry<String, String> currentPair = catalogSchemaPairs.poll();
              String currentCatalog = currentPair.getKey();
              String currentSchema = currentPair.getValue();
              String showTablesSQL = "show tables from " + currentCatalog + "." + currentSchema;
              if (!WildcardUtil.isMatchAnything(tableWithContext)) {
                showTablesSQL += " like '" + tableWithContext + "'";
              }
              LOGGER.debug("SQL command to fetch tables: {}", showTablesSQL);
              try {
                ResultSet rs =
                    sdkClient.executeStatement(
                        showTablesSQL,
                        session.getComputeResource(),
                        new HashMap<Integer, ImmutableSqlParameter>(),
                        StatementType.METADATA,
                        session,
                        null /* parentStatement */);
                while (rs.next()) {
                  rows.add(
                      Arrays.asList(
                          currentCatalog,
                          currentSchema,
                          rs.getString(2),
                          "TABLE",
                          null,
                          null,
                          null,
                          null,
                          null,
                          null));
                }
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    }
    executorService.shutdown();
    while (!executorService.isTerminated()) {
      // wait
    }
    // They are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME.
    rows.sort(
        Comparator.comparing((List<Object> i) -> i.get(3).toString())
            .thenComparing(i -> i.get(0).toString())
            .thenComparing(i -> i.get(1).toString())
            .thenComparing(i -> i.get(2).toString()));
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "gettables-metadata",
        Arrays.asList(
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "TABLE_TYPE",
            "REMARKS",
            "TYPE_CAT",
            "TYPE_SCHEM",
            "TYPE_NAME",
            "SELF_REFERENCING_COL_NAME",
            "REF_GENERATION"),
        Arrays.asList(
            "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR",
            "VARCHAR", "VARCHAR"),
        Arrays.asList(
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR,
            Types.VARCHAR),
        Arrays.asList(128, 128, 128, 128, 128, 128, 128, 128, 128, 128),
        rows,
        StatementType.METADATA);
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) {
    return null;
  }

  @Override
  public DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern)
      throws SQLException {
    ResultSet resultSet = listTables(session, catalog, schemaNamePattern, tableNamePattern);
    Queue<String[]> catalogSchemaTableCombinations = new ConcurrentLinkedQueue<>();
    while (resultSet.next()) {
      catalogSchemaTableCombinations.add(
          new String[] {resultSet.getString(1), resultSet.getString(2), resultSet.getString(3)});
    }

    List<List<Object>> rows = new CopyOnWriteArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(150);
    for (int i = 0; i < 150; i++) {
      executorService.submit(
          () -> {
            while (!catalogSchemaTableCombinations.isEmpty()) {
              String[] combination = catalogSchemaTableCombinations.poll();
              String showColumnsSQL =
                  "show columns in " + combination[0] + "." + combination[1] + "." + combination[2];
              LOGGER.debug("SQL command to fetch columns: {}", showColumnsSQL);
              try {
                ResultSet rs =
                    sdkClient.executeStatement(
                        showColumnsSQL,
                        session.getComputeResource(),
                        new HashMap<Integer, ImmutableSqlParameter>(),
                        StatementType.METADATA,
                        session,
                        null /* parentStatement */);
                while (rs.next()) {
                  if (rs.getString(1).matches(columnNamePattern)) {
                    rows.add(
                        Arrays.asList(
                            combination[0], combination[1], combination[2], rs.getString(1)));
                  }
                }
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    }
    executorService.shutdown();
    while (!executorService.isTerminated()) {
      // wait
    }
    // TODO: some columns are missing from result set, determine how to fill those
    rows.sort(
        Comparator.comparing((List<Object> i) -> i.get(0).toString())
            .thenComparing(i -> i.get(1).toString())
            .thenComparing(i -> i.get(2).toString()));
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        "metadata-statement",
        Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME"),
        Arrays.asList("VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR"),
        Arrays.asList(Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR),
        Arrays.asList(128, 128, 128, 128),
        rows,
        StatementType.METADATA);
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws SQLException {
    return null;
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    return null;
  }
}
