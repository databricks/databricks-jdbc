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

/**
 * This is for the new SQL commands added in runtime. Note that the DatabricksMetadataSdkClient
 * will be replaced by this class once runtime code is merged and this class is tested end to end.
 * https://docs.google.com/document/d/1E28o7jyPIp6_byZHGD5Eyc4uwGVSydX5o9PaiSY1V4s/edit#heading=h.681k0yimshae
 */
public class DatabricksNewMetadataSdkClient implements DatabricksMetadataClient {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DatabricksNewMetadataSdkClient.class);
  private static final Integer MAX_THREADS = 150;
  private final DatabricksSdkClient sdkClient;

  public DatabricksNewMetadataSdkClient(DatabricksSdkClient sdkClient) {
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
            session.getWarehouseId(),
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
    // Since catalog must be an identifier or all catalogs, we need not care about catalog
    // regex
    Queue<String> catalogs = getCatalogs(catalog, session);

    List<List<Object>> rows = new CopyOnWriteArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREADS);
    for (int i = 0; i < MAX_THREADS; i++) {
      executorService.submit(
          () -> {
            while (!catalogs.isEmpty()) {
              String currentCatalog = catalogs.poll();

              String showSchemaSQL = String.format("show schemas in `%s`", currentCatalog);
              if (!WildcardUtil.isMatchAnything(schemaNamePattern)) {
                showSchemaSQL += String.format(" like `%s`", schemaNamePattern);
              }
              System.out.println("Schemas is " + showSchemaSQL);
              LOGGER.debug("SQL command to fetch schemas: {}", showSchemaSQL);
              try {
                ResultSet rs =
                    sdkClient.executeStatement(
                        showSchemaSQL,
                        session.getWarehouseId(),
                        new HashMap<Integer, ImmutableSqlParameter>(),
                        StatementType.METADATA,
                        session,
                        null /* parentStatement */);
                while (rs.next()) {
                  rows.add(
                      Arrays.asList(
                          rs.getString(2),
                          currentCatalog)); // Todo: verify the index once runtime commands are
                  // added
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
    Queue<String> catalogs = getCatalogs(catalog, session);
    List<List<Object>> rows = new CopyOnWriteArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREADS);
    for (int i = 0; i < MAX_THREADS; i++) {
      executorService.submit(
          () -> {
            while (!catalogs.isEmpty()) {
              String currentCatalog = catalogs.poll();
              String showTablesSQL = String.format("show jdbc_tables in `%s`", currentCatalog);
              if (!WildcardUtil.isMatchAnything(tableNamePattern)) {
                showTablesSQL += String.format(" like `%s`", tableNamePattern);
              }
              if (!WildcardUtil.isMatchAnything(schemaNamePattern)) {
                showTablesSQL += String.format(" schema like `%s`", schemaNamePattern);
              }
              LOGGER.debug("SQL command to fetch tables: {}", showTablesSQL);
              try {
                ResultSet rs =
                    sdkClient.executeStatement(
                        showTablesSQL,
                        session.getWarehouseId(),
                        new HashMap<Integer, ImmutableSqlParameter>(),
                        StatementType.METADATA,
                        session,
                        null /* parentStatement */);
                while (rs.next()) {
                  rows.add(Arrays.asList(rs.getString(2), currentCatalog));
                }
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    }
    executorService.shutdown();
    // Todo : Add a better way to check if terminated below.
    while (!executorService.isTerminated()) {
      // wait
    }
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
    ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREADS);
    List<List<Object>> rows = new CopyOnWriteArrayList<>();
    Queue<String> catalogs = getCatalogs(catalog, session);
    for (int i = 0; i < MAX_THREADS; i++) {
      executorService.submit(
          () -> {
            while (!catalogs.isEmpty()) {
              String currentCatalog = catalogs.poll();
              String showColumnsSQL = String.format("show jdbc_columns in `%s`", currentCatalog);

              if (!WildcardUtil.isMatchAnything(columnNamePattern)) {
                showColumnsSQL += String.format(" like `%s`", columnNamePattern);
              }
              if (!WildcardUtil.isMatchAnything(tableNamePattern)) {
                showColumnsSQL += String.format(" table like `%s`", tableNamePattern);
              }
              if (!WildcardUtil.isMatchAnything(schemaNamePattern)) {
                showColumnsSQL += String.format(" schema like `%s`", schemaNamePattern);
              }
              LOGGER.debug("SQL command to fetch columns: {}", showColumnsSQL);
              try {
                ResultSet rs =
                    sdkClient.executeStatement(
                        showColumnsSQL,
                        session.getWarehouseId(),
                        new HashMap<Integer, ImmutableSqlParameter>(),
                        StatementType.METADATA,
                        session,
                        null /* parentStatement */);
                while (rs.next()) {
                  rows.add(Collections.singletonList(rs.getString(2)));
                }
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            }
          });
    }

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

  private Queue<String> getCatalogs(String catalog, IDatabricksSession session)
      throws SQLException {
    Queue<String> catalogs = new ConcurrentLinkedQueue<>();
    if (WildcardUtil.isMatchAnything(catalog)) {
      ResultSet rs = listCatalogs(session);
      while (rs.next()) {
        catalogs.add(rs.getString(1));
      }
    } else {
      catalogs.add(catalog);
    }
    return catalogs;
  }
}
