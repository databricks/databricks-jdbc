package com.databricks.jdbc.dbclient;

import com.databricks.jdbc.common.MetadataOperationType;
import java.util.Arrays;
import java.util.Objects;

/** JDBC arguments for one asynchronous SHOW metadata operation. */
public final class MetadataAsyncRequest {
  public enum Method {
    CATALOGS,
    SCHEMAS,
    TABLES,
    COLUMNS,
    FUNCTIONS,
    PRIMARY_KEYS,
    IMPORTED_KEYS,
    CROSS_REFERENCES
  }

  private final MetadataOperationType operation;
  private final Method method;
  private final String catalog;
  private final String schema;
  private final String table;
  private final String column;
  private final String[] tableTypes;
  private final String parentCatalog;
  private final String parentSchema;
  private final String parentTable;

  private MetadataAsyncRequest(
      MetadataOperationType operation,
      Method method,
      String catalog,
      String schema,
      String table,
      String column,
      String[] tableTypes,
      String parentCatalog,
      String parentSchema,
      String parentTable) {
    this.operation = Objects.requireNonNull(operation, "operation");
    this.method = method;
    this.catalog = catalog;
    this.schema = schema;
    this.table = table;
    this.column = column;
    this.tableTypes = tableTypes == null ? null : Arrays.copyOf(tableTypes, tableTypes.length);
    this.parentCatalog = parentCatalog;
    this.parentSchema = parentSchema;
    this.parentTable = parentTable;
  }

  public static MetadataAsyncRequest forCatalogs() {
    return new MetadataAsyncRequest(
        MetadataOperationType.GET_CATALOGS,
        Method.CATALOGS,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public static MetadataAsyncRequest forSchemas(String catalog, String schemaPattern) {
    return new MetadataAsyncRequest(
        MetadataOperationType.GET_SCHEMAS,
        Method.SCHEMAS,
        catalog,
        schemaPattern,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  public static MetadataAsyncRequest forTables(
      String catalog, String schemaPattern, String tablePattern, String[] types) {
    return new MetadataAsyncRequest(
        MetadataOperationType.GET_TABLES,
        Method.TABLES,
        catalog,
        schemaPattern,
        tablePattern,
        null,
        types,
        null,
        null,
        null);
  }

  public static MetadataAsyncRequest forColumns(
      String catalog, String schemaPattern, String tablePattern, String columnPattern) {
    return new MetadataAsyncRequest(
        MetadataOperationType.GET_COLUMNS,
        Method.COLUMNS,
        catalog,
        schemaPattern,
        tablePattern,
        columnPattern,
        null,
        null,
        null,
        null);
  }

  public static MetadataAsyncRequest forFunctions(
      String catalog, String schemaPattern, String functionPattern) {
    return new MetadataAsyncRequest(
        MetadataOperationType.GET_FUNCTIONS,
        Method.FUNCTIONS,
        catalog,
        schemaPattern,
        functionPattern,
        null,
        null,
        null,
        null,
        null);
  }

  public static MetadataAsyncRequest forPrimaryKeys(String catalog, String schema, String table) {
    return new MetadataAsyncRequest(
        MetadataOperationType.GET_PRIMARY_KEYS,
        Method.PRIMARY_KEYS,
        catalog,
        schema,
        table,
        null,
        null,
        null,
        null,
        null);
  }

  public static MetadataAsyncRequest forImportedKeys(String catalog, String schema, String table) {
    return new MetadataAsyncRequest(
        MetadataOperationType.GET_CROSS_REFERENCE,
        Method.IMPORTED_KEYS,
        catalog,
        schema,
        table,
        null,
        null,
        null,
        null,
        null);
  }

  public static MetadataAsyncRequest forCrossReferences(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) {
    return new MetadataAsyncRequest(
        MetadataOperationType.GET_CROSS_REFERENCE,
        Method.CROSS_REFERENCES,
        foreignCatalog,
        foreignSchema,
        foreignTable,
        null,
        null,
        parentCatalog,
        parentSchema,
        parentTable);
  }

  /** Allows callers to request an operation explicitly; unsupported SELECTs fail at submission. */
  public static MetadataAsyncRequest forOperation(MetadataOperationType operation) {
    return new MetadataAsyncRequest(
        operation, null, null, null, null, null, null, null, null, null);
  }

  public MetadataOperationType getOperation() {
    return operation;
  }

  public Method getMethod() {
    return method;
  }

  public String getCatalog() {
    return catalog;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
  }

  public String getFunctionPattern() {
    return method == Method.FUNCTIONS ? table : null;
  }

  public String getColumn() {
    return column;
  }

  public String[] getTableTypes() {
    return tableTypes == null ? null : Arrays.copyOf(tableTypes, tableTypes.length);
  }

  public String getParentCatalog() {
    return parentCatalog;
  }

  public String getParentSchema() {
    return parentSchema;
  }

  public String getParentTable() {
    return parentTable;
  }
}
