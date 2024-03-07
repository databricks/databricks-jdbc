package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.getSessionHandle;

import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.impl.thrift.commons.ThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.TGetPrimaryKeysReq;
import com.databricks.jdbc.client.impl.thrift.generated.TGetPrimaryKeysResp;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;

public class DatabricksThriftMetadataClient implements DatabricksMetadataClient {
  private final ThriftAccessor thriftAccessor;

  public DatabricksThriftMetadataClient(IDatabricksConnectionContext connectionContext) {
    this.thriftAccessor = new ThriftAccessor(connectionContext);
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table)
      throws DatabricksSQLException {
    TGetPrimaryKeysReq request =
        new TGetPrimaryKeysReq()
            .setSessionHandle(getSessionHandle(session))
            .setCatalogName(catalog)
            .setSchemaName(schema)
            .setTableName(table);
    TGetPrimaryKeysResp resp =
        (TGetPrimaryKeysResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_PRIMARY_KEYS);
    System.out.println("response of primary keys " + resp.toString());
    return null;
  }
}
