package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.verifySuccessStatus;

import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.impl.thrift.commons.ThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksThriftMetadataClient implements DatabricksMetadataClient {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DatabricksThriftMetadataClient.class);
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
      IDatabricksSession session, String catalog, String schemaNamePattern)
      throws DatabricksSQLException {
    LOGGER.debug(
        "Fetching schemas for all purpose cluster. Session {}, catalog {}, schemaNamePattern {}",
        session.toString(),
        catalog,
        schemaNamePattern);
    TGetSchemasReq request =
        new TGetSchemasReq()
            .setSessionHandle(session.getSessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern);
    TGetSchemasResp response =
        (TGetSchemasResp) thriftAccessor.getThriftResponse(request, CommandName.LIST_SCHEMAS);
    System.out.println("Here is schema response " + response.toString());
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
    return null;
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern) {
    LOGGER.debug(
        "Fetching tables for all purpose cluster. Session {}, catalog {}, schemaNamePattern {}, tableNamePattern {}",
        session.toString(),
        catalog,
        schemaNamePattern,
        tableNamePattern);
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session)
      throws DatabricksSQLException {
    LOGGER.debug("Fetching table types for all purpose cluster. Session {}", session.toString());
    TGetTableTypesReq request =
        new TGetTableTypesReq().setSessionHandle(session.getSessionHandle());
    TGetTableTypesResp response =
        (TGetTableTypesResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_TABLE_TYPES);
    System.out.println("??????? Table type info " + response.toString());
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
    return null;
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
    LOGGER.debug(
        "Fetching primary keys for all purpose cluster. session {}, catalog {}, schema {}, table {}",
        session.toString(),
        catalog,
        schema,
        table);
    TGetPrimaryKeysReq request =
        new TGetPrimaryKeysReq()
            .setSessionHandle(session.getSessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schema)
            .setTableName(table);
    TGetPrimaryKeysResp response =
        (TGetPrimaryKeysResp)
            thriftAccessor.getThriftResponse(request, CommandName.LIST_PRIMARY_KEYS);
    System.out.println("response of primary keys " + response.toString());
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
    return null;
  }
}
