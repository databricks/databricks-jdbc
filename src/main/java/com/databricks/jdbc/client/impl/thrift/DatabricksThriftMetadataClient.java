package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.client.impl.sdk.helper.MetadataResultSetBuilder.*;
import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.verifySuccessStatus;

import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.impl.sdk.helper.MetadataResultSetBuilder;
import com.databricks.jdbc.client.impl.thrift.commons.ThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class DatabricksThriftMetadataClient implements DatabricksMetadataClient {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(DatabricksThriftMetadataClient.class);
  private final ThriftAccessor thriftAccessor;

  public DatabricksThriftMetadataClient(IDatabricksConnectionContext connectionContext) {
    this.thriftAccessor = new ThriftAccessor(connectionContext);
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session)
      throws DatabricksSQLException {
    LOGGER.debug("Listing type info for all purpose cluster. Session {}", session.toString());
    // TODO: hardcode value rather than server call.
    TGetTypeInfoReq request = new TGetTypeInfoReq().setSessionHandle(session.getSessionHandle());
    TGetTypeInfoResp response =
        (TGetTypeInfoResp) thriftAccessor.getThriftResponse(request, CommandName.LIST_TYPE_INFO);
    System.out.println("Here is list type info response " + response.toString());
    return null;
  }
  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session)
          throws SQLException {
    LOGGER.debug("Fetching catalogs for all purpose cluster. Session {}", session.toString());
    TSparkGetDirectResults directResults = new TSparkGetDirectResults().setMaxRows(1000).setMaxBytes(100000);
    TGetCatalogsReq request = new TGetCatalogsReq().setSessionHandle(session.getSessionHandle()).setGetDirectResults(directResults);
    System.out.println("Here is request DIRECT results " + request.isSetGetDirectResults());
    TGetCatalogsResp response =
        (TGetCatalogsResp) thriftAccessor.getThriftResponse(request, CommandName.LIST_CATALOGS);
    TFetchResultsResp tFetchResultsResp = thriftAccessor.getResultSetResp(response.getOperationHandle());
    List<List<String>> collect = ((TStringColumn) tFetchResultsResp.getResults().getColumns().get(0).getFieldValue()).getValues().stream().map(Collections::singletonList).collect(Collectors.toList());
    List<List<Object>> rows = collect.stream()
            .map(subList -> new ArrayList<Object>(subList))
            .collect(Collectors.toList());
    return getCatalogsResult(rows);
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern)
          throws SQLException {
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
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
    TFetchResultsResp fetchResultsResp = thriftAccessor.getResultSetResp(response.operationHandle);
    System.out.println("List schema fetch response " + fetchResultsResp.results.getColumns().get(0).getFieldValue());
    return null;
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session, String catalog, String schemaNamePattern, String tableNamePattern)
      throws SQLException {
    LOGGER.debug(
        "Fetching tables for all purpose cluster. Session {}, catalog {}, schemaNamePattern {}, tableNamePattern {}",
        session.toString(),
        catalog,
        schemaNamePattern,
        tableNamePattern);
    TGetTablesReq request =
        new TGetTablesReq()
            .setSessionHandle(session.getSessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setTableName(tableNamePattern);
    TGetTablesResp response =
        (TGetTablesResp) thriftAccessor.getThriftResponse(request, CommandName.LIST_TABLES);
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
    TFetchResultsResp resp = thriftAccessor.getResultSetResp(response.getOperationHandle());
    System.out.println("List tables fetch response " + resp.toString());
    return null;
  }


  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) {
    LOGGER.debug("Fetching table types for all purpose cluster. Session {}", session.toString());
    return MetadataResultSetBuilder.getTableTypesResult();
  }

  @Override
  public DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern)
      throws DatabricksSQLException {

    TSparkGetDirectResults directResults = new TSparkGetDirectResults().setMaxRows(1000).setMaxBytes(100000);
    TGetColumnsReq request =
        new TGetColumnsReq()
            .setSessionHandle(session.getSessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setTableName(tableNamePattern)
            .setColumnName(columnNamePattern)
                .setGetDirectResults(directResults);
    TGetTableTypesResp response =
        (TGetTableTypesResp) thriftAccessor.getThriftResponse(request, CommandName.LIST_COLUMNS);
    TFetchResultsResp fetchResultsResp = thriftAccessor.getResultSetResp(response.operationHandle);
    System.out.println("List columns fetch response " + fetchResultsResp.toString());
    return null;
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws DatabricksSQLException {
    TGetFunctionsReq request =
        new TGetFunctionsReq()
            .setSessionHandle(session.getSessionHandle())
            .setCatalogName(catalog)
            .setSchemaName(schemaNamePattern)
            .setFunctionName(functionNamePattern);
    TGetFunctionsResp response =
        (TGetFunctionsResp) thriftAccessor.getThriftResponse(request, CommandName.LIST_FUNCTIONS);
    TFetchResultsResp fetchResultsResp = thriftAccessor.getResultSetResp(response.operationHandle);
    System.out.println("List functions fetch response " + fetchResultsResp.toString());
    return null;
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table)
          throws SQLException {
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
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
    TFetchResultsResp fetchResultsResp = thriftAccessor.getResultSetResp(response.operationHandle);
    List<Object> values = new ArrayList<>();
    for(int i=0;i<4;i++){
      Object s = ((TStringColumn) fetchResultsResp.results.getColumns().get(i).getFieldValue()).getValues().get(0);
      values.add(s);
    }
    List<List<Object>> rows = Collections.singletonList(values);
    return getPrimaryKeysResult(rows);
  }
}
