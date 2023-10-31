package com.databricks.jdbc.driver;

public interface IDatabricksConnectionContext {

  /**
   * Returns host-Url for Databricks server as parsed from JDBC connection in format
   * https://server:port
   *
   * @return Databricks host-Url
   */
  String getHostUrl();

  /**
   * Returns warehouse-Id as parsed from JDBC connection Url
   *
   * @return warehouse-Id
   */
  String getWarehouse();

  /**
   * Returns the auth token (personal access token/OAuth token etc)
   *
   * @return auth token
   */
  String getToken();
}
