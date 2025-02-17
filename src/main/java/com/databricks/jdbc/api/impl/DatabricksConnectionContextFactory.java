package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.util.Properties;

/** Factory class for creating instances of {@link IDatabricksConnectionContext}. */
public class DatabricksConnectionContextFactory {

  /**
   * Creates an instance of {@link IDatabricksConnectionContext} from the given URL and properties.
   *
   * @param url JDBC URL
   * @param properties JDBC connection properties
   * @return an instance of {@link IDatabricksConnectionContext}
   * @throws DatabricksSQLException if the URL or properties are invalid
   */
  public static IDatabricksConnectionContext create(String url, Properties properties)
      throws DatabricksSQLException {
    return DatabricksConnectionContext.parse(url, properties);
  }

  /**
   * Creates an instance of {@link IDatabricksConnectionContext} from the given URL, user and
   * password
   *
   * @param url JDBC URL
   * @param user JDBC connection properties
   * @param password JDBC connection properties
   * @return an instance of {@link IDatabricksConnectionContext}
   * @throws DatabricksSQLException if the URL or properties are invalid
   */
  public static IDatabricksConnectionContext create(String url, String user, String password)
      throws DatabricksSQLException {
    java.util.Properties info = new java.util.Properties();

    if (user != null) {
      info.put("user", user);
    }
    if (password != null) {
      info.put("password", password);
    }

    return create(url, info);
  }

  public static IDatabricksConnectionContext createWithoutError(String url, Properties properties) {
    return DatabricksConnectionContext.parseWithoutError(url, properties);
  }
}
