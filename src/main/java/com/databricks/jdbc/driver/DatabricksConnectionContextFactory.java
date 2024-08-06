package com.databricks.jdbc.driver;

import com.databricks.jdbc.core.DatabricksSQLException;
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
}
