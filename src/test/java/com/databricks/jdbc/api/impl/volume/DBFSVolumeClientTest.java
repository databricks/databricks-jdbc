package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DBFSVolumeClientTest {
  private static final String JDBC_URL =
      "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;useFileSystemAPI=1";

  @Mock DatabricksSdkClient client;

  @Test
  public void testGetDBFSVolumeClient() throws Exception {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);

    assertTrue(connection.getVolumeClient() instanceof DBFSVolumeClient);
  }

  @Test
  public void testFeatureNotSupported() throws SQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(JDBC_URL, new Properties());
    DatabricksConnection connection = new DatabricksConnection(connectionContext, client);

    DBFSVolumeClient volumeClient = (DBFSVolumeClient) connection.getVolumeClient();

    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> volumeClient.prefixExists("catalog", "schema", "volume", "prefix", true));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> volumeClient.objectExists("catalog", "schema", "volume", "objectPath", true));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> volumeClient.volumeExists("catalog", "schema", "volume", true));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> volumeClient.listObjects("catalog", "schema", "volume", "foo", true));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> volumeClient.getObject("catalog", "schema", "volume", "objectPath"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> volumeClient.getObject("catalog", "schema", "volume", "objectPath", "localPath"));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> volumeClient.putObject("catalog", "schema", "volume", "objectPath", null, 0L, true));
    assertThrows(
        DatabricksSQLFeatureNotSupportedException.class,
        () -> volumeClient.deleteObject("catalog", "schema", "volume", "objectPath"));
  }
}
