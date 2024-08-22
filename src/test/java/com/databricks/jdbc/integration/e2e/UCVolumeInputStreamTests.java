package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.getDogfoodJDBCConnection;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.dbclient.IDatabricksUCVolumeClient;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.http.entity.InputStreamEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UCVolumeInputStreamTests {

  private Connection con;

  private static final String LOCAL_FILE = "/tmp/local-e2e.txt";
  private static final String VOLUME_FILE = "e2e-stream.csv";
  private static final String FILE_CONTENT = "test-data";
  private static final String VOL_CATALOG = "samikshya_hackathon";
  private static final String VOL_SCHEMA = "default";
  private static final String VOL_ROOT = "gopal-psl";

  @BeforeEach
  void setUp() throws SQLException {
    con = getDogfoodJDBCConnection();
    System.out.println("Connection established......");
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (con != null) {
      con.close();
    }
  }

  @Test
  void testUCVolumeOperationsWithInputStream() throws Exception {
    IDatabricksUCVolumeClient client = ((IDatabricksConnection) con).getUCVolumeClient();

    File file = new File(LOCAL_FILE);
    try {
      Files.writeString(file.toPath(), FILE_CONTENT);

      System.out.println("File created");
      System.out.println(
          "Object inserted "
              + client.putObject(
                  VOL_CATALOG,
                  VOL_SCHEMA,
                  VOL_ROOT,
                  VOLUME_FILE,
                  new FileInputStream(file),
                  file.length(),
                  true));

      InputStreamEntity inputStream =
          client.getObject(VOL_CATALOG, VOL_SCHEMA, VOL_ROOT, VOLUME_FILE);
      assertEquals(FILE_CONTENT, new String(inputStream.getContent().readAllBytes()));
      inputStream.getContent().close();

      assertTrue(client.objectExists(VOL_CATALOG, VOL_SCHEMA, VOL_ROOT, VOLUME_FILE, false));
      con.setClientInfo(DatabricksJdbcConstants.ALLOWED_VOLUME_INGESTION_PATHS, "delete");
      client.deleteObject(VOL_CATALOG, VOL_SCHEMA, VOL_ROOT, VOLUME_FILE);
      assertFalse(client.objectExists(VOL_CATALOG, VOL_SCHEMA, VOL_ROOT, VOLUME_FILE, false));
    } finally {
      file.delete();
    }
  }
}
