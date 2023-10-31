package com.databricks.jdbc.core;

import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.jdbc.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.client.sqlexec.Session;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.sql.StatementExecutionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DatabricksSessionTest {

  private static final String JDBC_URL = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/erg6767gg;";
  private static final String JDBC_URL_INVALID = "jdbc:databricks://adb-565757575.18.azuredatabricks.net:4423/default;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehou/erg6767gg;";
  private static final String WAREHOUSE_ID = "erg6767gg";
  private static final String SESSION_ID = "session_id";

  @Mock
  DatabricksConnectionContext connectionContext;

  @Mock
  DatabricksSdkClient client;

  @Test
  public void testOpenAndCloseSession() {
    ImmutableSessionInfo sessionInfo = ImmutableSessionInfo.builder().sessionId(SESSION_ID).warehouseId(WAREHOUSE_ID).build();
    when(client.createSession(WAREHOUSE_ID)).thenReturn(sessionInfo);
    when(connectionContext.getWarehouse()).thenReturn(WAREHOUSE_ID);

    DatabricksSession session = new DatabricksSession(connectionContext,client);
    assertFalse(session.isOpen());
    session.open();
    assertTrue(session.isOpen());
    assertEquals(SESSION_ID, session.getSessionId());


    doNothing().when(client).deleteSession(SESSION_ID,WAREHOUSE_ID);
    session.close();
    assertFalse(session.isOpen());
    assertNull(session.getSessionId());
  }

  @Test
  public void testOpenSession_invalidWarehouseUrl() {
    assertThrows(IllegalArgumentException.class,
        () -> new DatabricksSession(DatabricksConnectionContext.parse(JDBC_URL_INVALID, new Properties()),
            null));
  }
}
