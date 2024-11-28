package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryEvent;

import java.io.IOException;
import java.util.LinkedHashMap;

public class TelemetryClientFactory {

    private static final JdbcLogger logger = JdbcLoggerFactory.getLogger(DatabricksConnection.class);

    static LinkedHashMap<String, TelemetryClient> telemetryClients = new LinkedHashMap<>();

    public static ITelemetryClient getTelemetryClient(IDatabricksConnectionContext connectionContext) {
        return telemetryClients.computeIfAbsent(
                connectionContext.getConnectionUuid(), k -> new TelemetryClient(connectionContext));
    }

    public static void closeTelemetryClient(IDatabricksConnectionContext connectionContext) {
        ITelemetryClient instance = telemetryClients.remove(connectionContext.getConnectionUuid());
        if (instance != null) {
            try {
                instance.close();
            } catch (Exception e) {
                logger.debug(String.format("Caught error while closing telemetry client. Error %s", e));
            }
        }
    }
}
