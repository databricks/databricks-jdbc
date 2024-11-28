package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryEvent;

import java.util.LinkedHashMap;

public class TelemetryClientFactory {

    private static final JdbcLogger logger = JdbcLoggerFactory.getLogger(DatabricksConnection.class);

    static LinkedHashMap<String, TelemetryClient> telemetryClients = new LinkedHashMap<>();

    public static ITelemetryClient getTelemetryClient(IDatabricksConnectionContext connectionContext) {
        String hostUrl;
        try {
            hostUrl = connectionContext.getHostUrl();
        } catch (DatabricksParsingException e) {
            // We should not throw exception for Telemetry flow, this should be non-blocking for regular JDBC
            // operations. Though very likely the exception should have already thrown by now
            logger.error(e, "Failed to initialize Telemetry client due to parsing error {}", e.getMessage());
            return new NoopTelemetryClient();
        }
        if (telemetryClients.containsKey(hostUrl)) {
            return telemetryClients.get(hostUrl);
        } else {
            TelemetryClient client = new TelemetryClient(connectionContext);
            telemetryClients.put(hostUrl, client);
            return client;
        }
    }
}
