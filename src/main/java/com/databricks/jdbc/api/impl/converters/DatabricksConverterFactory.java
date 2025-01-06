package com.databricks.jdbc.api.impl.converters;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;

public class DatabricksConverterFactory {
    private static final DatabricksConverterFactory INSTANCE = new DatabricksConverterFactory();
    public static DatabricksConverterFactory getInstance() {
        return INSTANCE;
    }

    public ConverterHelper getConverterHelper(IDatabricksConnectionContext context) {
        return instances.computeIfAbsent(
                context.getConnectionUuid(), k -> new ConverterHelper(context));
    }
}
