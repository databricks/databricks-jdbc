package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.common.LogLevel;

import java.sql.DriverPropertyInfo;
import java.util.*;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.PAIR_DELIMITER;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.URL_DELIMITER;
import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.*;

public class DatabricksDriverPropertyUtil {

    public static DriverPropertyInfo[] getInvalidUrlPropertyInfo(DatabricksJdbcUrlParams param) {
        DriverPropertyInfo[] propertyInfos = new DriverPropertyInfo[1];
        propertyInfos[0] = getUrlParamInfo(param, true);
        return propertyInfos;
    }

    public static Map<String, String> buildPropertiesMap(String connectionParamString, Properties properties) {
        Map<String, String> params = new HashMap<>();
        String[] urlParams = connectionParamString.split(URL_DELIMITER);
        for(int i=1; i< urlParams.length; i++) {
            String[] param = urlParams[i].split(PAIR_DELIMITER);
            if(param.length != 2) {
                param = new String[]{param[0], ""};
            }
            params.put(param[0].toLowerCase(), param[1]);
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            params.put(entry.getKey().toString().toLowerCase(), entry.getValue().toString());
        }
        return params;
    }

    public static List<DriverPropertyInfo> getMissingProperties(String connectionParamString, Properties properties) {
        Map<String, String> connectionPropertiesMap = buildPropertiesMap(connectionParamString, properties);
        List<DriverPropertyInfo> missingPropertyInfos = new ArrayList<>();
        // add required properties
        for(DatabricksJdbcUrlParams param: DatabricksJdbcUrlParams.values()) {
            if (param.isRequired()) {
                addMissingProperty(missingPropertyInfos, connectionPropertiesMap, param, true);
            }
        }
        // add optional but important properties
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, SSL, false);
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LOG_LEVEL, false);
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, USE_PROXY, false);
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, USE_THRIFT_CLIENT, false);
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, ENABLE_ARROW, false);
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, DIRECT_RESULT, false);
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, COMPRESSION_FLAG, false);
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LZ4_COMPRESSION_FLAG, false);
        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, USER_AGENT_ENTRY, false);

        // log-level properties
        if (connectionPropertiesMap.containsKey(LOG_LEVEL.getParamName())
                && !Objects.equals(connectionPropertiesMap.get(LOG_LEVEL.getParamName()), "0")) {
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LOG_PATH, false);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LOG_FILE_SIZE, false);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, LOG_FILE_COUNT, false);
        }

        // auth-related properties
        if (connectionPropertiesMap.containsKey(AUTH_MECH.getParamName())
                && IDatabricksConnectionContext.AuthMech.parseAuthMech(connectionPropertiesMap.get(AUTH_MECH.getParamName())) == IDatabricksConnectionContext.AuthMech.OAUTH) {
            IDatabricksConnectionContext.AuthFlow authFlow = connectionContext.getAuthFlow();

            if (connectionPropertiesMap.containsKey(AUTH_FLOW.getParamName())) {
                switch (authFlow) {
                    case TOKEN_PASSTHROUGH:
                        if (connectionContext.getOAuthRefreshToken() != null) {
                            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_ID, false);
                            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_SECRET, true);
                            handleTokenEndpointAndDiscoveryMode(missingPropertyInfos);
                        } else {
                            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, OAUTH_REFRESH_TOKEN, false);
                            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, AUTH_ACCESS_TOKEN, true);
                        }
                        break;
                    case CLIENT_CREDENTIALS:
                        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_SECRET, true);
                        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_ID, true);

                        if (connectionPropertiesMap.containsKey(USE_JWT_ASSERTION.getParamName())) {
                            if (connectionContext.useJWTAssertion()) {
                                addMissingProperty(missingPropertyInfos, connectionPropertiesMap, JWT_KEY_FILE, true);
                                addMissingProperty(missingPropertyInfos, connectionPropertiesMap, JWT_ALGORITHM, true);
                                addMissingProperty(missingPropertyInfos, connectionPropertiesMap, JWT_PASS_PHRASE, true);
                                addMissingProperty(missingPropertyInfos, connectionPropertiesMap, JWT_KID, true);
                                handleTokenEndpointAndDiscoveryMode(missingPropertyInfos);
                            }
                        } else {
                            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, USE_JWT_ASSERTION, false);
                        }
                        break;

                    case BROWSER_BASED_AUTHENTICATION:
                        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_ID, false);
                        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, CLIENT_SECRET, false);
                        addMissingProperty(missingPropertyInfos, connectionPropertiesMap, AUTH_SCOPE, false);
                        break;
                }
            } else {
                missingPropertyInfos.add(getDriverPropertyInfo(AUTH_FLOW, true));
            }
        }

        // proxy-related properties
        if (connectionPropertiesMap.containsKey(USE_PROXY.getParamName())
                && connectionContext.getUseProxy()) {
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, PROXY_HOST, true);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, PROXY_PORT, true);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, PROXY_USER, false);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, PROXY_PWD, false);
        }

        return missingPropertyInfos;
    }

    private void handleTokenEndpointAndDiscoveryMode(List<DriverPropertyInfo> missingPropertyInfos, Map<String, String> connectionPropertiesMap) {
        if (connectionContext.getTokenEndpoint() == null
                && connectionContext.isOAuthDiscoveryModeEnabled()
                && connectionContext.getOAuthDiscoveryURL() == null) {
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, DISCOVERY_URL, true);
        } else {
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, TOKEN_ENDPOINT, false);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, DISCOVERY_MODE, false);
            addMissingProperty(missingPropertyInfos, connectionPropertiesMap, DISCOVERY_URL, false);
        }
    }

    private static void addMissingProperty(
            List<DriverPropertyInfo> missingPropertyInfos,
            Map<String, String> connectionPropertiesMap,
            DatabricksJdbcUrlParams param,
            boolean required) {
        if (!connectionPropertiesMap.containsKey(param.getParamName())) {
            missingPropertyInfos.add(getUrlParamInfo(param, required));
        }
    }
}
