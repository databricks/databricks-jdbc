package com.jayant.testparams;

import static com.jayant.testparams.ParamUtils.putInMapForKey;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Executors;

public class ConnectionTestParams implements TestParams {
  @Override
  public Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs() {
    Set<Map.Entry<String, Integer>> set = new HashSet<>();

    // Do not close result set
    set.add(Map.entry("close", 0));
    set.add(Map.entry("abort", 1));

    // Do not close the statement
    set.add(Map.entry("closeStatement", 1));

    // Currently not supported
    set.add(Map.entry("setShardingKeyIfValid", 3));
    set.add(Map.entry("setShardingKeyIfValid", 2));
    set.add(Map.entry("setShardingKey", 2));
    set.add(Map.entry("setShardingKey", 1));
    return set;
  }

  @Override
  public Map<Map.Entry<String, Integer>, Set<Object[]>> getFunctionToArgsMap() {
    System.out.println("ConnectionTestParams.getFunctionToArgsMap");
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap = new HashMap<>();

    putInMapForKey(functionToArgsMap, Map.entry("createStatement", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("prepareStatement", 1), new String[] {"SELECT 1"});
    putInMapForKey(
        functionToArgsMap, Map.entry("prepareStatement", 3), new Object[] {"SELECT 1", 1, 1});
    putInMapForKey(
        functionToArgsMap, Map.entry("prepareStatement", 4), new Object[] {"SELECT 1", 1, 1, 1});
    putInMapForKey(
        functionToArgsMap, Map.entry("prepareStatement", 2), new Object[] {"SELECT 1", 1});
    putInMapForKey(functionToArgsMap, Map.entry("prepareCall", 1), new String[] {"SELECT 1"});
    putInMapForKey(functionToArgsMap, Map.entry("nativeSQL", 1), new String[] {"SELECT 1"});
    putInMapForKey(functionToArgsMap, Map.entry("setAutoCommit", 1), new Boolean[] {true});
    putInMapForKey(functionToArgsMap, Map.entry("getAutoCommit", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("commit", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("rollback", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("isClosed", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("getMetaData", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("setReadOnly", 1), new Boolean[] {true});
    putInMapForKey(functionToArgsMap, Map.entry("isReadOnly", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("getCatalog", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("setCatalog", 1), new Object[] {"hive_metastore"});
    putInMapForKey(functionToArgsMap, Map.entry("setTransactionIsolation", 1), new Integer[] {1});
    putInMapForKey(functionToArgsMap, Map.entry("getTransactionIsolation", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("getWarnings", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("clearWarnings", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("createStatement", 2), new Integer[] {1, 1});
    putInMapForKey(functionToArgsMap, Map.entry("prepareCall", 3), new Object[] {"SELECT 1", 1, 1});
    putInMapForKey(functionToArgsMap, Map.entry("getTypeMap", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("setTypeMap", 1), new Object[] {new HashMap<>()});
    putInMapForKey(functionToArgsMap, Map.entry("setHoldability", 1), new Integer[] {1});
    putInMapForKey(functionToArgsMap, Map.entry("getHoldability", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("setSavepoint", 0), new Object[] {});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("setSavepoint", 1),
        new String[] {"JDBC_COMPARATOR_SAVEPOINT"});
    putInMapForKey(functionToArgsMap, Map.entry("rollback", 1), new Object[] {null});
    putInMapForKey(functionToArgsMap, Map.entry("releaseSavepoint", 1), new Object[] {null});
    putInMapForKey(functionToArgsMap, Map.entry("createStatement", 3), new Object[] {1, 1, 1});
    putInMapForKey(
        functionToArgsMap, Map.entry("prepareCall", 4), new Object[] {"SELECT 1", 1, 1, 1});
    putInMapForKey(functionToArgsMap, Map.entry("createClob", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("createBlob", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("createNClob", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("createSQLXML", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("isValid", 1), new Integer[] {5});
    putInMapForKey(
        functionToArgsMap, Map.entry("setClientInfo", 2), new String[] {"NAME", "VALUE"});
    putInMapForKey(
        functionToArgsMap, Map.entry("setClientInfo", 1), new Object[] {new Properties()});
    putInMapForKey(functionToArgsMap, Map.entry("getClientInfo", 1), new String[] {"NAME"});
    putInMapForKey(functionToArgsMap, Map.entry("getClientInfo", 0), new Object[] {});
    putInMapForKey(
        functionToArgsMap, Map.entry("createArrayOf", 2), new Object[] {"NAME", new Object[] {}});
    putInMapForKey(
        functionToArgsMap, Map.entry("createStruct", 2), new Object[] {"NAME", new Object[] {}});
    putInMapForKey(functionToArgsMap, Map.entry("setSchema", 1), new String[] {"hive_metastore"});
    putInMapForKey(functionToArgsMap, Map.entry("getSchema", 0), new Object[] {});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("setNetworkTimeout", 2),
        new Object[] {Executors.newSingleThreadExecutor(), 5});
    putInMapForKey(functionToArgsMap, Map.entry("getNetworkTimeout", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("unwrap", 1), new Object[] {Connection.class});
    putInMapForKey(
        functionToArgsMap, Map.entry("isWrapperFor", 1), new Object[] {Connection.class});
    putInMapForKey(functionToArgsMap, Map.entry("getConnection", 0), new Object[] {});
    putInMapForKey(functionToArgsMap, Map.entry("getConnectionContext", 0), new Object[] {});
    return functionToArgsMap;
  }
}
