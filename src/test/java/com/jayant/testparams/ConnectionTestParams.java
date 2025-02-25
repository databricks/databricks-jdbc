package com.jayant.testparams;

import static com.jayant.testparams.ParamUtils.putInMapForKey;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.Executors;

public class ConnectionTestParams implements TestParams {

  private static final String parameterizedQuery =
      "SELECT * FROM main.tpcds_sf100_delta.catalog_sales where cs_bill_customer_sk = ? and cs_warehouse_sk = ? limit 5";

  @Override
  public Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs() {
    Set<Map.Entry<String, Integer>> set = new HashSet<>();

    // Do not close connection
    set.add(Map.entry("close", 0));
    set.add(Map.entry("abort", 1));

    // Do not close the statement
    set.add(Map.entry("closeStatement", 1));

    // JDBC needs to provide shardingKeyBuilder support, to test these functions
    set.add(Map.entry("setShardingKeyIfValid", 3));
    set.add(Map.entry("setShardingKeyIfValid", 2));
    set.add(Map.entry("setShardingKey", 2));
    set.add(Map.entry("setShardingKey", 1));
    return set;
  }

  @Override
  public Map<Map.Entry<String, Integer>, Set<Object[]>> getFunctionToArgsMap() {
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap = new HashMap<>();

    putInMapForKey(
        functionToArgsMap, Map.entry("prepareStatement", 1), new String[] {parameterizedQuery});
    for (Integer fetchDirection : ParamUtils.getAllFetchDirection()) {
      for (Integer concurrencyCondition : ParamUtils.getAllConcurrencyCondition()) {
        putInMapForKey(
            functionToArgsMap,
            Map.entry("prepareStatement", 3),
            new Object[] {parameterizedQuery, fetchDirection, concurrencyCondition});
        putInMapForKey(
            functionToArgsMap,
            Map.entry("prepareStatement", 4),
            new Object[] {parameterizedQuery, fetchDirection, concurrencyCondition, 1});
      }
    }
    putInMapForKey(
        functionToArgsMap, Map.entry("prepareStatement", 2), new Object[] {parameterizedQuery, 1});
    putInMapForKey(functionToArgsMap, Map.entry("prepareCall", 1), new String[] {"SELECT 1"});
    putInMapForKey(functionToArgsMap, Map.entry("nativeSQL", 1), new String[] {parameterizedQuery});
    putInMapForKey(functionToArgsMap, Map.entry("setAutoCommit", 1), new Boolean[] {true});
    putInMapForKey(functionToArgsMap, Map.entry("setReadOnly", 1), new Boolean[] {true});
    putInMapForKey(functionToArgsMap, Map.entry("setCatalog", 1), new Object[] {"hive_metastore"});
    putInMapForKey(functionToArgsMap, Map.entry("setTransactionIsolation", 1), new Integer[] {1});
    putInMapForKey(functionToArgsMap, Map.entry("createStatement", 2), new Integer[] {1, 1});
    putInMapForKey(functionToArgsMap, Map.entry("prepareCall", 3), new Object[] {"SELECT 1", 1, 1});
    putInMapForKey(functionToArgsMap, Map.entry("setTypeMap", 1), new Object[] {new HashMap<>()});
    putInMapForKey(functionToArgsMap, Map.entry("setHoldability", 1), new Integer[] {1});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("setSavepoint", 1),
        new String[] {"JDBC_COMPARATOR_SAVEPOINT"});
    putInMapForKey(functionToArgsMap, Map.entry("rollback", 1), new Object[] {null});
    putInMapForKey(functionToArgsMap, Map.entry("releaseSavepoint", 1), new Object[] {null});
    putInMapForKey(functionToArgsMap, Map.entry("createStatement", 3), new Object[] {1, 1, 1});
    putInMapForKey(
        functionToArgsMap, Map.entry("prepareCall", 4), new Object[] {"SELECT 1", 1, 1, 1});
    putInMapForKey(functionToArgsMap, Map.entry("isValid", 1), new Integer[] {5});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("setClientInfo", 2),
        new String[] {"ClientInfoName", "ClientInfoValue"});

    Properties clientInfo = new Properties();
    clientInfo.setProperty("ApplicationName", "ConnectionTestParams");
    clientInfo.setProperty("ClientUser", "jdbc_comparator");
    putInMapForKey(functionToArgsMap, Map.entry("setClientInfo", 1), new Object[] {clientInfo});

    putInMapForKey(
        functionToArgsMap, Map.entry("getClientInfo", 1), new String[] {"ClientInfoName"});
    putInMapForKey(
        functionToArgsMap, Map.entry("getClientInfo", 1), new String[] {"ApplicationName"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("createArrayOf", 2),
        new Object[] {"String", new Object[] {"String1", "String2", "String3"}});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("createStruct", 2),
        new Object[] {"Integer", new Object[] {1, "Object2", 3.241}});
    putInMapForKey(functionToArgsMap, Map.entry("setSchema", 1), new String[] {"hive_metastore"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("setNetworkTimeout", 2),
        new Object[] {Executors.newSingleThreadExecutor(), 5});
    putInMapForKey(functionToArgsMap, Map.entry("unwrap", 1), new Object[] {Connection.class});
    putInMapForKey(
        functionToArgsMap, Map.entry("isWrapperFor", 1), new Object[] {Connection.class});

    return functionToArgsMap;
  }
}
