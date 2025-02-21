package com.jayant.testparams;

import static com.jayant.testparams.ParamUtils.putInMapForKey;

import java.io.StringReader;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.*;

public class ResultSetTestParams implements TestParams {

  @Override
  public Set<Map.Entry<String, Integer>> getAcceptedKnownDiffs() {
    Set<Map.Entry<String, Integer>> set = new HashSet<>();

    // Do not close result set
    set.add(Map.entry("close", 0));

    // Don't compare classes
    set.add(Map.entry("getStatement", 0));
    set.add(Map.entry("getMetaData", 0));

    // Unsupported types in dbsql:
    // https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes
    set.add(Map.entry("getURL", 1));
    set.add(Map.entry("getNClob", 1));
    set.add(Map.entry("getNString", 1));
    set.add(Map.entry("getByte", 1));
    set.add(Map.entry("getClob", 1));
    set.add(Map.entry("getRef", 1));
    set.add(Map.entry("getRowId", 1));
    set.add(Map.entry("getAsciiStream", 1));
    set.add(Map.entry("getNCharacterStream", 1));
    set.add(Map.entry("getUnicodeStream", 1));
    set.add(Map.entry("getBinaryStream", 1));
    set.add(Map.entry("getBlob", 1));
    set.add(Map.entry("getSQLXML", 1));
    set.add(Map.entry("updateClob", 2));
    set.add(Map.entry("updateClob", 3));
    set.add(Map.entry("updateByte", 2));
    set.add(Map.entry("updateRowId", 2));
    set.add(Map.entry("updateBlob", 2));
    set.add(Map.entry("updateBlob", 3));
    set.add(Map.entry("updateSQLXML", 2));
    set.add(Map.entry("updateAsciiStream", 2));
    set.add(Map.entry("updateAsciiStream", 3));
    set.add(Map.entry("updateNClob", 2));
    set.add(Map.entry("updateNClob", 3));
    set.add(Map.entry("updateNCharacterStream", 2));
    set.add(Map.entry("updateNCharacterStream", 3));
    set.add(Map.entry("updateBinaryStream", 2));
    set.add(Map.entry("updateBinaryStream", 3));
    set.add(Map.entry("updateNString", 2));
    set.add(Map.entry("updateRef", 2));

    // Testing one method of this type, too much overhead to test the ones with 3 params
    set.add(Map.entry("updateCharacterStream", 3));
    set.add(Map.entry("getObject", 2));
    set.add(Map.entry("updateObject", 3));
    set.add(Map.entry("updateObject", 4));

    set.add(Map.entry("unwrap", 1));
    set.add(Map.entry("isWrapperFor", 1));

    return set;
  }

  /*
    The test table was created using the following command:

    CREATE TABLE test_result_set_types (
      id INT PRIMARY KEY,
      varchar_column STRING,
      boolean_column BOOLEAN,
      integer_column INT,
      bigint_column BIGINT,
      smallint_column SMALLINT,
      tinyint_column TINYINT,
      float_column FLOAT,
      double_column DOUBLE,
      decimal_column DECIMAL(10, 2),
      date_column DATE,
      timestamp_column TIMESTAMP,
      timestamp_ntz_column TIMESTAMP_NTZ,
      binary_column BINARY,
      array_column ARRAY<STRING>,
      map_column MAP<STRING, STRING>,
      struct_column STRUCT<name: STRING, age: INT>,
      variant_column VARIANT

      it contains the following row from id 1 to 10

      INSERT INTO test_result_set_types
      (id, varchar_column, boolean_column, integer_column, bigint_column, smallint_column, tinyint_column,
      float_column, double_column, decimal_column, date_column, timestamp_column, timestamp_ntz_column,
      binary_column, array_column, map_column, struct_column, variant_column)
      VALUES
      (1, 'Test Varchar', true, 123, 9876543210, 32000, 120, 1.23, 4.56, 7890.12, '2023-12-31',
      '2023-12-31 12:30:00', '2023-12-31 12:30:00', X'DEADBEEF', array('item1', 'item2'), map('key1', 'value1', 'key2', 'value2'),
      named_struct('name', 'John', 'age', 30), 'semi-structured data');
  );
     */

  @Override
  public Map<Map.Entry<String, Integer>, Set<Object[]>> getFunctionToArgsMap() {
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap = new HashMap<>();

    // Cursor related functions
    putInMapForKey(functionToArgsMap, Map.entry("absolute", 1), new Integer[] {1});
    putInMapForKey(functionToArgsMap, Map.entry("relative", 1), new Integer[] {-1});
    putInMapForKey(functionToArgsMap, Map.entry("setFetchSize", 1), new Integer[] {10});
    for (Integer i : getAllFetchDirection()) {
      putInMapForKey(functionToArgsMap, Map.entry("setFetchDirection", 1), new Integer[] {i});
    }

    putInMapForKey(functionToArgsMap, Map.entry("findColumn", 1), new String[] {"float_column"});

    // get functions based on column index
    putInMapForKey(functionToArgsMap, Map.entry("getString", 1), new Integer[] {2});
    putInMapForKey(functionToArgsMap, Map.entry("getCharacterStream", 1), new Integer[] {2});
    putInMapForKey(functionToArgsMap, Map.entry("getBoolean", 1), new Integer[] {3});
    putInMapForKey(functionToArgsMap, Map.entry("getInt", 1), new Integer[] {4});
    putInMapForKey(functionToArgsMap, Map.entry("getLong", 1), new Integer[] {5});
    putInMapForKey(functionToArgsMap, Map.entry("getShort", 1), new Integer[] {6});
    putInMapForKey(functionToArgsMap, Map.entry("getShort", 1), new Integer[] {7});
    putInMapForKey(functionToArgsMap, Map.entry("getFloat", 1), new Integer[] {8});
    putInMapForKey(functionToArgsMap, Map.entry("getDouble", 1), new Integer[] {9});
    putInMapForKey(functionToArgsMap, Map.entry("getBigDecimal", 1), new Integer[] {9});
    putInMapForKey(functionToArgsMap, Map.entry("getBigDecimal", 2), new Integer[] {9, 2});
    putInMapForKey(functionToArgsMap, Map.entry("getBigDecimal", 1), new Integer[] {10});
    putInMapForKey(functionToArgsMap, Map.entry("getBigDecimal", 2), new Integer[] {10, 2});
    putInMapForKey(functionToArgsMap, Map.entry("getDouble", 1), new Integer[] {10});
    putInMapForKey(functionToArgsMap, Map.entry("getDate", 1), new Integer[] {11});
    putInMapForKey(
        functionToArgsMap, Map.entry("getDate", 2), new Object[] {11, Calendar.getInstance()});
    putInMapForKey(functionToArgsMap, Map.entry("getTimestamp", 1), new Integer[] {12});
    putInMapForKey(functionToArgsMap, Map.entry("getTime", 1), new Integer[] {12});
    putInMapForKey(
        functionToArgsMap, Map.entry("getTimestamp", 2), new Object[] {12, Calendar.getInstance()});
    putInMapForKey(
        functionToArgsMap, Map.entry("getTime", 2), new Object[] {12, Calendar.getInstance()});
    putInMapForKey(functionToArgsMap, Map.entry("getTimestamp", 1), new Integer[] {13});
    putInMapForKey(functionToArgsMap, Map.entry("getBytes", 1), new Integer[] {14});
    putInMapForKey(functionToArgsMap, Map.entry("getArray", 1), new Integer[] {15});
    putInMapForKey(functionToArgsMap, Map.entry("getObject", 1), new Integer[] {16});
    putInMapForKey(functionToArgsMap, Map.entry("getObject", 1), new Integer[] {17});
    putInMapForKey(functionToArgsMap, Map.entry("getObject", 1), new Integer[] {18});

    // get functions based on column name
    putInMapForKey(functionToArgsMap, Map.entry("getString", 1), new String[] {"varchar_column"});
    putInMapForKey(
        functionToArgsMap, Map.entry("getCharacterStream", 1), new String[] {"varchar_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getBoolean", 1), new String[] {"boolean_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getInt", 1), new String[] {"integer_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getLong", 1), new String[] {"bigint_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getShort", 1), new String[] {"smallint_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getShort", 1), new String[] {"tinyint_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getFloat", 1), new String[] {"float_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getDouble", 1), new String[] {"double_column"});
    putInMapForKey(
        functionToArgsMap, Map.entry("getBigDecimal", 1), new String[] {"double_column"});
    putInMapForKey(
        functionToArgsMap, Map.entry("getBigDecimal", 2), new Object[] {"double_column", 2});
    putInMapForKey(
        functionToArgsMap, Map.entry("getBigDecimal", 1), new String[] {"decimal_column"});
    putInMapForKey(
        functionToArgsMap, Map.entry("getBigDecimal", 2), new Object[] {"decimal_column", 2});
    putInMapForKey(functionToArgsMap, Map.entry("getDouble", 1), new String[] {"decimal_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getDate", 1), new String[] {"date_column"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getDate", 2),
        new Object[] {"date_column", Calendar.getInstance()});
    putInMapForKey(
        functionToArgsMap, Map.entry("getTimestamp", 1), new String[] {"timestamp_column"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getTimestamp", 2),
        new Object[] {"timestamp_column", Calendar.getInstance()});
    putInMapForKey(functionToArgsMap, Map.entry("getTime", 1), new String[] {"timestamp_column"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("getTime", 2),
        new Object[] {"timestamp_column", Calendar.getInstance()});
    putInMapForKey(
        functionToArgsMap, Map.entry("getTimestamp", 1), new String[] {"timestamp_ntz_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getBytes", 1), new String[] {"binary_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getArray", 1), new String[] {"array_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getObject", 1), new String[] {"map_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getObject", 1), new String[] {"struct_column"});
    putInMapForKey(functionToArgsMap, Map.entry("getObject", 1), new String[] {"variant_column"});

    // update functions based on column index
    putInMapForKey(functionToArgsMap, Map.entry("updateString", 2), new Object[] {2, "string"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateCharacterStream", 2),
        new Object[] {2, new StringReader("string")});
    putInMapForKey(functionToArgsMap, Map.entry("updateBoolean", 2), new Object[] {3, true});
    putInMapForKey(functionToArgsMap, Map.entry("updateInt", 2), new Object[] {4, 1234});
    putInMapForKey(functionToArgsMap, Map.entry("updateLong", 2), new Object[] {5, 9892492323L});
    putInMapForKey(functionToArgsMap, Map.entry("updateShort", 2), new Object[] {6, 1234});
    putInMapForKey(functionToArgsMap, Map.entry("updateShort", 2), new Object[] {7, 12});
    putInMapForKey(functionToArgsMap, Map.entry("updateFloat", 2), new Object[] {8, 7.89});
    putInMapForKey(functionToArgsMap, Map.entry("updateDouble", 2), new Object[] {9, 11210.28412});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateBigDecimal", 2), new Object[] {9, 11210.28412});
    putInMapForKey(functionToArgsMap, Map.entry("updateDouble", 2), new Object[] {10, 121.22});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateDate", 2),
        new Object[] {11, new Date(System.currentTimeMillis())});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateTimestamp", 2),
        new Object[] {12, new Timestamp(System.currentTimeMillis())});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateTime", 2),
        new Object[] {12, new Timestamp(System.currentTimeMillis())});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateTimestamp", 2),
        new Object[] {13, new Timestamp(System.currentTimeMillis())});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateBytes", 2), new Object[] {14, "test".getBytes()});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateArray", 2),
        new Object[] {15, new String[] {"new1", "new2"}});
    putInMapForKey(functionToArgsMap, Map.entry("updateObject", 2), new Object[] {16, Map.of()});
    putInMapForKey(functionToArgsMap, Map.entry("updateObject", 2), new Object[] {17, Map.of()});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateObject", 2), new Object[] {18, "new-variant"});

    // update functions based on column name
    putInMapForKey(
        functionToArgsMap, Map.entry("updateString", 2), new Object[] {"varchar_column", "string"});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateCharacterStream", 2),
        new Object[] {"varchar_column", new StringReader("string")});
    putInMapForKey(
        functionToArgsMap, Map.entry("Boolean", 2), new Object[] {"boolean_column", true});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateInt", 2), new Object[] {"integer_column", 4221});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateLong", 2), new Object[] {"bigint_column", 121312313L});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateShort", 2), new Object[] {"smallint_column", 1212});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateShort", 2), new Object[] {"tinyint_column", 13});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateFloat", 2), new Object[] {"float_column", 42.82});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateDouble", 2),
        new Object[] {"double_column", 13322313.2313123});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateBigDecimal", 2),
        new Object[] {"double_column", 13322313.2313123});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateDouble", 2), new Object[] {"decimal_column", 1231.23});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateDate", 2),
        new Object[] {"date_column", new Date(System.currentTimeMillis())});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateTimestamp", 2),
        new Object[] {"timestamp_column", new Timestamp(System.currentTimeMillis())});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateTime", 2),
        new Object[] {"timestamp_column", new Timestamp(System.currentTimeMillis())});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateTimestamp", 2),
        new Object[] {"timestamp_ntz_column", new Timestamp(System.currentTimeMillis())});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateBytes", 2),
        new Object[] {"binary_column", "test".getBytes()});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateArray", 2),
        new Object[] {"array_column", new String[] {"new1", "new2"}});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateObject", 2), new Object[] {"map_column", Map.of()});
    putInMapForKey(
        functionToArgsMap, Map.entry("updateObject", 2), new Object[] {"struct_column", Map.of()});
    putInMapForKey(
        functionToArgsMap,
        Map.entry("updateObject", 2),
        new Object[] {"variant_column", "new-variant"});

    putInMapForKey(functionToArgsMap, Map.entry("updateNull", 1), new Object[] {"varchar_column"});
    putInMapForKey(functionToArgsMap, Map.entry("updateNull", 1), new Object[] {2});

    return functionToArgsMap;
  }

  private static List<Integer> getAllFetchDirection() {
    return Arrays.asList(ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE, ResultSet.FETCH_UNKNOWN);
  }
}
