package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import java.sql.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ComplexTypeQueryTests {

  private Connection connection;

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  private void setupConnection(int thriftVal, int complexSupport) throws SQLException {
    connection =
        getValidJDBCConnection(
            List.of(
                List.of("EnableComplexDatatypeSupport", String.valueOf(complexSupport)),
                List.of("usethriftclient", String.valueOf(thriftVal))));
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testQueryYieldingStruct(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT named_struct('age', 30, 'name', 'John Doe') AS person";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Struct s = rs.getStruct("person");
        assertNotNull(s);
        Object[] attrs = s.getAttributes();
        assertEquals(30, attrs[0]);
        assertEquals("John Doe", attrs[1]);
      } else {
        assertThrows(SQLException.class, () -> rs.getStruct("person"));
        Object obj = rs.getObject("person");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("30"));
        assertTrue(text.contains("John Doe"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testQueryYieldingArray(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT array(1, 2, 3, 4, 5) AS numbers";
    ResultSet rs = executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Array arr = rs.getArray("numbers");
        assertNotNull(arr);
        Object[] elements = (Object[]) arr.getArray();
        assertArrayEquals(new Object[] {1, 2, 3, 4, 5}, elements);
      } else {
        assertThrows(SQLException.class, () -> rs.getArray("numbers"));
        Object obj = rs.getObject("numbers");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("1"));
        assertTrue(text.contains("5"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testQueryYieldingMap(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT map('key1', 100, 'key2', 200) AS keyValuePairs";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        @SuppressWarnings("unchecked")
        Map<String, Integer> map = rs.getMap("keyValuePairs");
        assertNotNull(map);
        assertEquals(100, map.get("key1"));
        assertEquals(200, map.get("key2"));
      } else {
        assertThrows(SQLException.class, () -> rs.getMap("keyValuePairs"));
        Object obj = rs.getObject("keyValuePairs");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("key1"));
        assertTrue(text.contains("100"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testQueryYieldingNestedStructs(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql =
        "SELECT named_struct('person', named_struct('age', 30, 'name', 'John Doe')) AS personInfo";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Struct outer = rs.getStruct("personInfo");
        assertNotNull(outer);
        Object[] outerAttrs = outer.getAttributes();
        Struct inner = (Struct) outerAttrs[0];
        Object[] innerAttrs = inner.getAttributes();
        assertEquals(30, innerAttrs[0]);
        assertEquals("John Doe", innerAttrs[1]);
      } else {
        assertThrows(SQLException.class, () -> rs.getStruct("personInfo"));
        Object obj = rs.getObject("personInfo");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("John Doe"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testQueryYieldingArrayOfStructs(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql =
        "SELECT array(named_struct('age', 30, 'name', 'John'), "
            + "             named_struct('age', 40, 'name', 'Jane')) AS persons";
    ResultSet rs = executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Array arr = rs.getArray("persons");
        Object[] elements = (Object[]) arr.getArray();
        Struct p1 = (Struct) elements[0];
        Struct p2 = (Struct) elements[1];
        assertEquals(30, p1.getAttributes()[0]);
        assertEquals("John", p1.getAttributes()[1]);
        assertEquals(40, p2.getAttributes()[0]);
        assertEquals("Jane", p2.getAttributes()[1]);
      } else {
        assertThrows(SQLException.class, () -> rs.getArray("persons"));
        Object obj = rs.getObject("persons");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("John"));
        assertTrue(text.contains("Jane"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testEmptyArray(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT array() AS emptyArr";
    ResultSet rs = executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Array arr = rs.getArray("emptyArr");
        assertNotNull(arr);
        Object[] elements = (Object[]) arr.getArray();
        assertEquals(0, elements.length);
      } else {
        assertThrows(SQLException.class, () -> rs.getArray("emptyArr"));
        Object obj = rs.getObject("emptyArr");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertFalse(text.isEmpty());
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testNullArray(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT array(CAST(NULL AS INT), CAST(NULL AS INT)) AS nullArr";
    ResultSet rs = executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Array arr = rs.getArray("nullArr");
        assertNotNull(arr);
        Object[] elements = (Object[]) arr.getArray();
        assertEquals(2, elements.length);
        assertNull(elements[0]);
        assertNull(elements[1]);
      } else {
        assertThrows(SQLException.class, () -> rs.getArray("nullArr"));
        Object obj = rs.getObject("nullArr");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.toLowerCase().contains("null"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testNestedArray(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT array(array(1,2), array(3,4,5)) AS nestedArr";
    ResultSet rs = executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Array outer = rs.getArray("nestedArr");
        assertNotNull(outer);
        Object[] outerElems = (Object[]) outer.getArray();
        Array inner1 = (Array) outerElems[0];
        Array inner2 = (Array) outerElems[1];
        assertArrayEquals(new Object[] {1, 2}, (Object[]) inner1.getArray());
        assertArrayEquals(new Object[] {3, 4, 5}, (Object[]) inner2.getArray());
      } else {
        assertThrows(SQLException.class, () -> rs.getArray("nestedArr"));
        Object obj = rs.getObject("nestedArr");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("1"));
        assertTrue(text.contains("5"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testStructWithArray(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT named_struct('numbers', array(1,2,3), 'age', 40) AS structWithArray";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Struct s = rs.getStruct("structWithArray");
        Object[] attrs = s.getAttributes();
        Array arr = (Array) attrs[0];
        assertArrayEquals(new Object[] {1, 2, 3}, (Object[]) arr.getArray());
        assertEquals(40, attrs[1]);
      } else {
        assertThrows(SQLException.class, () -> rs.getStruct("structWithArray"));
        Object obj = rs.getObject("structWithArray");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("1"));
        assertTrue(text.contains("3"));
        assertTrue(text.contains("40"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testStructWithMap(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT named_struct('meta', map('key1','val1'), 'count', 2) AS structWithMap";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Struct s = rs.getStruct("structWithMap");
        Object[] attrs = s.getAttributes();
        @SuppressWarnings("unchecked")
        Map<String, String> meta = (Map<String, String>) attrs[0];
        assertEquals("val1", meta.get("key1"));
        assertEquals(2, attrs[1]);
      } else {
        assertThrows(SQLException.class, () -> rs.getStruct("structWithMap"));
        Object obj = rs.getObject("structWithMap");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("key1"));
        assertTrue(text.contains("val1"));
        assertTrue(text.contains("2"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testMapOfArrays(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT map('arr1', array(1,2), 'arr2', array(3,4,5)) AS mapOfArrays";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        @SuppressWarnings("unchecked")
        Map<String, Array> mapOfArrays = rs.getMap("mapOfArrays");
        Array arr1 = mapOfArrays.get("arr1");
        assertArrayEquals(new Object[] {1, 2}, (Object[]) arr1.getArray());
        Array arr2 = mapOfArrays.get("arr2");
        assertArrayEquals(new Object[] {3, 4, 5}, (Object[]) arr2.getArray());
      } else {
        assertThrows(SQLException.class, () -> rs.getMap("mapOfArrays"));
        Object obj = rs.getObject("mapOfArrays");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("arr1"));
        assertTrue(text.contains("3"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testArrayOfMaps(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT array(map('k1',1), map('k2',2,'k3',3)) AS arrayOfMaps";
    ResultSet rs = executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Array arr = rs.getArray("arrayOfMaps");
        Object[] maps = (Object[]) arr.getArray();
        @SuppressWarnings("unchecked")
        Map<String, Integer> m1 = (Map<String, Integer>) maps[0];
        @SuppressWarnings("unchecked")
        Map<String, Integer> m2 = (Map<String, Integer>) maps[1];
        assertEquals(1, m1.get("k1"));
        assertEquals(2, m2.get("k2"));
        assertEquals(3, m2.get("k3"));
      } else {
        assertThrows(SQLException.class, () -> rs.getArray("arrayOfMaps"));
        Object obj = rs.getObject("arrayOfMaps");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("k1"));
        assertTrue(text.contains("3"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testNullInStruct(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql =
        "SELECT named_struct('name', 'Alice', 'age', CAST(NULL AS INT)) AS partialNullStruct";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Struct s = rs.getStruct("partialNullStruct");
        Object[] attrs = s.getAttributes();
        assertEquals("Alice", attrs[0]);
        assertNull(attrs[1]);
      } else {
        assertThrows(SQLException.class, () -> rs.getStruct("partialNullStruct"));
        Object obj = rs.getObject("partialNullStruct");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("Alice"));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testFullyNullStruct(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT CAST(NULL AS struct<name:STRING,age:INT>) AS nullStruct";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        Struct s = rs.getStruct("nullStruct");
        assertNull(s);
      } else {
        assertThrows(SQLException.class, () -> rs.getStruct("nullStruct"));
        Object obj = rs.getObject("nullStruct");
        // Could be null or some "NULL" string, depending on driver
        if (obj != null) {
          assertTrue(obj instanceof String);
          String text = (String) obj;
          // Possibly empty or "NULL"
          // Minimal check
          assertFalse(text.isBlank());
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"0,0", "1,0", "0,1", "1,1"})
  void testMapWithNullValue(int thriftVal, int complexSupport) throws SQLException {
    setupConnection(thriftVal, complexSupport);

    String sql = "SELECT map('k1', CAST(NULL AS INT), 'k2', 200) AS mapWithNull";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, sql);
    assertNotNull(rs);
    while (rs.next()) {
      if (complexSupport == 1) {
        @SuppressWarnings("unchecked")
        Map<String, Integer> map = rs.getMap("mapWithNull");
        assertNotNull(map);
        assertTrue(map.containsKey("k1"));
        assertNull(map.get("k1"));
        assertEquals(200, map.get("k2"));
      } else {
        assertThrows(SQLException.class, () -> rs.getMap("mapWithNull"));
        Object obj = rs.getObject("mapWithNull");
        assertTrue(obj instanceof String);
        String text = (String) obj;
        assertTrue(text.contains("k1"));
        assertTrue(text.contains("200"));
      }
    }
  }
}
