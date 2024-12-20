package com.databricks.jdbc.integration.e2e;

import static com.databricks.jdbc.integration.IntegrationTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import java.sql.*;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ComplexTypeQueryTests {

  private Connection connection;

  @BeforeEach
  void setUp() throws SQLException {
    connection = getValidJDBCConnection();
  }

  @AfterEach
  void cleanUp() throws SQLException {
    if (connection != null) {
      connection.close();
    }
  }

  @Test
  void testQueryYieldingStruct() throws SQLException {
    String structQuerySQL = "SELECT named_struct('age', 30, 'name', 'John Doe') AS person";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, structQuerySQL);

    assertNotNull(rs, "ResultSet should not be null");

    while (rs.next()) {
      Struct struct = rs.getStruct("person");
      assertNotNull(struct, "Struct should not be null");

      Object[] attributes = struct.getAttributes();
      assertEquals(2, attributes.length, "Expected 2 attributes in the struct");

      assertEquals(30, attributes[0], "Expected age to be 30");
      assertEquals("John Doe", attributes[1], "Expected name to be 'John Doe'");
    }
  }

  @Test
  void testQueryYieldingArray() throws SQLException {
    String arrayQuerySQL = "SELECT array(1, 2, 3, 4, 5) AS numbers";
    ResultSet rs = executeQuery(connection, arrayQuerySQL);

    assertNotNull(rs, "ResultSet should not be null");

    while (rs.next()) {
      Array array = rs.getArray("numbers");
      assertNotNull(array, "Array should not be null");

      Object[] arrayElements = (Object[]) array.getArray();
      assertEquals(5, arrayElements.length, "Expected array length of 5");
      assertArrayEquals(new Object[] {1, 2, 3, 4, 5}, arrayElements, "Array elements mismatch");
    }
  }

  @Test
  void testQueryYieldingMap() throws SQLException {
    // Assuming the database supports maps, adjust the SQL syntax accordingly
    String mapQuerySQL = "SELECT map('key1', 100, 'key2', 200) AS keyValuePairs";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, mapQuerySQL);

    assertNotNull(rs, "ResultSet should not be null");

    while (rs.next()) {
      Map<String, Integer> map = rs.getMap("keyValuePairs");
      assertNotNull(map, "Map should not be null");

      assertEquals(2, map.size(), "Expected map size of 2");
      assertEquals(100, map.get("key1"), "Expected value for key1 to be 100");
      assertEquals(200, map.get("key2"), "Expected value for key2 to be 200");
    }
  }

  @Test
  void testQueryYieldingNestedStructs() throws SQLException {
    String nestedStructQuerySQL =
        "SELECT named_struct('person', named_struct('age', 30, 'name', 'John Doe')) AS personInfo";
    DatabricksResultSet rs = (DatabricksResultSet) executeQuery(connection, nestedStructQuerySQL);

    assertNotNull(rs, "ResultSet should not be null");

    while (rs.next()) {
      Struct personInfoStruct = rs.getStruct("personInfo");
      assertNotNull(personInfoStruct, "Outer Struct should not be null");

      Object[] outerAttributes = personInfoStruct.getAttributes();
      assertEquals(1, outerAttributes.length, "Expected 1 attribute in outer struct");

      Struct personStruct = (Struct) outerAttributes[0];
      assertNotNull(personStruct, "Inner Struct should not be null");

      Object[] innerAttributes = personStruct.getAttributes();
      assertEquals(2, innerAttributes.length, "Expected 2 attributes in the inner struct");
      assertEquals(30, innerAttributes[0], "Expected age to be 30");
      assertEquals("John Doe", innerAttributes[1], "Expected name to be 'John Doe'");
    }
  }

  @Test
  void testQueryYieldingArrayOfStructs() throws SQLException {
    String arrayOfStructsSQL =
        "SELECT array(named_struct('age', 30, 'name', 'John'), named_struct('age', 40, 'name', 'Jane')) AS persons";
    ResultSet rs = executeQuery(connection, arrayOfStructsSQL);

    assertNotNull(rs, "ResultSet should not be null");

    while (rs.next()) {
      Array array = rs.getArray("persons");
      assertNotNull(array, "Array should not be null");

      Object[] arrayElements = (Object[]) array.getArray();
      assertEquals(2, arrayElements.length, "Expected array length of 2");

      Struct person1 = (Struct) arrayElements[0];
      Struct person2 = (Struct) arrayElements[1];

      Object[] person1Attributes = person1.getAttributes();
      Object[] person2Attributes = person2.getAttributes();

      assertEquals(30, person1Attributes[0], "Expected first person's age to be 30");
      assertEquals("John", person1Attributes[1], "Expected first person's name to be 'John'");

      assertEquals(40, person2Attributes[0], "Expected second person's age to be 40");
      assertEquals("Jane", person2Attributes[1], "Expected second person's name to be 'Jane'");
    }
  }
}
