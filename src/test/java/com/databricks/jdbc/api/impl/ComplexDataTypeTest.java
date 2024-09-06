package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ComplexDataTypeTest {

  @Test
  public void testNestedStructInArray() throws SQLException {
    // Prepare elements and metadata
    Map<String, Object> structData1 = new LinkedHashMap<>();
    structData1.put("age", "30");
    structData1.put("email", "john.doe@example.com");

    Map<String, Object> structData2 = new LinkedHashMap<>();
    structData2.put("age", "40");
    structData2.put("email", "jane.doe@example.com");

    List<Object> elements = Arrays.asList(structData1, structData2);
    String metadata = "ARRAY<STRUCT<age:INT, email:STRING>>";

    // Create Array
    Array array = new DatabricksArray(elements, metadata);

    // Test getArray
    Object[] arrayElements = (Object[]) array.getArray();
    assertEquals(2, arrayElements.length);

    Struct struct1 = (Struct) arrayElements[0];
    Struct struct2 = (Struct) arrayElements[1];

    Object[] attrs1 = struct1.getAttributes();
    Object[] attrs2 = struct2.getAttributes();

    assertEquals(30, attrs1[0]);
    assertEquals("john.doe@example.com", attrs1[1]);

    assertEquals(40, attrs2[0]);
    assertEquals("jane.doe@example.com", attrs2[1]);
  }

  @Test
  public void testNestedArrayInStruct() throws SQLException {
    // Prepare attributes and metadata
    Map<String, Object> personData = new LinkedHashMap<>();
    personData.put("name", "John Doe");
    personData.put("tags", Arrays.asList("tag1", "tag2", "tag3"));

    String metadata = "STRUCT<name:STRING, tags:ARRAY<STRING>>";

    // Create Struct
    Struct struct = new DatabricksStruct(personData, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(2, attrs.length);
    assertEquals("John Doe", attrs[0]);

    // Ensure the tags field is properly parsed as an Array
    assertTrue(attrs[1] instanceof Array);
    Array tagsArray = (Array) attrs[1];
    Object[] tagElements = (Object[]) tagsArray.getArray();
    assertEquals(3, tagElements.length);
    assertEquals("tag1", tagElements[0]);
    assertEquals("tag2", tagElements[1]);
    assertEquals("tag3", tagElements[2]);
  }

  @Test
  public void testDatabricksStructWithMap() throws SQLException {
    // Prepare attributes and metadata
    Map<String, Object> addressMap = new LinkedHashMap<>();
    addressMap.put("home", "123 Main St");
    addressMap.put("work", "456 Office Rd");

    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("age", "30"); // String, but should be converted to int
    attributes.put("email", "john.doe@example.com");
    attributes.put("addresses", addressMap);

    String metadata = "STRUCT<age:INT, email:STRING, addresses:MAP<STRING, STRING>>";

    // Create Struct
    Struct struct = new DatabricksStruct(attributes, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(3, attrs.length);
    assertTrue(attrs[0] instanceof Integer);
    assertEquals(30, attrs[0]);
    assertEquals("john.doe@example.com", attrs[1]);

    // Test Map
    Map<String, String> addresses = (Map<String, String>) attrs[2];
    assertEquals("123 Main St", addresses.get("home"));
    assertEquals("456 Office Rd", addresses.get("work"));
  }

  @Test
  public void testDatabricksArrayWithMap() throws SQLException {
    // Prepare elements and metadata
    Map<String, Object> map1 = new LinkedHashMap<>();
    map1.put("key1", "100");
    map1.put("key2", "200");

    Map<String, Object> map2 = new LinkedHashMap<>();
    map2.put("key1", "300");
    map2.put("key2", "400");

    List<Object> elements = Arrays.asList(map1, map2);
    String metadata = "ARRAY<MAP<STRING, INT>>";

    // Create Array
    Array array = new DatabricksArray(elements, metadata);

    // Test getArray
    Object[] arrayElements = (Object[]) array.getArray();
    assertEquals(2, arrayElements.length);

    Map<String, Integer> mapElement1 = (Map<String, Integer>) arrayElements[0];
    Map<String, Integer> mapElement2 = (Map<String, Integer>) arrayElements[1];

    assertEquals(100, mapElement1.get("key1"));
    assertEquals(200, mapElement1.get("key2"));

    assertEquals(300, mapElement2.get("key1"));
    assertEquals(400, mapElement2.get("key2"));
  }

  @Test
  public void testArrayOfStructsWithNestedStructs() throws SQLException {
    // Prepare nested struct elements
    Map<String, Object> address1 = new LinkedHashMap<>();
    address1.put("city", "New York");
    address1.put("zip", "10001");

    Map<String, Object> structData1 = new LinkedHashMap<>();
    structData1.put("age", "30");
    structData1.put("email", "john.doe@example.com");
    structData1.put("address", address1);

    Map<String, Object> address2 = new LinkedHashMap<>();
    address2.put("city", "Los Angeles");
    address2.put("zip", "90001");

    Map<String, Object> structData2 = new LinkedHashMap<>();
    structData2.put("age", "40");
    structData2.put("email", "jane.doe@example.com");
    structData2.put("address", address2);

    List<Object> elements = Arrays.asList(structData1, structData2);
    String metadata =
        "ARRAY<STRUCT<age:INT, email:STRING, address:STRUCT<city:STRING, zip:STRING>>>";

    // Create Array
    Array array = new DatabricksArray(elements, metadata);

    // Test getArray
    Object[] arrayElements = (Object[]) array.getArray();

    assertEquals(2, arrayElements.length);

    Struct struct1 = (Struct) arrayElements[0];
    Struct struct2 = (Struct) arrayElements[1];

    Object[] attrs1 = struct1.getAttributes();
    Object[] attrs2 = struct2.getAttributes();

    assertEquals(30, attrs1[0]);
    assertEquals("john.doe@example.com", attrs1[1]);

    Struct addressStruct1 = (Struct) attrs1[2];
    Object[] addressAttrs1 = addressStruct1.getAttributes();
    assertEquals("New York", addressAttrs1[0]);
    assertEquals("10001", addressAttrs1[1]);

    assertEquals(40, attrs2[0]);
    assertEquals("jane.doe@example.com", attrs2[1]);

    Struct addressStruct2 = (Struct) attrs2[2];
    Object[] addressAttrs2 = addressStruct2.getAttributes();
    assertEquals("Los Angeles", addressAttrs2[0]);
    assertEquals("90001", addressAttrs2[1]);
  }

  @Test
  public void testArrayOfMapsWithStructs() throws SQLException {
    // Prepare map elements containing structs
    Map<String, Object> address1 = new LinkedHashMap<>();
    address1.put("city", "San Francisco");
    address1.put("zip", "94105");

    Map<String, Object> address2 = new LinkedHashMap<>();
    address2.put("city", "Chicago");
    address2.put("zip", "60601");

    Map<String, Object> map1 = new LinkedHashMap<>();
    map1.put("primary", address1);
    map1.put("secondary", address2);

    Map<String, Object> address3 = new LinkedHashMap<>();
    address3.put("city", "Seattle");
    address3.put("zip", "98101");

    Map<String, Object> address4 = new LinkedHashMap<>();
    address4.put("city", "Boston");
    address4.put("zip", "02101");

    Map<String, Object> map2 = new LinkedHashMap<>();
    map2.put("primary", address3);
    map2.put("secondary", address4);

    List<Object> elements = Arrays.asList(map1, map2);
    String metadata = "ARRAY<MAP<STRING, STRUCT<city:STRING, zip:STRING>>>";

    // Create Array
    Array array = new DatabricksArray(elements, metadata);

    // Test getArray
    Object[] arrayElements = (Object[]) array.getArray();
    assertEquals(2, arrayElements.length);

    Map<String, Struct> mapElement1 = (Map<String, Struct>) arrayElements[0];
    Map<String, Struct> mapElement2 = (Map<String, Struct>) arrayElements[1];

    Struct primary1 = mapElement1.get("primary");
    Struct secondary1 = mapElement1.get("secondary");

    Object[] primaryAttrs1 = primary1.getAttributes();
    Object[] secondaryAttrs1 = secondary1.getAttributes();

    assertEquals("San Francisco", primaryAttrs1[0]);
    assertEquals("94105", primaryAttrs1[1]);
    assertEquals("Chicago", secondaryAttrs1[0]);
    assertEquals("60601", secondaryAttrs1[1]);

    Struct primary2 = mapElement2.get("primary");
    Struct secondary2 = mapElement2.get("secondary");

    Object[] primaryAttrs2 = primary2.getAttributes();
    Object[] secondaryAttrs2 = secondary2.getAttributes();

    assertEquals("Seattle", primaryAttrs2[0]);
    assertEquals("98101", primaryAttrs2[1]);
    assertEquals("Boston", secondaryAttrs2[0]);
    assertEquals("02101", secondaryAttrs2[1]);
  }

  @Test
  public void testStructWithArrayOfStructs() throws SQLException {
    // Prepare attributes and metadata
    Map<String, Object> address1 = new LinkedHashMap<>();
    address1.put("city", "Miami");
    address1.put("zip", "33101");

    Map<String, Object> address2 = new LinkedHashMap<>();
    address2.put("city", "Dallas");
    address2.put("zip", "75201");

    List<Object> addresses = Arrays.asList(address1, address2);

    Map<String, Object> personData = new LinkedHashMap<>();
    personData.put("name", "Alice");
    personData.put("addresses", addresses);

    String metadata = "STRUCT<name:STRING, addresses:ARRAY<STRUCT<city:STRING, zip:STRING>>>";

    // Create Struct
    Struct struct = new DatabricksStruct(personData, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(2, attrs.length);
    assertEquals("Alice", attrs[0]);

    // Ensure the addresses field is properly parsed as an Array of Structs
    assertTrue(attrs[1] instanceof Array);
    Array addressesArray = (Array) attrs[1];
    Object[] addressElements = (Object[]) addressesArray.getArray();
    assertEquals(2, addressElements.length);

    Struct addressStruct1 = (Struct) addressElements[0];
    Struct addressStruct2 = (Struct) addressElements[1];

    Object[] addressAttrs1 = addressStruct1.getAttributes();
    Object[] addressAttrs2 = addressStruct2.getAttributes();

    assertEquals("Miami", addressAttrs1[0]);
    assertEquals("33101", addressAttrs1[1]);

    assertEquals("Dallas", addressAttrs2[0]);
    assertEquals("75201", addressAttrs2[1]);
  }

  @Test
  public void testComplexStructWithMapAndArray() throws SQLException {
    // Prepare attributes and metadata
    Map<String, Object> project1 = new LinkedHashMap<>();
    project1.put("name", "Project Alpha");
    project1.put("duration", "6 months");

    Map<String, Object> project2 = new LinkedHashMap<>();
    project2.put("name", "Project Beta");
    project2.put("duration", "3 months");

    List<Object> projects = Arrays.asList(project1, project2);

    Map<String, Object> teamLead = new LinkedHashMap<>();
    teamLead.put("name", "Bob");
    teamLead.put("age", "45");

    Map<String, Object> teamMember1 = new LinkedHashMap<>();
    teamMember1.put("name", "Charlie");
    teamMember1.put("age", "30");

    Map<String, Object> teamMember2 = new LinkedHashMap<>();
    teamMember2.put("name", "Dave");
    teamMember2.put("age", "35");

    Map<String, Object> team = new LinkedHashMap<>();
    team.put("lead", teamLead);
    team.put("members", Arrays.asList(teamMember1, teamMember2));

    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("team", team);
    attributes.put("projects", projects);

    String metadata =
        "STRUCT<team:STRUCT<lead:STRUCT<name:STRING, age:INT>, members:ARRAY<STRUCT<name:STRING, age:INT>>>, projects:ARRAY<STRUCT<name:STRING, duration:STRING>>>";

    // Create Struct
    Struct struct = new DatabricksStruct(attributes, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(2, attrs.length);

    // Check team structure
    Struct teamStruct = (Struct) attrs[0];
    Object[] teamAttrs = teamStruct.getAttributes();

    Struct leadStruct = (Struct) teamAttrs[0];
    Object[] leadAttrs = leadStruct.getAttributes();
    assertEquals("Bob", leadAttrs[0]);
    assertEquals(45, leadAttrs[1]);

    Array membersArray = (Array) teamAttrs[1];
    Object[] members = (Object[]) membersArray.getArray();
    assertEquals(2, members.length);

    Struct member1 = (Struct) members[0];
    Struct member2 = (Struct) members[1];

    Object[] member1Attrs = member1.getAttributes();
    Object[] member2Attrs = member2.getAttributes();

    assertEquals("Charlie", member1Attrs[0]);
    assertEquals(30, member1Attrs[1]);

    assertEquals("Dave", member2Attrs[0]);
    assertEquals(35, member2Attrs[1]);

    // Check projects array
    Array projectsArray = (Array) attrs[1];
    Object[] projectElements = (Object[]) projectsArray.getArray();
    assertEquals(2, projectElements.length);

    Struct projectStruct1 = (Struct) projectElements[0];
    Struct projectStruct2 = (Struct) projectElements[1];

    Object[] projectAttrs1 = projectStruct1.getAttributes();
    Object[] projectAttrs2 = projectStruct2.getAttributes();

    assertEquals("Project Alpha", projectAttrs1[0]);
    assertEquals("6 months", projectAttrs1[1]);

    assertEquals("Project Beta", projectAttrs2[0]);
    assertEquals("3 months", projectAttrs2[1]);
  }

  @Test
  public void testStructWithNullFields() throws SQLException {
    // Prepare struct with null fields
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("name", null); // Null value
    attributes.put("age", null); // Null value

    String metadata = "STRUCT<name:STRING, age:INT>";

    // Create Struct
    Struct struct = new DatabricksStruct(attributes, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(2, attrs.length);

    // Both fields should be null
    assertNull(attrs[0]); // Null name field
    assertNull(attrs[1]); // Null age field
  }

  // Test with empty Array
  @Test
  public void testEmptyArray() throws SQLException {
    // Prepare empty array and metadata
    List<Object> emptyElements = Arrays.asList();
    String metadata = "ARRAY<INT>";

    // Create Array
    Array array = new DatabricksArray(emptyElements, metadata);

    // Test getArray
    Object[] arrayElements = (Object[]) array.getArray();
    assertEquals(0, arrayElements.length);
  }

  // Test with empty Map
  @Test
  public void testEmptyMapInStruct() throws SQLException {
    // Prepare empty map and metadata
    Map<String, Object> emptyMap = new LinkedHashMap<>();
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("emptyMap", emptyMap);

    String metadata = "STRUCT<emptyMap:MAP<STRING, INT>>";

    // Create Struct
    Struct struct = new DatabricksStruct(attributes, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(1, attrs.length);

    // Test empty map
    Map<String, Integer> emptyMapResult = (Map<String, Integer>) attrs[0];
    assertEquals(0, emptyMapResult.size());
  }

  // Test with deep nested Struct
  @Test
  public void testDeepNestedStruct() throws SQLException {
    // Prepare deeply nested struct elements
    Map<String, Object> innerMostStruct = new LinkedHashMap<>();
    innerMostStruct.put("leaf", "value");

    Map<String, Object> middleStruct = new LinkedHashMap<>();
    middleStruct.put("inner", innerMostStruct);

    Map<String, Object> outerStruct = new LinkedHashMap<>();
    outerStruct.put("middle", middleStruct);

    String metadata = "STRUCT<middle:STRUCT<inner:STRUCT<leaf:STRING>>>";

    // Create Struct
    Struct struct = new DatabricksStruct(outerStruct, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(1, attrs.length);

    Struct middleStructResult = (Struct) attrs[0];
    Object[] middleAttrs = middleStructResult.getAttributes();
    assertEquals(1, middleAttrs.length);

    Struct innerStructResult = (Struct) middleAttrs[0];
    Object[] innerAttrs = innerStructResult.getAttributes();
    assertEquals(1, innerAttrs.length);

    assertEquals("value", innerAttrs[0]);
  }

  // Test Map with Array of Structs
  @Test
  public void testMapWithArrayOfStructs() throws SQLException {
    // Prepare struct elements and map
    Map<String, Object> person1 = new LinkedHashMap<>();
    person1.put("name", "John");
    person1.put("age", "30");

    Map<String, Object> person2 = new LinkedHashMap<>();
    person2.put("name", "Jane");
    person2.put("age", "25");

    List<Object> persons = Arrays.asList(person1, person2);

    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("people", persons);

    String metadata = "STRUCT<people:ARRAY<STRUCT<name:STRING, age:INT>>>";

    // Create Struct
    Struct struct = new DatabricksStruct(attributes, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(1, attrs.length);

    Array peopleArray = (Array) attrs[0];
    Object[] peopleElements = (Object[]) peopleArray.getArray();
    assertEquals(2, peopleElements.length);

    Struct personStruct1 = (Struct) peopleElements[0];
    Struct personStruct2 = (Struct) peopleElements[1];

    Object[] personAttrs1 = personStruct1.getAttributes();
    Object[] personAttrs2 = personStruct2.getAttributes();

    assertEquals("John", personAttrs1[0]);
    assertEquals(30, personAttrs1[1]);

    assertEquals("Jane", personAttrs2[0]);
    assertEquals(25, personAttrs2[1]);
  }

  // Test Struct with various primitive data types
  @Test
  public void testStructWithPrimitives() throws SQLException {
    // Prepare struct with various primitive types
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("intField", "123");
    attributes.put("floatField", "45.67");
    attributes.put("boolField", "true");
    attributes.put("stringField", "hello");

    String metadata =
        "STRUCT<intField:INT, floatField:FLOAT, boolField:BOOLEAN, stringField:STRING>";

    // Create Struct
    Struct struct = new DatabricksStruct(attributes, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(4, attrs.length);

    assertTrue(attrs[0] instanceof Integer);
    assertEquals(123, attrs[0]);

    assertTrue(attrs[1] instanceof Float);
    assertEquals(45.67f, attrs[1]);

    assertTrue(attrs[2] instanceof Boolean);
    assertEquals(true, attrs[2]);

    assertTrue(attrs[3] instanceof String);
    assertEquals("hello", attrs[3]);
  }

  @Test
  public void testArrayWithIntegers() throws SQLException {
    // Prepare elements of the same type (Integers)
    List<Object> elements = Arrays.asList(123, 456, 789);
    String metadata = "ARRAY<INT>";

    // Create Array
    Array array = new DatabricksArray(elements, metadata);

    // Test getArray
    Object[] arrayElements = (Object[]) array.getArray();
    assertEquals(3, arrayElements.length);

    assertEquals(123, arrayElements[0]);
    assertEquals(456, arrayElements[1]);
    assertEquals(789, arrayElements[2]);
  }

  // Test deeply nested Array
  @Test
  public void testDeepNestedArray() throws SQLException {
    // Prepare deeply nested arrays
    List<Object> innerArray = Arrays.asList(1, 2, 3);
    List<Object> middleArray = Arrays.asList(innerArray);
    List<Object> outerArray = Arrays.asList(middleArray);

    String metadata = "ARRAY<ARRAY<ARRAY<INT>>>";

    // Create Array
    Array array = new DatabricksArray(outerArray, metadata);

    // Test getArray
    Object[] arrayElements = (Object[]) array.getArray();
    assertEquals(1, arrayElements.length);

    Array middleArrayResult = (Array) arrayElements[0];
    Object[] middleArrayElements = (Object[]) middleArrayResult.getArray();
    assertEquals(1, middleArrayElements.length);

    Array innerArrayResult = (Array) middleArrayElements[0];
    Object[] innerArrayElements = (Object[]) innerArrayResult.getArray();
    assertEquals(3, innerArrayElements.length);

    assertEquals(1, innerArrayElements[0]);
    assertEquals(2, innerArrayElements[1]);
    assertEquals(3, innerArrayElements[2]);
  }

  @Test
  public void testStructWithSQLTypes() throws SQLException {
    // Prepare struct with various SQL types
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("intField", "123");
    attributes.put("bigIntField", "123456789012345");
    attributes.put("decimalField", "12345.6789");
    attributes.put("dateField", "2024-08-28");
    attributes.put("timestampField", "2024-08-28 12:34:56");
    attributes.put("booleanField", "true");
    attributes.put("binaryField", "binaryData".getBytes());

    String metadata =
        "STRUCT<intField:INT, bigIntField:BIGINT, decimalField:DECIMAL, dateField:DATE, timestampField:TIMESTAMP, booleanField:BOOLEAN, binaryField:BINARY>";

    // Create Struct
    Struct struct = new DatabricksStruct(attributes, metadata);

    // Test getAttributes
    Object[] attrs = struct.getAttributes();
    assertEquals(7, attrs.length);

    assertTrue(attrs[0] instanceof Integer);
    assertEquals(123, attrs[0]);

    assertTrue(attrs[1] instanceof Long);
    assertEquals(123456789012345L, attrs[1]);

    assertTrue(attrs[2] instanceof BigDecimal);
    assertEquals(new BigDecimal("12345.6789"), attrs[2]);

    assertTrue(attrs[3] instanceof Date);
    assertEquals(Date.valueOf("2024-08-28"), attrs[3]);

    assertTrue(attrs[4] instanceof Timestamp);
    assertEquals(Timestamp.valueOf("2024-08-28 12:34:56"), attrs[4]);

    assertTrue(attrs[5] instanceof Boolean);
    assertEquals(true, attrs[5]);

    assertTrue(attrs[6] instanceof byte[]);
    assertArrayEquals("binaryData".getBytes(), (byte[]) attrs[6]);
  }
}
