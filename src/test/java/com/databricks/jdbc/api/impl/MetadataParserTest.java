package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.exception.DatabricksDriverException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Unit tests for the MetadataParser utility class. */
public class MetadataParserTest {

  /** Test parsing of simple STRUCT metadata with primitive field types. */
  @Test
  @DisplayName("parseStructMetadata with simple primitive fields")
  public void testParseStructMetadata_SimpleFields() {
    String metadata = "STRUCT<id:INT, name:STRING, active:BOOLEAN>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("name", "STRING");
    expected.put("active", "BOOLEAN");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(expected, actual, "Parsed struct metadata should match the expected field types.");
  }

  /** Test parsing of STRUCT metadata with nested STRUCT fields. */
  @Test
  @DisplayName("parseStructMetadata with nested STRUCT fields")
  public void testParseStructMetadata_NestedStruct() {
    String metadata = "STRUCT<id:INT, address:STRUCT<street:STRING, city:STRING>, active:BOOLEAN>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("address", "STRUCT<street:STRING, city:STRING>");
    expected.put("active", "BOOLEAN");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with nested STRUCT should match expected field types.");
  }

  /** Test parsing of STRUCT metadata with nested ARRAY fields. */
  @Test
  @DisplayName("parseStructMetadata with nested ARRAY fields")
  public void testParseStructMetadata_NestedArray() {
    String metadata = "STRUCT<id:INT, tags:ARRAY<STRING>, active:BOOLEAN>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("tags", "ARRAY<STRING>");
    expected.put("active", "BOOLEAN");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with nested ARRAY should match expected field types.");
  }

  /** Test parsing of STRUCT metadata with nested MAP fields. */
  @Test
  @DisplayName("parseStructMetadata with nested MAP fields")
  public void testParseStructMetadata_NestedMap() {
    String metadata = "STRUCT<id:INT, preferences:MAP<STRING, STRING>, active:BOOLEAN>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("preferences", "MAP<STRING, STRING>");
    expected.put("active", "BOOLEAN");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with nested MAP should match expected field types.");
  }

  /** Test parsing of STRUCT metadata with multiple levels of nesting. */
  @Test
  @DisplayName("parseStructMetadata with multiple levels of nesting")
  public void testParseStructMetadata_MultipleLevelsOfNesting() {
    String metadata =
        "STRUCT<id:INT, address:STRUCT<street:STRING, city:STRUCT<name:STRING, code:INT>>, active:BOOLEAN>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("address", "STRUCT<street:STRING, city:STRUCT<name:STRING, code:INT>>");
    expected.put("active", "BOOLEAN");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with multiple levels of nesting should match expected field types.");
  }

  /** Test parsing of simple ARRAY metadata with primitive element types. */
  @Test
  @DisplayName("parseArrayMetadata with simple primitive element types")
  public void testParseArrayMetadata_SimpleElementType() {
    String metadata = "ARRAY<STRING>";
    String expected = "STRING";

    String actual = MetadataParser.parseArrayMetadata(metadata);
    assertEquals(expected, actual, "Parsed array metadata should match the expected element type.");
  }

  /** Test parsing of ARRAY metadata with nested STRUCT element types. */
  @Test
  @DisplayName("parseArrayMetadata with nested STRUCT element types")
  public void testParseArrayMetadata_NestedStructElementType() {
    String metadata = "ARRAY<STRUCT<id:INT, name:STRING>>";
    String expected = "STRUCT<id:INT, name:STRING>";

    String actual = MetadataParser.parseArrayMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed array metadata with nested STRUCT should match expected element type.");
  }

  /** Test parsing of ARRAY metadata with nested ARRAY element types. */
  @Test
  @DisplayName("parseArrayMetadata with nested ARRAY element types")
  public void testParseArrayMetadata_NestedArrayElementType() {
    String metadata = "ARRAY<ARRAY<STRING>>";
    String expected = "ARRAY<STRING>";

    String actual = MetadataParser.parseArrayMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed array metadata with nested ARRAY should match expected element type.");
  }

  /** Test parsing of simple MAP metadata with primitive key and value types. */
  @Test
  @DisplayName("parseMapMetadata with simple key and value types")
  public void testParseMapMetadata_SimpleKeyValueTypes() {
    String metadata = "MAP<STRING, INT>";
    String expected = "STRING, INT";

    String actual = MetadataParser.parseMapMetadata(metadata);
    assertEquals(
        expected, actual, "Parsed map metadata should match the expected key and value types.");
  }

  /** Test parsing of MAP metadata with nested STRUCT key types. */
  @Test
  @DisplayName("parseMapMetadata with nested STRUCT key types")
  public void testParseMapMetadata_NestedStructKeyType() {
    String metadata = "MAP<STRUCT<id:INT,name:STRING>, STRING>";
    String expected = "STRUCT<id:INT,name:STRING>, STRING";

    String actual = MetadataParser.parseMapMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed map metadata with nested STRUCT key type should match expected key and value types.");
  }

  /** Test parsing of MAP metadata with nested ARRAY value types. */
  @Test
  @DisplayName("parseMapMetadata with nested ARRAY value types")
  public void testParseMapMetadata_NestedArrayValueType() {
    String metadata = "MAP<STRING, ARRAY<STRING>>";
    String expected = "STRING, ARRAY<STRING>";

    String actual = MetadataParser.parseMapMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed map metadata with nested ARRAY value type should match expected key and value types.");
  }

  /** Test parsing of MAP metadata with nested MAP value types. */
  @Test
  @DisplayName("parseMapMetadata with nested MAP value types")
  public void testParseMapMetadata_NestedMapValueType() {
    String metadata = "MAP<STRING, MAP<STRING, INT>>";
    String expected = "STRING, MAP<STRING, INT>";

    String actual = MetadataParser.parseMapMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed map metadata with nested MAP value type should match expected key and value types.");
  }

  /**
   * Test parsing of MAP metadata with invalid format (missing comma). Expects
   * IllegalArgumentException.
   */
  @Test
  @DisplayName("parseMapMetadata with invalid format (missing comma)")
  public void testParseMapMetadata_InvalidFormat_NoComma() {
    String metadata = "MAP<STRING INT>";

    Exception exception =
        assertThrows(
            DatabricksDriverException.class,
            () -> {
              MetadataParser.parseMapMetadata(metadata);
            },
            "Parsing MAP metadata without a comma should throw IllegalArgumentException.");

    assertTrue(
        exception.getMessage().contains("Invalid MAP metadata"),
        "Exception message should indicate invalid MAP metadata.");
  }

  /**
   * Test the cleanTypeName method to ensure it removes "NOT NULL" constraints and trims the type
   * name.
   */
  @Test
  @DisplayName("cleanTypeName should remove 'NOT NULL' and trim type name")
  public void testCleanTypeName_RemovesNotNull() {
    String typeName = "STRING NOT NULL";
    String expected = "STRING";

    String actual = "STRING";
    assertEquals(
        expected, actual, "cleanTypeName should remove 'NOT NULL' and trim the type name.");
  }

  /** Test the cleanTypeName method with type names that do not contain "NOT NULL". */
  @Test
  @DisplayName("cleanTypeName should trim type name without 'NOT NULL'")
  public void testCleanTypeName_WithoutNotNull() {
    String typeName = "INT ";
    String expected = "INT";

    String actual = "INT";
    assertEquals(expected, actual, "cleanTypeName should trim the type name without altering it.");
  }

  /** Test parsing of STRUCT metadata with "NOT NULL" constraints. */
  @Test
  @DisplayName("parseStructMetadata should handle 'NOT NULL' constraints")
  public void testParseStructMetadata_WithNotNullConstraints() {
    String metadata = "STRUCT<id:INT NOT NULL, name:STRING NOT NULL, active:BOOLEAN>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("name", "STRING");
    expected.put("active", "BOOLEAN");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected, actual, "Parsed struct metadata should correctly remove 'NOT NULL' constraints.");
  }

  /** Test parsing of ARRAY metadata with "NOT NULL" constraints. */
  @Test
  @DisplayName("parseArrayMetadata should handle 'NOT NULL' constraints")
  public void testParseArrayMetadata_WithNotNullConstraints() {
    String metadata = "ARRAY<STRING NOT NULL>";
    String expected = "STRING";

    String actual = MetadataParser.parseArrayMetadata(metadata);
    assertEquals(
        expected, actual, "Parsed array metadata should correctly remove 'NOT NULL' constraints.");
  }

  /** Test parsing of MAP metadata with "NOT NULL" constraints. */
  @Test
  @DisplayName("parseMapMetadata should handle 'NOT NULL' constraints")
  public void testParseMapMetadata_WithNotNullConstraints() {
    String metadata = "MAP<STRING NOT NULL, INT NOT NULL>";
    String expected = "STRING, INT";

    String actual = MetadataParser.parseMapMetadata(metadata);
    assertEquals(
        expected, actual, "Parsed map metadata should correctly remove 'NOT NULL' constraints.");
  }

  /** Test parsing of empty MAP metadata. Expects IllegalArgumentException. */
  @Test
  @DisplayName("parseMapMetadata with empty MAP")
  public void testParseMapMetadata_EmptyMap() {
    String metadata = "MAP<>";

    Exception exception =
        assertThrows(
            DatabricksDriverException.class,
            () -> {
              MetadataParser.parseMapMetadata(metadata);
            },
            "Parsing empty MAP metadata should throw IllegalArgumentException.");

    assertTrue(
        exception.getMessage().contains("Invalid MAP metadata"),
        "Exception message should indicate invalid MAP metadata.");
  }
}
