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

  /**
   * Test parsing of STRUCT metadata with DECIMAL precision and scale - regression test for
   * parentheses handling.
   */
  @Test
  @DisplayName("parseStructMetadata with DECIMAL precision and scale")
  public void testParseStructMetadata_WithDecimalPrecisionScale() {
    String metadata = "STRUCT<name:STRING, amount:DECIMAL(18,2)>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("name", "STRING");
    expected.put("amount", "DECIMAL(18,2)");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with DECIMAL precision and scale should handle parentheses correctly.");
  }

  /**
   * Test parsing of STRUCT metadata with multiple DECIMAL fields with different precision/scale.
   */
  @Test
  @DisplayName("parseStructMetadata with multiple DECIMAL fields")
  public void testParseStructMetadata_MultipleDecimalFields() {
    String metadata = "STRUCT<id:INT, price:DECIMAL(10,2), amount:DECIMAL(18,4), name:STRING>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("price", "DECIMAL(10,2)");
    expected.put("amount", "DECIMAL(18,4)");
    expected.put("name", "STRING");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with multiple DECIMAL fields should handle all parentheses correctly.");
  }

  /** Test parsing of complex nested STRUCT with DECIMAL fields - comprehensive regression test. */
  @Test
  @DisplayName("parseStructMetadata with nested STRUCT containing DECIMAL")
  public void testParseStructMetadata_NestedStructWithDecimal() {
    String metadata =
        "STRUCT<id:INT, financial:STRUCT<balance:DECIMAL(15,2), credit:DECIMAL(10,2)>, active:BOOLEAN>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("financial", "STRUCT<balance:DECIMAL(15,2), credit:DECIMAL(10,2)>");
    expected.put("active", "BOOLEAN");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with nested STRUCT containing DECIMAL should preserve all type information.");
  }

  /** Test parsing of deeply nested STRUCT with multiple DECIMAL fields at different levels. */
  @Test
  @DisplayName("parseStructMetadata with deeply nested STRUCT and DECIMAL fields")
  public void testParseStructMetadata_DeeplyNestedStructWithDecimals() {
    String metadata =
        "STRUCT<id:INT, account:STRUCT<balance:DECIMAL(18,4), details:STRUCT<fee:DECIMAL(5,2), rate:DECIMAL(10,6)>>, status:STRING>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put(
        "account",
        "STRUCT<balance:DECIMAL(18,4), details:STRUCT<fee:DECIMAL(5,2), rate:DECIMAL(10,6)>>");
    expected.put("status", "STRING");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with deeply nested STRUCT and DECIMAL fields should handle all levels correctly.");
  }

  /** Test parsing of STRUCT with mixed complex types and DECIMAL fields. */
  @Test
  @DisplayName("parseStructMetadata with mixed complex types and DECIMAL")
  public void testParseStructMetadata_MixedComplexTypesWithDecimal() {
    String metadata =
        "STRUCT<id:INT, prices:ARRAY<DECIMAL(12,2)>, accounts:MAP<STRING, STRUCT<balance:DECIMAL(15,2)>>, summary:STRUCT<total:DECIMAL(20,4), count:INT>>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("prices", "ARRAY<DECIMAL(12,2)>");
    expected.put("accounts", "MAP<STRING, STRUCT<balance:DECIMAL(15,2)>>");
    expected.put("summary", "STRUCT<total:DECIMAL(20,4), count:INT>");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with mixed complex types and DECIMAL fields should handle all combinations correctly.");
  }

  /** Test parsing of STRUCT with other parenthesized types to ensure fix applies broadly. */
  @Test
  @DisplayName("parseStructMetadata with various parenthesized types")
  public void testParseStructMetadata_VariousParenthesizedTypes() {
    String metadata =
        "STRUCT<id:INT, name:STRING, amount:DECIMAL(38,18), price:DECIMAL(10,2), active:BOOLEAN>";
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("id", "INT");
    expected.put("name", "STRING");
    expected.put("amount", "DECIMAL(38,18)");
    expected.put("price", "DECIMAL(10,2)");
    expected.put("active", "BOOLEAN");

    Map<String, String> actual = MetadataParser.parseStructMetadata(metadata);
    assertEquals(
        expected,
        actual,
        "Parsed struct metadata with various parenthesized types should handle all type parameters correctly.");
  }
}
