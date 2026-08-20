package com.databricks.jdbc.comparator.suite;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/** Loads human-accepted, read-only APC comparator cases from a local JSON manifest. */
final class ApcCustomTestManifest {
  static final String PROPERTY = "APC_CUSTOM_TEST_MANIFEST";

  private static final int SCHEMA_VERSION = 1;
  private static final int MAX_CASES = 20;
  private static final int MAX_MANIFEST_BYTES = 256_000;
  private static final int MAX_TITLE_LENGTH = 120;
  private static final int MAX_SQL_LENGTH = 16_000;
  private static final Set<String> ROOT_FIELDS = Set.of("schema_version", "cases");
  private static final Set<String> CASE_FIELDS = Set.of("id", "title", "sql");
  private static final Pattern CASE_ID = Pattern.compile("[a-z][a-z0-9-]{0,63}");
  private static final Pattern SELECT = Pattern.compile("(?is)^\\s*SELECT\\b");
  private static final Pattern UNSUPPORTED_SQL =
      Pattern.compile("(?is)\\b(?:INFORMATION_SCHEMA|SYSTEM\\.)");
  private static final ObjectMapper MAPPER =
      new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

  private ApcCustomTestManifest() {}

  static List<TestCase> load() {
    String configuredPath = System.getProperty(PROPERTY);
    if (configuredPath == null || configuredPath.isBlank()) {
      return List.of();
    }

    Path path = Path.of(configuredPath);
    if (!path.isAbsolute()
        || Files.isSymbolicLink(path)
        || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
      throw invalid("manifest must be an absolute, regular, non-symbolic-link file");
    }

    try {
      if (Files.size(path) > MAX_MANIFEST_BYTES) {
        throw invalid("manifest exceeds " + MAX_MANIFEST_BYTES + " bytes");
      }
      JsonNode root = MAPPER.readTree(path.toFile());
      requireObject(root, ROOT_FIELDS, "manifest");
      JsonNode version = root.get("schema_version");
      if (!version.isInt() || version.intValue() != SCHEMA_VERSION) {
        throw invalid("schema_version must be " + SCHEMA_VERSION);
      }

      JsonNode cases = root.get("cases");
      if (!cases.isArray() || cases.isEmpty() || cases.size() > MAX_CASES) {
        throw invalid("cases must contain between 1 and " + MAX_CASES + " entries");
      }

      List<TestCase> testCases = new ArrayList<>();
      Set<String> caseIds = new HashSet<>();
      for (JsonNode testCase : cases) {
        requireObject(testCase, CASE_FIELDS, "case");
        String id = requireText(testCase, "id");
        String title = requireText(testCase, "title");
        String sql = requireText(testCase, "sql");

        if (!CASE_ID.matcher(id).matches()) {
          throw invalid("case id must match " + CASE_ID.pattern());
        }
        if (!caseIds.add(id)) {
          throw invalid("duplicate case id: " + id);
        }
        if (title.length() > MAX_TITLE_LENGTH) {
          throw invalid("case title exceeds " + MAX_TITLE_LENGTH + " characters");
        }
        if (title.contains("\n") || title.contains("\r") || title.contains("\0")) {
          throw invalid("case title must be one line");
        }
        validateSql(sql);
        testCases.add(new TestCase(sql, "[" + id + "] " + title));
      }
      return List.copyOf(testCases);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "Cannot read APC custom test manifest: " + e.getMessage(), e);
    }
  }

  private static void requireObject(JsonNode node, Set<String> fields, String label) {
    if (node == null || !node.isObject()) {
      throw invalid(label + " must be a JSON object");
    }
    Set<String> actualFields = new HashSet<>();
    Iterator<String> names = node.fieldNames();
    names.forEachRemaining(actualFields::add);
    if (!actualFields.equals(fields)) {
      throw invalid(label + " fields must be exactly " + fields);
    }
  }

  private static String requireText(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (!value.isTextual() || value.textValue().isBlank()) {
      throw invalid(field + " must be a non-empty string");
    }
    String text = value.textValue();
    if (!text.equals(text.trim())) {
      throw invalid(field + " must not have surrounding whitespace");
    }
    return text;
  }

  private static void validateSql(String sql) {
    if (sql.length() > MAX_SQL_LENGTH) {
      throw invalid("SQL exceeds " + MAX_SQL_LENGTH + " characters");
    }
    if (sql.contains("\0")) {
      throw invalid("SQL must not contain a null character");
    }
    if (!SELECT.matcher(sql).find()) {
      throw invalid("SQL must be one SELECT statement");
    }
    if (sql.contains(";")
        || sql.contains("--")
        || sql.contains("/*")
        || sql.contains("*/")
        || sql.contains("${")
        || sql.contains("{{")) {
      throw invalid("SQL must not contain multiple statements, comments, or substitutions");
    }
    if (UNSUPPORTED_SQL.matcher(sql).find()) {
      throw invalid("SQL must not use excluded metadata surfaces");
    }
  }

  private static IllegalArgumentException invalid(String message) {
    return new IllegalArgumentException("Invalid APC custom test manifest: " + message);
  }
}
