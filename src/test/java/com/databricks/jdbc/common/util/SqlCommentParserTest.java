package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class SqlCommentParserTest {

  @Test
  public void testNullInput() {
    assertNull(SqlCommentParser.stripCommentsAndWhitespaces(null));
  }

  @Test
  public void testEmptyInput() {
    assertEquals("", SqlCommentParser.stripCommentsAndWhitespaces(""));
  }

  @Test
  public void testNoComments() {
    String sql = "SELECT 'foo', \"bar\", `baz` FROM table";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  // --- Line comments ---

  @Test
  public void testLineCommentAtStart() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "-- comment\nSELECT 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testLineCommentInMiddle() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT 'foo', \"bar\", `baz` -- comment\nFROM table"));
  }

  @Test
  public void testLineCommentAtEnd() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT 'foo', \"bar\", `baz` FROM table -- comment"));
  }

  @Test
  public void testOnlyLineComment() {
    assertEquals("", SqlCommentParser.stripCommentsAndWhitespaces("-- just a comment"));
  }

  @Test
  public void testNestedLineComment() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "-- comment -- comment\nSELECT 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testLineCommentWithCarriageReturn() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "-- comment\rSELECT 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testLineCommentWithCarriageReturnNewline() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "-- comment\r\nSELECT 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testMultipleLineComments() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "-- comment\nSELECT 'foo', \"bar\", `baz` -- comment\n--comment\nFROM table -- comment\n-- comment"));
  }

  // --- Single line block comment tests ---

  @Test
  public void testSingleLineBlockCommentAtStart() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "/* comment */ SELECT 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testSingleLineBlockCommentInMiddle() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT /* comment */ 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testSingleLineBlockCommentAtEnd() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT 'foo', \"bar\", `baz` FROM table /* comment */"));
  }

  @Test
  public void testOnlySingleLineBlockComment() {
    assertEquals("", SqlCommentParser.stripCommentsAndWhitespaces("/* just a comment */"));
  }

  @Test
  public void testMultipleSingleLineBlockComment() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "/* a */ SELECT /* b */ 'foo', \"bar\", `baz` FROM table /* c */"));
  }

  @Test
  public void testAdjacentSingleLineBlockComment() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT /* a */ /* b */ 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testNestedSingleLineBlockComment() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT /* outer /* inner */ outer */ 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testTripleNestedSingleLineBlockComment() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT /* a /* b /* c */ b */ a */ 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testUnterminatedSingleLineBlockComment() {
    assertEquals("SELECT", SqlCommentParser.stripCommentsAndWhitespaces("SELECT /* never closed"));
  }

  // --- Multi line block comment tests ---

  @Test
  public void testMultiLineBlockCommentAtStart() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "/*\nmulti line\n*/ SELECT 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testMultiLineBlockCommentInMiddle() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT /*\nmulti line\n*/ 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testMultiLineBlockCommentAtEnd() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT 'foo', \"bar\", `baz` FROM table /*\nmulti line\n*/"));
  }

  @Test
  public void testOnlyMultiLineBlockComment() {
    assertEquals("", SqlCommentParser.stripCommentsAndWhitespaces("/*\nmulti line\n*/"));
  }

  @Test
  public void testMultipleMultiLineBlockComment() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "/*\nmulti line\n*/ SELECT /*\nmulti line\n*/ 'foo', \"bar\", `baz` FROM table /*\nmulti line\n*/"));
  }

  @Test
  public void testAdjacentMultiLineBlockComment() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT /*\nmulti line\n*/ /*\nmulti line\n*/ 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testNestedMultiLineBlockComment() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT /*\nouter\n/*\ninner\n*/\nouter\n*/ 'foo', \"bar\", `baz` FROM table"));
  }

  @Test
  public void testUnterminatedMultiLineBlockComment() {
    assertEquals(
        "SELECT", SqlCommentParser.stripCommentsAndWhitespaces("SELECT /*\nnever\nclosed"));
  }

  // --- Single quote string tests ---

  @Test
  public void testLineCommentInsideSingleQuoteString() {
    String sql = "SELECT 'val -- not a comment'";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testSingleLineBlockCommentInsideSingleQuoteString() {
    String sql = "SELECT 'val /* not a comment */'";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testMultiLineBlockCommentInsideSingleQuoteString() {
    String sql = "SELECT 'val /*\nnot a comment\n*/'";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testEscapedSingleQuoteWithLineComment() {
    String sql = "SELECT 'it''s -- not a comment'";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testEscapedSingleQuoteWithSingleLineBlockComment() {
    String sql = "SELECT 'it''s /* not a comment */'";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testEscapedSingleQuoteWithMultiLineBlockComment() {
    String sql = "SELECT 'it''s /*\nnot a comment\n*/'";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testUnterminatedSingleQuoteString() {
    String sql = "SELECT 'never closed";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  // --- Double quote identifier tests ---

  @Test
  public void testLineCommentInsideDoubleQuoteIdentifier() {
    String sql = "SELECT \"col -- not a comment\" FROM table";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testEscapedDoubleQuoteWithLineComment() {
    String sql = "SELECT \"a\"\"b -- not a comment\" FROM table";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testUnterminatedDoubleQuote() {
    String sql = "SELECT \"never closed";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  // --- Backtick identifier tests ---

  @Test
  public void testLineCommentInsideBacktickIdentifier() {
    String sql = "SELECT `col -- not a comment` FROM table";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testEscapedBacktickWithLineComment() {
    String sql = "SELECT `a``b -- not a comment` FROM table";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testUnterminatedBacktick() {
    String sql = "SELECT `never closed";
    assertEquals(sql, SqlCommentParser.stripCommentsAndWhitespaces(sql));
  }

  @Test
  public void testTokenFusionPrevention() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "SELECT/**/'foo',/**/\"bar\",/**/`baz` FROM table"));
  }

  @Test
  public void testMixedCommentTypes() {
    assertEquals(
        "SELECT 'foo', \"bar\", `baz` FROM table",
        SqlCommentParser.stripCommentsAndWhitespaces(
            "/*\nmulti line\n*/ /* single line */ -- comment\nSELECT 'foo', \"bar\", `baz` /*\nmulti line\n*/ -- comment\n/* single line */ FROM table -- comment\n/*\nmulti line\n*/ /* single line */"));
  }

  // --- forEach tests ---

  @Test
  public void testForEachNullInput() {
    List<Character> chars = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar(null, (state, c) -> chars.add(c));
    assertTrue(chars.isEmpty());
  }

  @Test
  public void testForEachEmptyInput() {
    List<Character> chars = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar("", (state, c) -> chars.add(c));
    assertTrue(chars.isEmpty());
  }

  @Test
  public void testForEachNormalCharsHaveNormalState() {
    List<SqlCommentParser.State> states = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar("SELECT * FROM table", (state, c) -> states.add(state));
    assertTrue(states.stream().allMatch(s -> s == SqlCommentParser.State.NORMAL));
  }

  @Test
  public void testForEachLineCommentsAreIgnored() {
    List<SqlCommentParser.State> states = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar("-- comment", (state, c) -> states.add(state));
    assertEquals(0, states.size());
  }

  @Test
  public void testForEachSingleLineBlockCommentsAreIgnored() {
    List<SqlCommentParser.State> states = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar("/* single line */", (state, c) -> states.add(state));
    assertEquals(1, states.size());
    assertEquals(SqlCommentParser.State.NORMAL, states.get(0));
  }

  @Test
  public void testForEachMultiLineBlockCommentsAreIgnored() {
    List<SqlCommentParser.State> states = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar("/*\nmulti line\n*/", (state, c) -> states.add(state));
    assertEquals(1, states.size());
    assertEquals(SqlCommentParser.State.NORMAL, states.get(0));
  }

  @Test
  public void testForEachSingleQuotedCharsHaveSingleQuotedState() {
    List<SqlCommentParser.State> states = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar(
        "'foo -- comment /* comment */'", (state, c) -> states.add(state));
    assertTrue(states.stream().allMatch(s -> s == SqlCommentParser.State.IN_SINGLE_QUOTE));
  }

  @Test
  public void testForEachDoubleQuotedCharsHaveDoubleQuotedState() {
    List<SqlCommentParser.State> states = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar("\"foo -- comment\"", (state, c) -> states.add(state));
    assertTrue(states.stream().allMatch(s -> s == SqlCommentParser.State.IN_DOUBLE_QUOTE));
  }

  @Test
  public void testForEachEmitsSyntheticSpaceOnBlockCommentExit() {
    List<Character> chars = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar("a/*x*/b", (state, c) -> chars.add(c));
    assertEquals(Arrays.asList('a', ' ', 'b'), chars);
  }

  @Test
  public void testForEachEmitsSyntheticSpaceOnLineCommentExit() {
    List<Character> chars = new ArrayList<>();
    SqlCommentParser.forEachNonCommentChar("a--x\nb", (state, c) -> chars.add(c));
    assertEquals(Arrays.asList('a', ' ', 'b'), chars);
  }
}
