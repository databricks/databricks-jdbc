package com.databricks.jdbc.common.util;

import java.util.ArrayList;
import java.util.List;

/** Utility class for parsing out comments from SQL strings */
public class SqlCommentParser {

  public enum State {
    NORMAL,
    IN_SINGLE_QUOTE,
    IN_DOUBLE_QUOTE,
    IN_BACKTICK,
    IN_LINE_COMMENT,
    IN_BLOCK_COMMENT
  }

  @FunctionalInterface
  public interface SqlCharConsumer {
    void accept(State state, char c);
  }

  /**
   * Index-aware variant of {@link SqlCharConsumer}. The third argument is the source-string index
   * of the emitted character. For synthetic characters (the space emitted after a comment), the
   * index is the position of the character that ended the comment.
   */
  @FunctionalInterface
  public interface IndexedSqlCharConsumer {
    void accept(State state, char c, int sourceIndex);
  }

  /**
   * Iterates over each character in the SQL string while keeping track of comment, string literal,
   * and identifier state. Each character that is not part of a comment calls the consumer with the
   * current state and character. Emits a ' ' character after each comment to avoid token fusion.
   * Quote boundaries follow Spark's {@code SqlBaseLexer}; parser settings that change how token
   * contents are interpreted do not change those boundaries.
   *
   * @param sql the SQL string to parse
   * @param consumer called for each visible character with its parsing state
   */
  public static void forEachNonCommentChar(String sql, SqlCharConsumer consumer) {
    forEachNonCommentChar(sql, (state, c, sourceIndex) -> consumer.accept(state, c));
  }

  /**
   * Same as {@link #forEachNonCommentChar(String, SqlCharConsumer)} but the consumer also receives
   * the source-string index of each emitted character. Useful when callers need to relate emitted
   * characters back to positions in the original SQL (e.g. parameter placeholder discovery).
   */
  public static void forEachNonCommentChar(String sql, IndexedSqlCharConsumer consumer) {
    forEachNonCommentChar(sql, consumer, false);
  }

  /** Iterate APC SQL using the same escaped-quote and raw-literal rules as placeholder parsing. */
  public static void forEachApcNonCommentChar(String sql, IndexedSqlCharConsumer consumer) {
    forEachNonCommentChar(sql, consumer, true);
  }

  private static void forEachNonCommentChar(
      String sql, IndexedSqlCharConsumer consumer, boolean apcPlaceholders) {
    if (sql == null || sql.isEmpty()) {
      return;
    }

    State state = State.NORMAL;
    int blockCommentDepth = 0;
    boolean rawLiteral = false;

    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      char next = (i + 1 < sql.length()) ? sql.charAt(i + 1) : '\0';

      switch (state) {
        case NORMAL:
          if (c == '-' && next == '-') {
            state = State.IN_LINE_COMMENT;
            i++; // skip second '-'
          } else if (c == '/' && next == '*') {
            state = State.IN_BLOCK_COMMENT;
            blockCommentDepth = 1;
            i++; // skip '*'
          } else if (c == '\'') {
            rawLiteral = apcPlaceholders ? isRawLiteralPrefix(sql, i) : hasRawStringPrefix(sql, i);
            state = State.IN_SINGLE_QUOTE;
            consumer.accept(state, c, i);
          } else if (c == '"') {
            rawLiteral = apcPlaceholders ? isRawLiteralPrefix(sql, i) : hasRawStringPrefix(sql, i);
            state = State.IN_DOUBLE_QUOTE;
            consumer.accept(state, c, i);
          } else if (c == '`') {
            state = State.IN_BACKTICK;
            consumer.accept(state, c, i);
          } else {
            consumer.accept(state, c, i);
          }
          break;

        case IN_SINGLE_QUOTE:
          consumer.accept(state, c, i);
          if (!rawLiteral && c == '\\' && next != '\0') {
            consumer.accept(state, next, i + 1);
            i++; // skip backslash-escaped character
          } else if (!rawLiteral && c == '\'' && next == '\'') {
            consumer.accept(state, next, i + 1);
            i++; // skip escaped quote
          } else if (c == '\'') {
            state = State.NORMAL;
            rawLiteral = false;
          }
          break;

        case IN_DOUBLE_QUOTE:
          consumer.accept(state, c, i);
          if (!rawLiteral && c == '\\' && next != '\0') {
            consumer.accept(state, next, i + 1);
            i++; // skip backslash-escaped character
          } else if (!rawLiteral && c == '"' && next == '"') {
            consumer.accept(state, next, i + 1);
            i++; // skip escaped quote
          } else if (c == '"') {
            state = State.NORMAL;
            rawLiteral = false;
          }
          break;

        case IN_BACKTICK:
          consumer.accept(state, c, i);
          if (c == '`' && next == '`') {
            consumer.accept(state, next, i + 1);
            i++; // skip escaped backtick
          } else if (c == '`') {
            state = State.NORMAL;
          }
          break;

        case IN_LINE_COMMENT:
          if (c == '\\' && next == '\n') {
            i++;
          } else if (c == '\n' || c == '\r') {
            state = State.NORMAL;
            consumer.accept(state, ' ', i);
            if (c == '\r' && next == '\n') {
              i++; // Treat \r\n as a single line ending
            }
          }
          // else: skip character (part of the comment)
          break;

        case IN_BLOCK_COMMENT:
          if (c == '/' && next == '*') {
            blockCommentDepth++;
            i++; // skip '*'
          } else if (c == '*' && next == '/') {
            blockCommentDepth--;
            i++; // skip '/'
            if (blockCommentDepth == 0) {
              state = State.NORMAL;
              consumer.accept(state, ' ', i);
            }
          }
          // else: skip character (part of the comment)
          break;
      }
    }
  }

  private static boolean hasRawStringPrefix(String sql, int quoteIndex) {
    if (quoteIndex == 0) {
      return false;
    }
    char prefix = sql.charAt(quoteIndex - 1);
    if (prefix != 'r' && prefix != 'R') {
      return false;
    }
    return quoteIndex == 1 || !Character.isJavaIdentifierPart(sql.charAt(quoteIndex - 2));
  }

  /**
   * Finds the source-string indices of placeholder ('?') characters that appear in {@link
   * State#NORMAL} state — i.e. not inside comments, string literals, or quoted identifiers. Used by
   * parameter binding to locate the real placeholders in a SQL statement.
   *
   * @param sql the SQL string to scan
   * @return source-string indices of placeholder '?' characters, in order
   */
  public static int[] findPlaceholderPositions(String sql) {
    return findPlaceholderPositions(sql, false);
  }

  /** Finds APC placeholders, accounting for escaped quotes and raw string literals. */
  public static int[] findApcPlaceholderPositions(String sql) {
    return findPlaceholderPositions(sql, true);
  }

  private static int[] findPlaceholderPositions(String sql, boolean apc) {
    List<Integer> positions = new ArrayList<>();
    forEachNonCommentChar(
        sql,
        (state, c, sourceIndex) -> {
          if (state == State.NORMAL && c == '?') {
            positions.add(sourceIndex);
          }
        },
        apc);
    int[] out = new int[positions.size()];
    for (int k = 0; k < out.length; k++) {
      out[k] = positions.get(k);
    }
    return out;
  }

  private static boolean isRawLiteralPrefix(String sql, int quoteIndex) {
    int prefixIndex = quoteIndex - 1;
    if (prefixIndex < 0 || (sql.charAt(prefixIndex) != 'r' && sql.charAt(prefixIndex) != 'R')) {
      return false;
    }
    return prefixIndex == 0
        || (!Character.isLetterOrDigit(sql.charAt(prefixIndex - 1))
            && sql.charAt(prefixIndex - 1) != '_');
  }

  /**
   * Removes all comments and extra whitespace from the SQL string.
   *
   * @param sql the SQL string to remove comments and extra whitespace from
   * @return the SQL string with all comments and extra whitespace removed
   */
  public static String stripCommentsAndWhitespaces(String sql) {
    return stripCommentsAndWhitespaces(sql, false);
  }

  /** Removes comments and extra whitespace using APC's escaped-quote and raw-literal rules. */
  public static String stripApcCommentsAndWhitespaces(String sql) {
    return stripCommentsAndWhitespaces(sql, true);
  }

  private static String stripCommentsAndWhitespaces(String sql, boolean apc) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }

    StringBuilder result = new StringBuilder(sql.length());
    boolean[] lastWasSpace = {false};
    forEachNonCommentChar(
        sql,
        (state, c, sourceIndex) -> {
          boolean isNormalWhitespace = state == State.NORMAL && Character.isWhitespace(c);
          if (isNormalWhitespace && lastWasSpace[0]) {
            return;
          }
          lastWasSpace[0] = isNormalWhitespace;
          result.append(c);
        },
        apc);
    return result.toString().trim();
  }
}
