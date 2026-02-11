package com.databricks.jdbc.common.util;

/**
 * Utility class for parsing SQL strings with awareness of comments and string literals. Provides a
 * state-machine iterator ({@link #forEach}) for tasks such as comment stripping and parameter
 * counting.
 */
public class SqlCommentParser {

  public enum State {
    NORMAL,
    IN_SINGLE_QUOTE,
    IN_DOUBLE_QUOTE,
    IN_LINE_COMMENT,
    IN_BLOCK_COMMENT
  }

  @FunctionalInterface
  public interface SqlCharConsumer {
    void accept(State state, char c);
  }

  /**
   * Iterates over each character in the SQL string using a state machine that tracks comments and
   * string literals. For each "visible" character (not part of a comment body or comment
   * delimiter), calls the consumer with the current parsing state and the character.
   *
   * <p>Handles:
   *
   * <ul>
   *   <li>Single-line comments ({@code --} to end of line)
   *   <li>Block comments, including nested and multiline block comments
   *   <li>Single-quoted string literals with escaped quote sequences
   *   <li>Double-quoted identifiers with escaped quote sequences
   * </ul>
   *
   * <p>When a comment ends, a synthetic space character is emitted in {@link State#NORMAL} state to
   * prevent token fusion.
   *
   * @param sql the SQL string to parse
   * @param consumer called for each visible character with its parsing state
   */
  public static void forEach(String sql, SqlCharConsumer consumer) {
    if (sql == null || sql.isEmpty()) {
      return;
    }

    State state = State.NORMAL;
    int blockCommentDepth = 0;

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
            state = State.IN_SINGLE_QUOTE;
            consumer.accept(State.IN_SINGLE_QUOTE, c);
          } else if (c == '"') {
            state = State.IN_DOUBLE_QUOTE;
            consumer.accept(State.IN_DOUBLE_QUOTE, c);
          } else {
            consumer.accept(State.NORMAL, c);
          }
          break;

        case IN_SINGLE_QUOTE:
          consumer.accept(State.IN_SINGLE_QUOTE, c);
          if (c == '\'' && next == '\'') {
            consumer.accept(State.IN_SINGLE_QUOTE, next);
            i++; // skip escaped quote
          } else if (c == '\'') {
            state = State.NORMAL;
          }
          break;

        case IN_DOUBLE_QUOTE:
          consumer.accept(State.IN_DOUBLE_QUOTE, c);
          if (c == '"' && next == '"') {
            consumer.accept(State.IN_DOUBLE_QUOTE, next);
            i++; // skip escaped quote
          } else if (c == '"') {
            state = State.NORMAL;
          }
          break;

        case IN_LINE_COMMENT:
          if (c == '\r' && next == '\n') {
            // Treat \r\n as a single line ending
            state = State.NORMAL;
            consumer.accept(State.NORMAL, ' ');
            i++; // skip '\n'
          } else if (c == '\n' || c == '\r') {
            state = State.NORMAL;
            consumer.accept(State.NORMAL, ' ');
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
              consumer.accept(State.NORMAL, ' ');
            }
          }
          // else: skip character (part of the comment)
          break;
      }
    }
  }

  /**
   * Removes all SQL comments and extra whitespace from the input string.
   *
   * @param sql the SQL string to strip comments and extra whitespace from
   * @return the SQL with all comments and extra whitespace removed
   */
  public static String stripCommentsAndWhitespaces(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }

    StringBuilder result = new StringBuilder(sql.length());
    forEach(sql, (state, c) -> result.append(c));
    return result.toString().replaceAll("\\s+", " ").trim();
  }
}
