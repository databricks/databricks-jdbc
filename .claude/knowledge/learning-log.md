# Engineer-bot learning log

This file is the canonical, human-gated knowledge log shared by two consumers
configured in `.bot/config.yaml`:

- `retrospective.log_path` — the retrospective flow APPENDS a dated section here
  when it extracts a durable, reusable learning (via a human-gated rolling PR).
- `author.knowledge_log` — the author phase READS this file so fixes benefit from
  what has been learned. Seeded here so the read path is never a missing file.

No learnings have been recorded yet. Dated sections are appended below by the
retrospective flow.
## Entries

### 2026-08-14: learnings since 2026-08-13T17:41:38Z
- **Context:** Author run #31744986912 fixed a parameter-bind bug where `DatabricksTypeUtil.getDatabricksTypeFromSQLType` collapsed both `Types.FLOAT` and `Types.REAL` to the 4-byte Databricks `FLOAT` wire type, silently narrowing FLOAT binds.
  **Rule:** Per the JDBC spec (Appendix B type table), `Types.FLOAT` is a synonym for `DOUBLE` (8-byte double precision, Java `double`) and must map to the 8-byte wire type; only `Types.REAL` is 4-byte single precision (Java `float`) — never collapse FLOAT and REAL to the same Databricks type, and treat the declared SQL target type (not the bound value's native type) as what drives the wire type.
- **Context:** In author run #31744986912 the read-only diagnosis/plan phase spent a turn calling `edit_file` to create a test file (turn 44), only to discover editing tools are unavailable in that phase (turn 46) — the deliverable is the structured plan, not code.
  **Rule:** The diagnosis/plan phase is read-only; don't attempt `edit_file`/write operations there. Defer all file creation and edits to the author_tests/fix phases and keep the diagnosis phase to reads, greps, and the structured plan output.
