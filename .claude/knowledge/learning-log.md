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

### 2026-08-18: learnings since 2026-08-17T17:33:39Z
- **Context:** PR #1582 wired `merge_group` triggers into required-check workflows (checkNextChangelog, releaseFreeze, prCheck) for a GitHub merge queue.
  **Rule:** For a required status check to stay valid under a GitHub merge queue, register the workflow on the `merge_group` event; PR-semantic jobs that read `github.event.pull_request.*` should self-skip in the queue with `if: github.event_name != 'merge_group'` — a skipped required check counts as a pass under branch protection, so the queue is not blocked while the gate stays enforced at PR time.
- **Context:** In PR #1582 the engineer-bot re-posted the same "NEEDS HUMAN DECISION / blocked" verdict on successive replies of one review thread because the reviewer's finding required editing `.github/workflows/*.yml`, a non-writable path in this environment.
  **Rule:** When a review finding targets a path the environment cannot write (e.g. `.github/workflows/*`), state once that it is agreed-but-not-actionable-here and stop; do not re-analyze or re-post a blocked verdict on each subsequent thread reply.
