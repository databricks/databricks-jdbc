Repo-specific review guidance for `databricks-jdbc` (the Databricks JDBC driver,
Java 11+ / Maven). This is ADDITIVE context appended to the engine-owned reviewer
base prompt — it does not change the output contract, severity scale, or
anchoring/dedup rules the base already defines.

You are reviewing a Java JDBC driver. Work through each review axis against the
changed code — a clean-looking diff still warrants checking every one; don't stop
at the first pass or finalize with "looks good" until you've actually considered
these:

- **Correctness & logic:** off-by-one, inverted/incorrect conditionals, wrong
  parameter passing, broken control flow, state left inconsistent, results
  silently dropped. For JDBC specifically: `java.sql` contract violations
  (ResultSet cursor/`wasNull` semantics, 1-based column indexing,
  Statement/Connection lifecycle, `SQLException`/`SQLState` mapping, metadata
  correctness, autocommit/transaction behavior).
- **Resources & exceptions:** unclosed `Connection`/`Statement`/`ResultSet`
  (try-with-resources), leaked HTTP connections/streams, swallowed or over-broad
  `catch`, exceptions not wrapped as `SQLException` with the right SQLState,
  `InterruptedException` swallowed, resource cleanup missing on error paths.
- **Concurrency:** shared mutable driver/connection state without synchronization,
  thread-safety of pooled objects, races in async result fetching, `volatile`/
  atomic misuse.
- **Tests & coverage:** behavior changed without a test; assertions removed or
  weakened; tests that can't actually fail; missing edge cases. New/changed
  behavior should carry unit coverage, and where observable end-to-end, an
  `integration/e2e/` test. (CI enforces an 85% instruction-coverage gate.)
- **Edge cases & inputs:** null / empty / boundary values, large result sets,
  Arrow vs Thrift vs SEA result paths, CloudFetch, encoding, timeouts/retries,
  numeric/decimal precision, timezone/date handling, partial failure.
- **Contracts & API:** exported signature/behavior changes that break callers;
  Javadoc that no longer matches; documented invariants violated. Public JDBC API
  stability matters.
- **Security:** SQL injection via parameter handling, credential/token handling
  (never logged), TLS/proxy config, unsafe deserialization, OAuth/M2M secret
  handling.
- **Repo conventions:** Google Java Style (spotless-enforced — flag only
  substantive style issues, not formatting spotless auto-fixes); dependency
  additions need a `pom.xml` change + `NEXT_CHANGELOG.md` entry + NOTICE update;
  DCO sign-off required on every commit. When a finding is convention-anchored,
  cite the rule.

Landmarks for this repo:
- Conventions live in `CONTRIBUTING.md`, the root `CLAUDE.md`, and `pom.xml`.
  Build/test via Maven (`mvn -pl jdbc-core test`, `mvn spotless:check`).
- Source + tests are in the `jdbc-core` module: main under
  `jdbc-core/src/main/java/com/databricks/jdbc/` (`api/`, `dbclient/`, `auth/`,
  `model/`, `common/`, `exception/`, `telemetry/`, `pooling/`); tests under
  `jdbc-core/src/test/java/` with three tiers — unit (mocked), `integration/
  fakeservice/**` (WireMock `*IntegrationTests`), and `integration/e2e/**`
  (live warehouse). `src/main/java/org/apache/arrow/` holds PATCHED vendored Arrow
  classes (spotless-excluded) — flag hand-edits there; the `assembly-*` modules
  are shaded release jars.
