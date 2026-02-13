# CLAUDE.md — Databricks JDBC Driver

## Build & Test

- **Build**: `mvn clean install -DskipTests`
- **Run tests**: `mvn test`
- **Run a single test**: `mvn test -pl . -Dtest=ClassName#methodName`
- **Format code**: `mvn spotless:apply` (also runs automatically during build/test)
- **JDK**: Java 11+

## Project Structure

- `src/main/java/com/databricks/jdbc/` — main driver source
- `src/test/java/com/databricks/jdbc/` — unit and integration tests
- `development/.release-freeze.json` — controls release freeze state

## PR Checks — Common Pitfalls

### 1. DCO Sign-off (required)

Every commit must include a `Signed-off-by` line. Always use the `-s` flag:

```bash
git commit -s -m "Your commit message"
```

### 2. NEXT_CHANGELOG.md (required unless opted out)

The `check-next-changelog` CI check verifies that either:

- `NEXT_CHANGELOG.md` is modified in the PR, **or**
- The PR description body contains `NO_CHANGELOG=true`

For internal refactors, test-only changes, or fixes that don't need a changelog entry, add `NO_CHANGELOG=true` to the PR description.

### 3. Release Freeze

When a release freeze is active (`development/.release-freeze.json` has `"freeze": true`), PRs are blocked from merging. To override, add `OVERRIDE_FREEZE=true` to the PR description. Use this only when the merge is necessary during the freeze window.

### 4. GitHub Account (non-EMU)

Users may have multiple GitHub accounts (e.g., an EMU/enterprise account and a personal account). Pushing and creating PRs on this repo requires the **non-EMU** account. Check the active account with `gh auth status` and switch if needed with `gh auth switch --user <username>`.

## Coding Conventions

- Follow the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)
