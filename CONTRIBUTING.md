# Contributing to Databricks JDBC Driver

---

- [Communication](#communication)
- [Contribution Workflow](#contribution-workflow)
- [Development Environment Setup](#development-environment-setup)
- [Debugging](#debugging)
- [Testing](#testing)
- [Coding Style](#coding-style)
- [Sign Your Work](#sign-your-work)

We happily welcome contributions to the Databricks JDBC Driver. We use [GitHub Issues](https://github.com/databricks/databricks-jdbc/issues)
to track community reported issues and [GitHub Pull Requests](https://github.com/databricks/databricks-jdbc/pulls) for
accepting changes. We are excited to work with the open source communities in the many years to come to improve the JDBC Driver.

## Communication

- Before starting work on a major feature, please reach out to us via GitHub, [E-Mail](mailto:eng-oss-sql-driver@databricks.com), etc.
  We will make sure no one else is already working on it and ask you to open a GitHub issue.
- A "major feature" is defined as any change that is > 100 lines of code altered (not including tests), or changes any
  user-facing behavior.
- We will use the GitHub issue to discuss the feature and come to agreement.
- The GitHub review process for major features is also important so that organizations with write access can come to
  agreement on design.
- If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or
  linked to from the issue and hosted in a world-readable location.
- Small patches and bug fixes don't need prior communication. If you have identified a bug and have ways to solve it,
  please create an issue or create a pull request.

## Contribution Workflow

Code contributions—bug fixes, new development, test improvement—all follow a GitHub-centered workflow. To participate in
Databricks JDBC Driver development, set up a GitHub account. Then:

- Fork the repo. Go to the project repo page and use the Fork button. This will create a copy of the repo, under your
  username. (For more details on how to fork a repository see [this guide](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo).)

- Clone down the repo to your local system.

  ```bash
  git clone git@github.com:YOUR_USER_NAME/databricks-jdbc.git
  ```

- Create a new branch to hold your work.

  ```bash
  git checkout -b new-branch-name
  ```

- Work on your new code. Write tests and run the Spotless code formatting plugin. The Spotless plugin also runs
  automatically during the build process.

  ```bash
  mvn test
  mvn spotless:apply
  ```

- Commit your changes.

  ```bash
  git add -A

  git commit -m "Commit Message"
  ```

- Push your changes to your GitHub repo.

  ```bash
  git push origin branch-name
  ```

- Open a Pull Request (PR). Go to the original project repo on GitHub. There will be a message about your recently
  pushed branch, asking if you would like to open a pull request. Follow the prompts, compare across repositories, and
  submit the PR. This will email the committers. You may want to consider sending an email to the mailing list for more
  visibility. (For more details, see the [GitHub guide on PRs](https://help.github.com/articles/creating-a-pull-request-from-a-fork).)

Maintainers and other contributors will review your PR. Please participate in the conversation, and try to make any
requested changes. Once the PR is approved, the code will be merged.

Additional git and GitHub resources:

[Git documentation](https://git-scm.com/documentation)

[Git development workflow](https://docs.scipy.org/doc/numpy/dev/development_workflow.html)

[Resolving merge conflicts](https://help.github.com/articles/resolving-a-merge-conflict-using-the-command-line/)

## Development Environment Setup

We recommend using IntelliJ to develop the Databricks JDBC Driver. You can download the community edition [here](https://www.jetbrains.com/idea/download/).
Once you have IntelliJ installed, open the Databricks JDBC Driver project by selecting `File -> Open` and selecting the
`databricks-jdbc` folder. Ensure that you have the Maven plugin loaded.

## Debugging

The Databricks JDBC Driver uses both [SLF4J](https://www.slf4j.org/) and [JUL](https://docs.oracle.com/javase/8/docs/api/java/util/logging/package-summary.html)
logging framework. By default, the driver uses the JUL logging framework. For more information on logging, refer to the
repository's [README.md](https://github.com/databricks/databricks-jdbc/blob/main/README.md).

The easiest and recommended way to enable debug logging is via JDBC URL which will automatically use the JUL logging
framework. This will send the logs to the file pattern `databricks_jdbc.log.%i` under the specified `LogPath`.

```
jdbc:databricks:<host:port>/default;transportMode=https;ssl=1;AuthMech=3;httpPath=<http path>;LogPath=/Users/spiderman;LogLevel=debug
```

Note that when running the repository tests using maven or editor, logs will be printed to the console as per the
`logging.properties` in `src/test/resources`.

## Testing

- All changes to the Databricks JDBC Driver should be covered by unit tests.
- New features should be covered by integration tests where applicable. These tests are supported by a custom JUnit
  extension built on [WireMock](https://wiremock.org/), which helps simulate production-like environments while reducing
  reliance on live systems. This approach improves CI/CD efficiency. For more details on the testing framework, refer to
  the repository's [README.md](https://github.com/databricks/databricks-jdbc/blob/main/README.md).

## Coding Style

- We generally follow this [Java Style Guide](https://google.github.io/styleguide/javaguide.html).

## Sign Your Work

The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the
patch or otherwise have the right to pass it on as an open-source patch.

```
Signed-off-by: Holly Smith <holly.smith@email.com>
Use your real name (sorry, no pseudonyms or anonymous contributions.)
```
