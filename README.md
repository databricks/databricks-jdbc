# databricks-jdbc
Repository for Java connector for Databricks

**Status**: In Development

The Databricks JDBC driver implements the JDBC interface providing connectivity to a Databricks SQL warehouse

## Getting started
You can install Databricks JDBC driver by adding the following to your `pom.xml`:

```pom.xml
<dependency>
  <groupId>com.databricks</groupId>
  <artifactId>databricks-jdbc</artifactId>
  <version>0.9.9-oss</version>
</dependency>
```
Databricks JDBC is compatible with Java 11 and higher. CI testing runs on Java versions 11, 17, and 21.
## Instructions for building
From development or main branch, run `mvn clean package`

The jar file is generated as target/databricks-jdbc-oss-jar-with-dependencies.jar

## Authentication
The JDBC driver supports following modes for authentication:

1. Personal Access Tokens: Set AuthMech=3 in connection string to use Personal Access Tokens, which can be set using PWD property.
2. OAuth2: Set AuthMech=11 for using OAuth2. We only support Azure and AWS as cloud providers for OAuth2.
   - Access Token: Set Auth_Flow=0 for providing passthrough access token using PWD property.
   - Client Credentials: Set Auth_Flow=1 for using Machine-to-machine OAuth flow.
   - Browser based OAuth: Set Auth_Flow=2 for using User-to-machine OAuth flow.

## Integration Tests
The project includes a suite of integration tests located in the
`src/test/java/com/databricks/jdbc/integration/fakeservice/tests`. Each test runs against a set of fake-services
corresponding to each production service, namely `SQL_EXEC`/`SQL_GATEWAY` and `DBFS`. The [fake-service](./src/test/java/com/databricks/jdbc/integration/fakeservice/FakeServiceExtension.java)
is based on the open-source project [WireMock](https://wiremock.org/). The tests can be run in the following
fake-service modes controlled by the environment variable <u>`FAKE_SERVICE_MODE`</u>:

1. `RECORD`: In this mode, the fake-service will record the responses from the production service and save them to the
   corresponding directory in `/src/test/resources/`. This mode is useful for updating the responses when contract with
   the production service changes.
2. `REPLAY` (default): In this mode, the fake-service will replay the recorded responses saved in the corresponding
   directory in `/src/test/resources/`. This mode is useful for running the tests without connecting to the production
   service.
3. `DRY`: In this mode, the tests will run against the production service and the fake-service will simply act as a
   pass-through proxy, meaning it neither records nor replays the responses. This mode is useful for debugging and
   authoring the tests.

### Running Integration Tests
The driver supports both SQL-Execution (default) and Thrift clients. Integration tests can be executed using either the
SQL-Execution or Thrift client, determined by setting the environment variable <u>`USE_THRIFT_CLIENT`</u> to `true` or
`false`. By default, tests run using the SQL-Execution client. Depending on the environment, either the `SQL_EXEC` or
`SQL_GATEWAY` (Thrift) fake-service is used, and test properties such as `HTTP_PATH`, `DATABRICKS_HOST`, `CATALOG`,
`SCHEMA`, etc., are loaded accordingly.

Running [connection](./src/test/java/com/databricks/jdbc/integration/fakeservice/tests/ConnectionIntegrationTests.java)
tests in `REPLAY` mode using `SQL_GATEWAY`:
```
USE_THRIFT_CLIENT=true FAKE_SERVICE_TEST_MODE=replay mvn -Dtest=com.databricks.jdbc.integration.fakeservice.tests.ConnectionIntegrationTests test
```

Running all tests in `REPLAY` mode using `SQL_EXEC`:
```
USE_THRIFT_CLIENT=false FAKE_SERVICE_TEST_MODE=replay mvn -Dtest=*IntegrationTests test
```

To run tests in either `RECORD` or `DRY` mode, set a personal access token in the <u>`DATABRICKS_TOKEN`</u> environment
variable.

Running [execution](./src/test/java/com/databricks/jdbc/integration/fakeservice/tests/ExecutionIntegrationTests.java)
tests in `RECORD` mode using `SQL_EXEC`:
```
DATABRICKS_TOKEN=<personal-access-token> USE_THRIFT_CLIENT=false FAKE_SERVICE_TEST_MODE=record mvn -Dtest=com.databricks.jdbc.integration.fakeservice.tests.ExecutionIntegrationTests test
```
This will replace the recorded responses with the new responses from the production services.

## Logging

The driver supports both [SLF4J](https://www.slf4j.org/) and [JUL](https://docs.oracle.com/javase/8/docs/api/java/util/logging/package-summary.html) logging frameworks.

- __SLF4J__: SLF4J logging can be enabled by setting the system property `-Dcom.databricks.jdbc.loggerImpl=SLF4JLOGGER`.
  Customers need to provide the SLF4J binding implementation and corresponding configuration file in the classpath.
  The intention is to give freedom to customers to adapt the JDBC logging as per their needs.
  Example of using SLF4J with Log4j2; dependencies and configuration in `pom.xml` and `log4j2.xml` respectively:

  ```
  <dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-slf4j2-impl</artifactId>
    <version>${log4j.version}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-core</artifactId>
    <version>${log4j.version}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-api</artifactId>
    <version>${log4j.version}</version>
  </dependency>
  ```

  ```
   <?xml version="1.0" encoding="UTF-8"?>
   <Configuration status="WARN">
       <Appenders>
           <!-- Console appender for default logging -->
           <Console name="Console" target="SYSTEM_OUT">
               <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n"/>
           </Console>
       </Appenders>
   
       <Loggers>
           <!-- Root logger to catch any logs that don't match other loggers -->
           <Root level="info">
               <AppenderRef ref="Console"/>
           </Root>
       </Loggers>
   </Configuration>
  ```

- __Java Util Logging (JUL)__: JUL logging can be enabled by setting the system property
  `-Dcom.databricks.jdbc.loggerImpl=JDKLOGGER`. By default, JDBC driver uses the JUL logging framework. The intention is
  to provide an out-of-the-box logging implementation without dependencies external to the JDK. There are two ways to
  configure JUL logging in the JDBC driver:
  - __JDBC URL__: Standard logging parameters namely, `logLevel`, `logPath`, `logFileSize` (MB), and `logFileCount`can
    be passed in the JDBC URL. Example:

    ```
    jdbc:databricks://your-databricks-host:443;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/your-warehouse-id;UID=token;logLevel=DEBUG;logPath=/path/to/dir;logFileSize=10;logFileCount=5
    ```

  - __Configuration File__: The logging properties can also be set in a `logging.properties` file. The file should be
    present in the classpath. Example:

    ```
    handlers=java.util.logging.FileHandler, java.util.logging.ConsoleHandler
    .level=INFO
    java.util.logging.FileHandler.level=ALL
    java.util.logging.FileHandler.pattern=/path/to/dir/databricks-jdbc.log
    java.util.logging.FileHandler.limit=10000000
    java.util.logging.FileHandler.count=5
    java.util.logging.FileHandler.formatter=java.util.logging.SimpleFormatter
    java.util.logging.ConsoleHandler.level=ALL
    java.util.logging.ConsoleHandler.formatter=java.util.logging.SimpleFormatter
    ```
