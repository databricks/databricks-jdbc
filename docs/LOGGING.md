# Logging Configuration

The Databricks JDBC driver supports both [SLF4J](https://www.slf4j.org/) and [JUL](https://docs.oracle.com/javase/8/docs/api/java/util/logging/package-summary.html) logging frameworks.

## SLF4J Logging

SLF4J logging can be enabled by setting the system property:
```
-Dcom.databricks.jdbc.loggerImpl=SLF4JLOGGER
```

You need to provide an SLF4J binding implementation and corresponding configuration file in the classpath. This gives you the freedom to adapt the JDBC logging to your specific needs.

### Example: Using SLF4J with Log4j2

Add the following dependencies to your `pom.xml`:

```xml
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

Create a `log4j2.xml` configuration file:

```xml
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

## Java Util Logging (JUL)

JUL logging is enabled by default, or can be explicitly set with:
```
-Dcom.databricks.jdbc.loggerImpl=JDKLOGGER
```

There are two ways to configure JUL logging:

### 1. JDBC URL Parameters

Standard logging parameters can be passed in the JDBC URL:

```
jdbc:databricks://your-databricks-host:443;transportMode=http;ssl=1;AuthMech=3;httpPath=/sql/1.0/warehouses/your-warehouse-id;UID=token;logLevel=DEBUG;logPath=/path/to/dir;logFileSize=10;logFileCount=5
```

Available parameters:
- `logLevel`: Logging level (e.g., DEBUG, INFO)
- `logPath`: Directory path for log files
- `logFileSize`: Maximum size of each log file in MB
- `logFileCount`: Maximum number of log files to keep

### 2. Configuration File

Logging properties can also be set in a `logging.properties` file in the classpath:

```properties
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
