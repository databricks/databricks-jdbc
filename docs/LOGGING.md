# Logging Configuration

The Databricks JDBC driver supports Java Util Logging (JUL) and SLF4J logging frameworks. The available options depend on whether you use the fat JAR or thin JAR.

## Quick Reference

| JAR Type | JUL | SLF4J (native) | SLF4J (via bridge) |
|----------|-----|----------------|-------------------|
| Fat JAR (`databricks-jdbc-X.Y.Z.jar`) | Supported | Not supported | Supported |
| Thin JAR (`databricks-jdbc-X.Y.Z-thin.jar`) | Supported | Supported | N/A |

---

## Fat JAR Logging

The fat JAR bundles all dependencies with shading. **Only JUL logging is natively supported.** The fat JAR includes an SLF4J-to-JUL bridge, so logs from internal libraries (Databricks SDK, Apache HTTP client, etc.) are automatically routed to JUL and will appear in your configured log output.

### Configuration via JDBC URL

```
jdbc:databricks://<host>:443;HttpPath=<path>;LogLevel=5;LogPath=/var/log/databricks
```

### Configuration via Properties

```java
Properties props = new Properties();
props.setProperty("LogLevel", "5");        // DEBUG
props.setProperty("LogPath", "/var/log");  // Directory for log files
props.setProperty("LogFileSize", "10");    // Max file size in MB
props.setProperty("LogFileCount", "5");    // Number of rotating files

Connection conn = DriverManager.getConnection(url, props);
```

### Log Level Values

| Value | Level |
|-------|-------|
| 0 | OFF |
| 1 | FATAL |
| 2 | ERROR |
| 3 | WARN |
| 4 | INFO |
| 5 | DEBUG |
| 6 | TRACE |

### Note on SLF4JLOGGER Mode

Setting `-Dcom.databricks.jdbc.loggerImpl=SLF4JLOGGER` with the fat JAR will **not** produce any log output. The fat JAR is designed for out-of-the-box use with BI tools and applications that cannot manage dependencies. SLF4J is bundled because the Databricks SDK requires it at runtime, and it is shaded to avoid conflicts with user environments. As a result, the shaded SLF4J does not connect to user-provided bindings.

---

## Thin JAR Logging

The thin JAR does not bundle dependencies, giving you full control over logging configuration. Both JUL and SLF4J are supported.

### Using JUL (Default)

Configure via JDBC URL parameters as shown above, or use a `logging.properties` file:

```properties
handlers=java.util.logging.FileHandler, java.util.logging.ConsoleHandler
.level=INFO
java.util.logging.FileHandler.pattern=/var/log/databricks-jdbc.log
java.util.logging.FileHandler.limit=10000000
java.util.logging.FileHandler.count=5
java.util.logging.ConsoleHandler.level=ALL
```

### Using SLF4J

Enable SLF4J logging:

```
-Dcom.databricks.jdbc.loggerImpl=SLF4JLOGGER
```

Add an SLF4J binding to your project (e.g., Logback):

```xml
<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.4.14</version>
</dependency>
```

Optionally exclude the driver's SLF4J version to use your own:

```xml
<dependency>
    <groupId>com.databricks</groupId>
    <artifactId>databricks-jdbc</artifactId>
    <version>3.0.7</version>
    <classifier>thin</classifier>
    <exclusions>
        <exclusion>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

---

## Integrating with SLF4J (Fat JAR)

Fat JAR users can integrate driver logs into their SLF4J/Logback setup using the JUL-to-SLF4J bridge.

### Step 1: Add Dependency

```xml
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>jul-to-slf4j</artifactId>
    <version>2.0.13</version>
</dependency>
```

### Step 2: Create `logging.properties`

```properties
handlers = org.slf4j.bridge.SLF4JBridgeHandler
.level = FINEST
```

### Step 3: Pass JVM Argument

```
-Djava.util.logging.config.file=/path/to/logging.properties
```

This argument is required to bypass the driver's internal JUL configuration and allow logs to propagate to the SLF4J bridge.

### Step 4: Configure Logback

```xml
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- Databricks JDBC Driver logs (includes SDK, HTTP client, etc.) -->
    <logger name="com.databricks" level="DEBUG"/>

    <root level="INFO">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

The `com.databricks` logger captures all driver logs including shaded internal libraries (Databricks SDK, Apache HTTP client, etc.).
