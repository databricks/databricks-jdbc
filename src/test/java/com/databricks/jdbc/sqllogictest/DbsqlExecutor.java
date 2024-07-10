/*
This class holds the dbsql executor for the SQL Logic Tests.

We intend to run these tests periodically from a github workflow.
It also hits the actual Databricks staging environment given in the connection string below so we ensure that
it can be run only by providing a secret for that environment.
 */

package com.databricks.jdbc.sqllogictest;

import com.databricks.client.jdbc.Driver;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import net.hydromatic.sqllogictest.*;
import net.hydromatic.sqllogictest.executors.JdbcExecutor;

public class DbsqlExecutor extends JdbcExecutor {

  static String dbsqlUrl =
      "jdbc:databricks://e2-dogfood.staging.cloud.databricks.com:443"
          + "/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
          + "/sql/1.0/warehouses/791ba2a31c7fd70a;ConnCatalog=field_demos;"
          + "ConnSchema=ossjdbc;";

  /**
   * Create an executor that uses JDBC to run tests.
   *
   * @param options Execution options.
   */
  public DbsqlExecutor(OptionsParser.SuppliedOptions options, String token) {
    super(options, dbsqlUrl, "", token);
  }

  /**
   * Register the HSQL DB executor with the command-line options.
   *
   * @param optionsParser Options that will guide the execution.
   */
  public static void register(OptionsParser optionsParser) {
    AtomicReference<String> pat = new AtomicReference<>();
    optionsParser.registerOption(
        "-p",
        "PAT",
        "PAT for the env in the jdbc url",
        o -> {
          pat.set(o);
          return true;
        });

    optionsParser.registerExecutor(
        "dbsql",
        () -> {
          DbsqlExecutor result = new DbsqlExecutor(optionsParser.getOptions(), pat.get());
          try {
            DriverManager.registerDriver(new Driver());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
          try {
            Set<String> bugs = optionsParser.getOptions().readBugsFile();
            result.avoid(bugs);
            return result;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void establishConnection() throws SQLException {
    super.establishConnection();
    this.getConnection().setCatalog("field_demos");
    this.getConnection().setSchema("ossjdbc");
  }

  @Override
  public void dropAllTables() throws SQLException {
    List<String> tables = getTableList();
    options.out.println("Number of tables to drop: " + tables.size());
    for (String tableName : tables) {
      // Unfortunately prepare statements cannot be parameterized in
      // table names.  Sonar complains about this, but there is
      // nothing we can do but suppress the warning.
      options.out.println("Dropping table: " + tableName);
      String del = "DROP TABLE " + tableName;
      options.message(del, 2);
      try (Statement drop = this.getConnection().createStatement()) {
        drop.execute(del); // NOSONAR
      }
    }
  }

  @Override
  public void dropAllViews() throws SQLException {
    List<String> tables = getViewList();
    for (String tableName : tables) {
      // Unfortunately prepare statements cannot be parameterized in
      // table names.  Sonar complains about this, but there is
      // nothing we can do but suppress the warning.
      options.out.println("Dropping view: " + tableName);
      String del = "DROP VIEW IF EXISTS " + tableName;
      options.message(del, 2);
      try (Statement drop = this.getConnection().createStatement()) {
        drop.execute(del); // NOSONAR
      }
    }
  }

  @Override
  public boolean validate(
      SqlTestQuery query,
      ResultSet rs,
      SqlTestQueryOutputDescription description,
      TestStatistics statistics)
      throws SQLException, NoSuchAlgorithmException {
    boolean failed = super.validate(query, rs, description, statistics);
    // Ignore failures due to div sign as DBSQL does not round off to int
    return failed ? !query.getQuery().contains("/") : failed;
  }

  @Override
  public TestStatistics execute(SltTestFile file, OptionsParser.SuppliedOptions options)
      throws SQLException {
    options.out.println("Running " + file.toString());
    this.startTest();
    this.establishConnection();
    this.dropAllTables();
    TestStatistics result = new TestStatistics(options.stopAtFirstError, options.verbosity);
    result.incFiles();
    // Changed super function here to only run the first few commands from each file
    for (ISqlTestOperation operation : file.fileContents) {
      SltSqlStatement stat = operation.as(SltSqlStatement.class);
      if (stat != null) {
        try {
          options.out.println(stat.statement);
          this.statement(stat);
          if (!stat.shouldPass) {
            options.err.println("Statement should have failed: " + operation);
          }
        } catch (SQLException ex) {
          // errors in statements cannot be recovered.
          if (stat.shouldPass) {
            // shouldPass should always be true, otherwise
            // the exception should not be thrown.
            options.err.println("Error '" + ex.getMessage() + "' in SQL statement " + operation);
            result.incFilesNotParsed();
            return result;
          }
        }
      } else {
        SqlTestQuery query = operation.to(options.err, SqlTestQuery.class);
        boolean stop;
        try {
          options.out.println(query.getQuery());
          stop = this.query(query, result);
        } catch (Throwable ex) {
          // Need to catch Throwable to handle assertion failures too
          options.message("Exception during query: " + ex.getMessage(), 1);
          stop = result.addFailure(new TestStatistics.FailedTestDescription(query, null, "", ex));
        }
        if (stop) {
          break;
        }
      }
    }
    this.dropAllViews();
    this.dropAllTables();
    this.closeConnection();
    options.message(this.elapsedTime(file.getTestCount()), 1);
    return result;
  }

  /**
   * Run a query.
   *
   * @param query Query to execute.
   * @param statistics Execution statistics recording the result of the query execution.
   * @return True if we need to stop executing.
   */
  boolean query(SqlTestQuery query, TestStatistics statistics)
      throws SQLException, NoSuchAlgorithmException {
    if (this.buggyOperations.contains(query.getQuery()) || this.options.doNotExecute) {
      statistics.incIgnored();
      options.message("Skipping " + query.getQuery(), 2);
      return false;
    }
    try (Statement stmt = this.getConnection().createStatement()) {
      try (ResultSet resultSet = stmt.executeQuery(query.getQuery())) {
        boolean result = this.validate(query, resultSet, query.outputDescription, statistics);
        options.message(statistics.totalTests() + ": " + query.getQuery(), 2);
        return result;
      }
    }
  }

  List<String> getViewList() {
    List<String> result = new ArrayList<>();
    // TODO: Add implementation once getTables allows fetching VIEWS
    return result;
  }

  List<String> getTableList() throws SQLException {
    List<String> result = new ArrayList<>();
    DatabaseMetaData md = this.getConnection().getMetaData();
    ResultSet rs =
        md.getTables(
            this.getConnection().getCatalog(),
            this.getConnection().getSchema(),
            "%",
            new String[] {"TABLE"});
    while (rs.next()) {
      String tableName = rs.getString(3);
      result.add(tableName);
    }
    rs.close();
    return result;
  }
}
