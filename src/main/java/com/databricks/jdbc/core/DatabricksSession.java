package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.impl.DatabricksSdkClient;
import com.databricks.sdk.service.sql.Session;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Implementation for Session interface, which maintains an underlying session in SQL Gateway.
 */
public class DatabricksSession implements IDatabricksSession {

  private static final int LINKS_FETCHER_THREAD_POOL_SIZE = 4;
  private static final String LINKS_FETCHER_THREAD_POOL_PREFIX = "databricks-jdbc-links-fetcher-";
  private final DatabricksClient databricksClient;
  private final String warehouseId;

  // Common thread-pool for downloading external links asynchronously
  private final ExecutorService executor;

  private boolean isSessionOpen;
  private Session session;

  // For context based commands
  private String catalog;

  private String schema;

  /**
   * Creates an instance of Databricks session for given connection context
   * @param connectionContext underlying connection context
   */
  public DatabricksSession(IDatabricksConnectionContext connectionContext) {
    this.databricksClient = new DatabricksSdkClient(connectionContext);
    this.isSessionOpen = false;
    this.session = null;
    this.warehouseId = connectionContext.getWarehouse();
    this.executor = createLinksDownloaderExecutorService();
  }

  /**
   * Construct method to be used for mocking in a test case.
   */
  @VisibleForTesting
  DatabricksSession(IDatabricksConnectionContext connectionContext, DatabricksClient databricksClient) {
    this.databricksClient = databricksClient;
    this.isSessionOpen = false;
    this.session = null;
    this.warehouseId = connectionContext.getWarehouse();
    this.executor = Executors.newSingleThreadExecutor();
  }

  private static ExecutorService createLinksDownloaderExecutorService() {
    ThreadFactory threadFactory =
        new ThreadFactory() {
          private int threadCount = 1;

          public Thread newThread(final Runnable r) {
            final Thread thread = new Thread(r);
            thread.setName(LINKS_FETCHER_THREAD_POOL_PREFIX + threadCount++);
            // TODO: catch uncaught exceptions
            thread.setDaemon(true);

            return thread;
          }
        };
    return Executors.newFixedThreadPool(LINKS_FETCHER_THREAD_POOL_SIZE, threadFactory);
  }

  @Override
  @Nullable
  public String getSessionId() {
    return isSessionOpen ? session.getSessionId() : null;
  }

  @Override
  public String getWarehouseId() {
    return warehouseId;
  }

  @Override
  public boolean isOpen() {
    // TODO: check for expired sessions
    return isSessionOpen;
  }

  @Override
  public void open() {
    // TODO: check for expired sessions
    synchronized (this) {
      if (!isSessionOpen) {
        // TODO: handle errors
        this.session = databricksClient.createSession(this.warehouseId);
        this.isSessionOpen = true;
      }
    }
  }

  @Override
  public void close() {
    // TODO: check for any pending query executions
    synchronized (this) {
      if (isSessionOpen) {
        // TODO: handle closed connections by server
        databricksClient.deleteSession(this.session.getSessionId());
        this.executor.shutdown();
        this.session = null;
        this.isSessionOpen = false;
      }
    }
  }

  @Override
  public DatabricksClient getDatabricksClient() {
    return databricksClient;
  }

  @Override
  public ExecutorService getExecutorService() {
    return this.executor;
  }

  @Override
  public String getCatalog() {
    return catalog;
  }

  @Override
  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  @Override
  public String getSchema() {
    return schema;
  }

  @Override
  public void setSchema(String schema) {
    this.schema = schema;
  }
}
