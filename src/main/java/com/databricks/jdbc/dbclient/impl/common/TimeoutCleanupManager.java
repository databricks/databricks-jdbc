package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final class TimeoutCleanupManager {
  private static final Map<IDatabricksConnectionContext, TimeoutCleanupManager> MANAGERS =
      new WeakHashMap<>();
  private final ThreadPoolExecutor workers;

  static synchronized TimeoutCleanupManager forConnection(IDatabricksConnectionContext context) {
    if (context == null) {
      return new TimeoutCleanupManager(64, 64);
    }
    return MANAGERS.computeIfAbsent(context, ignored -> new TimeoutCleanupManager(64, 64));
  }

  TimeoutCleanupManager(int concurrency, int queueCapacity) {
    workers =
        new ThreadPoolExecutor(
            concurrency,
            concurrency,
            30,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(queueCapacity),
            task -> {
              Thread thread = new Thread(task, "databricks-jdbc-timeout-cancel");
              thread.setDaemon(true);
              thread.setContextClassLoader(null);
              return thread;
            });
    workers.allowCoreThreadTimeOut(true);
  }

  void dispatch(Runnable cancel) {
    try {
      workers.execute(cancel);
    } catch (RejectedExecutionException saturated) {
      cancel.run();
    }
  }
}
