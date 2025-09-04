package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.FAKE_SERVICE_URI_PROP_SUFFIX;

import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.Closeable;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.client5.http.HttpRoute;
import org.apache.hc.client5.http.impl.DefaultSchemePortResolver;
import org.apache.hc.client5.http.impl.IdleConnectionEvictor;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.TimeValue;

/**
 * A singleton manager for an asynchronous HTTP client using Apache HttpAsyncClient. This class
 * implements a reference-counting mechanism to manage the lifecycle of the shared HTTP client. The
 * client is automatically initialized on first use and shut down when no references remain.
 */
public class GlobalAsyncHttpClient {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(GlobalAsyncHttpClient.class);
  private static volatile GlobalAsyncHttpClientHolder instance;
  private static final Object LOCK = new Object();

  private GlobalAsyncHttpClient() {
    // Private constructor to prevent instantiation
  }

  /**
   * Gets the shared HTTP client instance, creating it if necessary. Increments the reference count
   * for the client.
   *
   * @return A shared {@link CloseableHttpAsyncClient} instance
   */
  public static CloseableHttpAsyncClient getClient() {
    GlobalAsyncHttpClientHolder holder = instance;
    if (holder == null) {
      synchronized (LOCK) {
        holder = instance;
        if (holder == null) {
          holder = new GlobalAsyncHttpClientHolder();
          instance = holder;
        }
      }
    }
    holder.incrementReference();
    return holder.getClient();
  }

  /**
   * Decrements the reference count for the shared HTTP client. When the reference count reaches
   * zero, the client is automatically shut down.
   */
  public static void releaseClient() {
    GlobalAsyncHttpClientHolder holder = instance;
    if (holder != null) {
      holder.decrementReference();
    }
  }

  private static class GlobalAsyncHttpClientHolder implements Closeable {
    static final int MAX_TOTAL_CONNECTIONS = 2500;
    static final int MAX_CONNECTIONS_PER_ROUTE = 1000;
    static final int EVICTION_CHECK_INTERVAL_SECONDS = 60;
    static final int IDLE_CONNECTION_TIMEOUT_SECONDS = 180;
    static final int IO_THREADS_COUNT = Math.max(Runtime.getRuntime().availableProcessors() * 2, 8);

    private final CloseableHttpAsyncClient client;
    private final PoolingAsyncClientConnectionManager connectionManager;
    private final IdleConnectionEvictor connectionEvictor;
    private final AtomicInteger referenceCount;

    GlobalAsyncHttpClientHolder() {
      LOGGER.info("Initializing global async HTTP client");
      referenceCount = new AtomicInteger(0);

      IOReactorConfig ioReactorConfig =
          IOReactorConfig.custom().setIoThreadCount(IO_THREADS_COUNT).build();

      connectionManager = new PoolingAsyncClientConnectionManager();
      connectionManager.setMaxTotal(MAX_TOTAL_CONNECTIONS);
      connectionManager.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_ROUTE);

      HttpAsyncClientBuilder builder =
          HttpAsyncClients.custom()
              .setIOReactorConfig(ioReactorConfig)
              .setConnectionManager(connectionManager);
      if (DriverUtil.isRunningAgainstFake()) {
        setTestingRouteInAsyncHttpClient(builder);
      }
      client = builder.build();
      client.start();

      connectionEvictor =
          new IdleConnectionEvictor(
              connectionManager,
              TimeValue.of(EVICTION_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS),
              TimeValue.of(IDLE_CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      connectionEvictor.start();
    }

    CloseableHttpAsyncClient getClient() {
      return client;
    }

    void incrementReference() {
      referenceCount.incrementAndGet();
    }

    void decrementReference() {
      if (referenceCount.decrementAndGet() == 0) {
        synchronized (LOCK) {
          if (referenceCount.get() == 0) {
            close();
            instance = null;
            LOGGER.info("Global async HTTP client has been shut down");
          }
        }
      }
    }

    @Override
    public void close() {
      LOGGER.info("Closing global async HTTP client");
      connectionEvictor.shutdown();
      client.close(CloseMode.GRACEFUL);
      connectionManager.close();
    }

    /**
     * Configures a custom route planner in the {@link HttpAsyncClientBuilder} to handle routing
     * between actual services and their fake service counterparts during testing.
     *
     * <p>The route planner implements the following logic:
     *
     * <ul>
     *   <li>For localhost requests, direct routing is used without any proxy
     *   <li>For other hosts, looks up a fake service URI from system properties using the pattern
     *       {@code [target-uri] + FAKE_SERVICE_URI_PROP_SUFFIX} and sets it as a proxy
     * </ul>
     *
     * @param builder The {@link HttpAsyncClientBuilder} to be configured with the custom route
     *     planner
     */
    void setTestingRouteInAsyncHttpClient(HttpAsyncClientBuilder builder) {
      builder.setRoutePlanner(
          (host, context) -> {
            final HttpHost target;
            target =
                new HttpHost(
                    host.getSchemeName(),
                    host.getHostName(),
                    DefaultSchemePortResolver.INSTANCE.resolve(host));

            // If the target host is localhost, then no need to set proxy
            if ("localhost".equalsIgnoreCase(host.getHostName())) {
              return new HttpRoute(target, null, false);
            }

            // Get the fake service URI for the target URI and set it as proxy
            final HttpHost proxy;
            try {
              proxy =
                  HttpHost.create(System.getProperty(host.toURI() + FAKE_SERVICE_URI_PROP_SUFFIX));
            } catch (URISyntaxException e) {
              throw new HttpException(e.getMessage());
            }

            return new HttpRoute(target, null, proxy, false);
          });
    }
  }
}
