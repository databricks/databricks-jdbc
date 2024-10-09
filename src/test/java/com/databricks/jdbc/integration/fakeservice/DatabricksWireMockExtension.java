package com.databricks.jdbc.integration.fakeservice;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.JvmProxyConfigurer;
import com.github.tomakehurst.wiremock.junit.DslWrapper;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.util.Optional;
import org.junit.jupiter.api.extension.*;
import org.junit.platform.commons.support.AnnotationSupport;

/**
 * A JUnit 5 extension that manages the lifecycle of a {@link WireMockServer}.
 *
 * <p>This is inspired from {@link com.github.tomakehurst.wiremock.junit5.WireMockExtension} with
 * changes to pass {@link ExtensionContext} to lifecycle hooks ({@link #onBeforeEach}, {@link
 * #onAfterEach}, {@link #onBeforeAll}, {@link #onAfterAll}).
 *
 * <p>TODO: Switch to OSS version once the <a
 * href="https://github.com/wiremock/wiremock/pull/1981">PR</a> is merged
 */
public class DatabricksWireMockExtension extends DslWrapper
    implements ParameterResolver,
        BeforeAllCallback,
        AfterAllCallback,
        BeforeEachCallback,
        AfterEachCallback {

  private final boolean configureStaticDsl;

  private final boolean failOnUnmatchedRequests;

  private final boolean isDeclarative;

  private Options options;

  private WireMockServer wireMockServer;

  private WireMockRuntimeInfo runtimeInfo;

  private boolean isNonStatic = false;

  private Boolean proxyMode;

  protected DatabricksWireMockExtension(DatabricksWireMockExtension.Builder builder) {
    this.options = builder.options;
    this.configureStaticDsl = builder.configureStaticDsl;
    this.failOnUnmatchedRequests = builder.failOnUnmatchedRequests;
    this.proxyMode = builder.proxyMode;
    this.isDeclarative = false;
  }

  /**
   * To be overridden in subclasses in order to run code immediately after per-class WireMock setup.
   */
  protected void onBeforeAll(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws Exception {}

  /**
   * To be overridden in subclasses in order to run code immediately after per-test WireMock setup.
   */
  protected void onBeforeEach(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws Exception {}

  /**
   * To be overridden in subclasses in order to run code immediately after per-test cleanup of
   * WireMock and its associated resources.
   */
  protected void onAfterEach(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws Exception {}

  /**
   * To be overridden in subclasses in order to run code immediately after per-class cleanup of
   * WireMock.
   */
  protected void onAfterAll(WireMockRuntimeInfo wireMockRuntimeInfo, ExtensionContext context)
      throws Exception {}

  /** {@inheritDoc} */
  @Override
  public boolean supportsParameter(
      final ParameterContext parameterContext, final ExtensionContext extensionContext)
      throws ParameterResolutionException {
    return parameterIsWireMockRuntimeInfo(parameterContext);
  }

  /** {@inheritDoc} */
  @Override
  public Object resolveParameter(
      final ParameterContext parameterContext, final ExtensionContext extensionContext)
      throws ParameterResolutionException {

    if (parameterIsWireMockRuntimeInfo(parameterContext)) {
      return runtimeInfo;
    }

    return null;
  }

  private void startServerIfRequired(ExtensionContext extensionContext) {
    if (wireMockServer == null || !wireMockServer.isRunning()) {
      wireMockServer = new WireMockServer(resolveOptions(extensionContext));
      wireMockServer.start();

      runtimeInfo = new WireMockRuntimeInfo(wireMockServer);

      this.admin = wireMockServer;
      this.stubbing = wireMockServer;

      if (configureStaticDsl) {
        WireMock.configureFor(new WireMock(this));
      }
    }
  }

  private void setAdditionalOptions(ExtensionContext extensionContext) {
    if (proxyMode == null) {
      proxyMode =
          extensionContext
              .getElement()
              .flatMap(
                  annotatedElement ->
                      AnnotationSupport.findAnnotation(annotatedElement, WireMockTest.class))
              .map(WireMockTest::proxyMode)
              .orElse(false);
    }
  }

  private Options resolveOptions(ExtensionContext extensionContext) {
    final Options defaultOptions = WireMockConfiguration.options().dynamicPort();
    return extensionContext
        .getElement()
        .flatMap(
            annotatedElement ->
                this.isDeclarative
                    ? AnnotationSupport.findAnnotation(annotatedElement, WireMockTest.class)
                    : Optional.empty())
        .map(this::buildOptionsFromWireMockTestAnnotation)
        .orElseGet(() -> Optional.ofNullable(this.options).orElse(defaultOptions));
  }

  private Options buildOptionsFromWireMockTestAnnotation(WireMockTest annotation) {
    WireMockConfiguration options =
        WireMockConfiguration.options()
            .port(annotation.httpPort())
            .enableBrowserProxying(annotation.proxyMode());

    if (annotation.httpsEnabled()) {
      options.httpsPort(annotation.httpsPort());
    }

    return options;
  }

  private void stopServerIfRunning() {
    if (wireMockServer.isRunning()) {
      wireMockServer.stop();
    }
  }

  private boolean parameterIsWireMockRuntimeInfo(ParameterContext parameterContext) {
    return parameterContext.getParameter().getType().equals(WireMockRuntimeInfo.class)
        && this.isDeclarative;
  }

  /** {@inheritDoc} */
  @Override
  public final void beforeAll(ExtensionContext context) throws Exception {
    startServerIfRequired(context);
    setAdditionalOptions(context);

    onBeforeAll(runtimeInfo, context);
  }

  /** {@inheritDoc} */
  @Override
  public final void beforeEach(ExtensionContext context) throws Exception {
    if (wireMockServer == null) {
      isNonStatic = true;
      startServerIfRequired(context);
    } else {
      resetToDefaultMappings();
    }

    setAdditionalOptions(context);

    if (proxyMode) {
      JvmProxyConfigurer.configureFor(wireMockServer);
    }

    onBeforeEach(runtimeInfo, context);
  }

  /** {@inheritDoc} */
  @Override
  public final void afterAll(ExtensionContext context) throws Exception {
    stopServerIfRunning();

    onAfterAll(runtimeInfo, context);
  }

  /** {@inheritDoc} */
  @Override
  public final void afterEach(ExtensionContext context) throws Exception {
    if (failOnUnmatchedRequests) {
      wireMockServer.checkForUnmatchedRequests();
    }

    if (isNonStatic) {
      stopServerIfRunning();
    }

    if (proxyMode) {
      JvmProxyConfigurer.restorePrevious();
    }

    onAfterEach(runtimeInfo, context);
  }

  public WireMockRuntimeInfo getRuntimeInfo() {
    return new WireMockRuntimeInfo(wireMockServer);
  }

  public String baseUrl() {
    return wireMockServer.baseUrl();
  }

  public String url(String path) {
    return wireMockServer.url(path);
  }

  public int getHttpsPort() {
    return wireMockServer.httpsPort();
  }

  public int getPort() {
    return wireMockServer.port();
  }

  /** Builder for {@link DatabricksWireMockExtension}. */
  public static class Builder {

    private Options options = WireMockConfiguration.wireMockConfig().dynamicPort();
    private boolean configureStaticDsl = false;
    private boolean failOnUnmatchedRequests = false;
    private boolean proxyMode = false;

    public DatabricksWireMockExtension.Builder options(Options options) {
      this.options = options;
      return this;
    }

    public DatabricksWireMockExtension.Builder configureStaticDsl(boolean configureStaticDsl) {
      this.configureStaticDsl = configureStaticDsl;
      return this;
    }

    public DatabricksWireMockExtension.Builder failOnUnmatchedRequests(boolean failOnUnmatched) {
      this.failOnUnmatchedRequests = failOnUnmatched;
      return this;
    }

    public DatabricksWireMockExtension.Builder proxyMode(boolean proxyMode) {
      this.proxyMode = proxyMode;
      return this;
    }

    public DatabricksWireMockExtension build() {
      if (proxyMode
          && !options.browserProxySettings().enabled()
          && (options instanceof WireMockConfiguration)) {
        ((WireMockConfiguration) options).enableBrowserProxying(true);
      }

      return new DatabricksWireMockExtension(this);
    }
  }
}
