package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.core.UserAgent;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class UserAgentHelperTest {

  @Mock private IDatabricksConnectionContext connectionContext;

  private MockedStatic<TelemetryHelper> telemetryHelperMock;
  private MockedStatic<UserAgent> userAgentMock;

  @BeforeEach
  public void setup() {
    telemetryHelperMock = Mockito.mockStatic(TelemetryHelper.class);
    userAgentMock = Mockito.mockStatic(UserAgent.class);
  }

  @AfterEach
  public void tearDown() {
    telemetryHelperMock.close();
    userAgentMock.close();
  }

  private static Stream<Arguments> provideApplicationNameTestCases() {
    return Stream.of(
        // Test case 1: UserAgentEntry takes precedence
        Arguments.of(
            "MyUserAgent",
            "AppNameValue",
            "ClientInfoApp",
            "MyUserAgent",
            "When useragententry is set"),
        // Test case 2: ApplicationName is used when UserAgentEntry is null
        Arguments.of(
            null,
            "AppNameValue",
            "ClientInfoApp",
            "AppNameValue",
            "When useragententry is not set but applicationname is"),
        // Test case 3: ClientInfo is used when both UserAgentEntry and ApplicationName are null
        Arguments.of(
            null,
            null, // applicationName
            "ClientInfoApp",
            "ClientInfoApp",
            "When URL params are not set but client info is provided"));
  }

  @ParameterizedTest(name = "{4}")
  @MethodSource("provideApplicationNameTestCases")
  public void testDetermineApplicationName(
      String customerUserAgent,
      String applicationName,
      String clientInfoApp,
      String expectedResult,
      String testDescription) {
    // Setup only necessary stubs
    Mockito.lenient().when(connectionContext.getCustomerUserAgent()).thenReturn(customerUserAgent);
    Mockito.lenient().when(connectionContext.getApplicationName()).thenReturn(applicationName);

    // Execute
    String result = UserAgentHelper.determineApplicationName(connectionContext, clientInfoApp);

    // Verify
    assertEquals(expectedResult, result);
  }

  @Test
  public void testDetermineApplicationName_WithSystemProperty() {
    // When falling back to system property
    when(connectionContext.getCustomerUserAgent()).thenReturn(null);
    when(connectionContext.getApplicationName()).thenReturn(null);

    System.setProperty("app.name", "SystemPropApp");
    try {
      String result = UserAgentHelper.determineApplicationName(connectionContext, null);
      assertEquals("SystemPropApp", result);
    } finally {
      System.clearProperty("app.name");
    }
  }

  @Test
  public void testUpdateUserAgentAndTelemetry() {
    // Test that both telemetry and user agent are updated
    when(connectionContext.getCustomerUserAgent()).thenReturn("TestApp");
    when(UserAgent.sanitize("version")).thenReturn("version");

    UserAgentHelper.updateUserAgentAndTelemetry(connectionContext, null);

    telemetryHelperMock.verify(() -> TelemetryHelper.updateClientAppName("TestApp"));
    userAgentMock.verify(() -> UserAgent.withOtherInfo("TestApp", "version"));
  }

  @Test
  public void testUpdateUserAgentAndTelemetry_WithVersion() {
    // Test with app name containing version
    when(connectionContext.getCustomerUserAgent()).thenReturn("MyApp/1.2.3");
    when(UserAgent.sanitize("1.2.3")).thenReturn("1.2.3");

    UserAgentHelper.updateUserAgentAndTelemetry(connectionContext, null);

    telemetryHelperMock.verify(() -> TelemetryHelper.updateClientAppName("MyApp/1.2.3"));
    userAgentMock.verify(() -> UserAgent.withOtherInfo("MyApp", "1.2.3"));
  }
}
