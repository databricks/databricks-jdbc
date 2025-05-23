package com.databricks.jdbc.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.core.UserAgent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

  @Test
  public void testDetermineApplicationName_WithUserAgentEntry() {
    // When useragententry is set
    when(connectionContext.getCustomerUserAgent()).thenReturn("MyUserAgent");

    String result =
        UserAgentHelper.determineApplicationName(connectionContext, "AlsoShouldNotUseThis");

    assertEquals("MyUserAgent", result);
  }

  @Test
  public void testDetermineApplicationName_WithApplicationName() {
    // When useragententry is not set but applicationname is
    when(connectionContext.getCustomerUserAgent()).thenReturn(null);
    when(connectionContext.getApplicationName()).thenReturn("AppNameValue");

    String result = UserAgentHelper.determineApplicationName(connectionContext, "ShouldNotUseThis");

    assertEquals("AppNameValue", result);
  }

  @Test
  public void testDetermineApplicationName_WithClientInfo() {
    // When URL params are not set but client info is provided
    when(connectionContext.getCustomerUserAgent()).thenReturn(null);
    when(connectionContext.getApplicationName()).thenReturn(null);

    String result = UserAgentHelper.determineApplicationName(connectionContext, "ClientInfoApp");

    assertEquals("ClientInfoApp", result);
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
