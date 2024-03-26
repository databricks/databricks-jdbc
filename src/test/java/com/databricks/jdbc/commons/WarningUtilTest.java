package com.databricks.jdbc.commons;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.commons.util.WarningUtil;
import java.sql.SQLWarning;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WarningUtilTest {
  private static WarningUtil warningUtil = new WarningUtil();

  @Test
  public void testAddWarningToExistingWarning() {
    SQLWarning initialWarning = new SQLWarning("Initial warning");
    SQLWarning addedWarning = warningUtil.addWarning(initialWarning, "New warning");
    assertSame(initialWarning, addedWarning);
    assertSame(initialWarning.getNextWarning().getMessage(), "New warning");
  }

  @Test
  public void testAddWarningToNullWarning() {
    SQLWarning addedWarning = warningUtil.addWarning(null, "New warning");
    assertNotNull(addedWarning);
    assertEquals("New warning", addedWarning.getMessage());
    assertNull(addedWarning.getNextWarning());
  }
}
