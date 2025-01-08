package com.databricks.jdbc.api.impl.arrow.incubator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

public class DownloadPhaseTest {

  @Test
  public void testEnumValues() {
    // Verify we have exactly the expected number of enum values
    assertEquals(2, DownloadPhase.values().length);

    // Verify enum values exist and can be referenced
    assertNotNull(DownloadPhase.DATA_DOWNLOAD);
    assertNotNull(DownloadPhase.DOWNLOAD_SETUP);
  }

  @Test
  public void testDescriptions() {
    assertEquals("data download", DownloadPhase.DATA_DOWNLOAD.getDescription());
    assertEquals("download setup", DownloadPhase.DOWNLOAD_SETUP.getDescription());
  }

  @Test
  public void testValueOf() {
    // Verify enum can be looked up by name
    assertEquals(DownloadPhase.DATA_DOWNLOAD, DownloadPhase.valueOf("DATA_DOWNLOAD"));
    assertEquals(DownloadPhase.DOWNLOAD_SETUP, DownloadPhase.valueOf("DOWNLOAD_SETUP"));
  }
}
