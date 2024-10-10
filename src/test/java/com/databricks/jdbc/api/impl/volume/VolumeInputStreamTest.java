package com.databricks.jdbc.api.impl.volume;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.callback.IDatabricksStatementHandle;
import com.databricks.jdbc.api.impl.IExecutionResult;
import java.io.InputStream;
import org.apache.http.HttpEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class VolumeInputStreamTest {

  @Mock HttpEntity httpEntity;
  @Mock InputStream inputStream;
  @Mock IExecutionResult resultHandler;
  @Mock IDatabricksStatementHandle statement;

  @Test
  void testInputStream() throws Exception {
    when(httpEntity.getContent()).thenReturn(inputStream);
    VolumeInputStream volumeInputStream =
        new VolumeInputStream(httpEntity, resultHandler, statement);

    when(inputStream.available()).thenReturn(1);
    assertEquals(1, volumeInputStream.available());

    when(inputStream.read()).thenReturn(10);
    assertEquals(10, volumeInputStream.read());

    byte[] buffer = new byte[1];
    when(inputStream.read(buffer)).thenReturn(11);
    assertEquals(11, volumeInputStream.read(buffer));

    when(inputStream.read(buffer, 1, 2)).thenReturn(12);
    assertEquals(12, volumeInputStream.read(buffer, 1, 2));

    when(inputStream.markSupported()).thenReturn(true);
    assertTrue(volumeInputStream.markSupported());

    when(inputStream.skip(3L)).thenReturn(5L);
    assertEquals(5L, volumeInputStream.skip(3L));

    volumeInputStream.reset();
    verify(inputStream).reset();

    volumeInputStream.mark(1);
    verify(inputStream).mark(1);

    when(httpEntity.isStreaming()).thenReturn(false);
    volumeInputStream.close();
    verify(resultHandler).close();
    verify(statement).close(true);
  }
}
