/**
 * 
 */
package tachyon.hadoop;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import tachyon.client.TachyonFS;

/**
 * Unit tests for TFS
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TachyonFS.class)
public class TFSTest {

  private TFS tfs;

  @Before
  public void setup() throws Exception {
    tfs = new TFS();
  }

  @Test
  public void shouldInitializeWithTachyonSchemePassedByUser() throws Exception {
    mockTachyonFSGet();
    // when
    tfs.initialize(new URI("tachyon://stanley:19998/tmp/path.txt"), new Configuration());
    // then
    PowerMockito.verifyStatic();
    TachyonFS.get("tachyon://stanley:19998");
  }

  @Test
  public void shouldInitializeWithTachyonFTSchemePassedByUser() throws Exception {
    mockTachyonFSGet();
    // when
    tfs.initialize(new URI("tachyon-ft://stanley:19998/tmp/path.txt"), new Configuration());
    // then
    verifyStatic();
    TachyonFS.get("tachyon-ft://stanley:19998");
  }

  private void mockTachyonFSGet() throws IOException {
    mockStatic(TachyonFS.class);
    TachyonFS tachyonFS = mock(TachyonFS.class);
    when(TachyonFS.get(anyString())).thenReturn(tachyonFS);
  }
}
