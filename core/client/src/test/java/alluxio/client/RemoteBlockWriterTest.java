package alluxio.client;

import alluxio.Configuration;
import alluxio.Constants;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * Tests for the {@link RemoteBlockWriter} class.
 */
public class RemoteBlockWriterTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void createFromMockClass() throws Exception {
    RemoteBlockWriter mock = Mockito.mock(RemoteBlockWriter.class);
    Configuration conf = new Configuration();
    conf.set(Constants.USER_BLOCK_REMOTE_WRITER, mock.getClass().getName());
    Assert.assertTrue(RemoteBlockWriter.Factory.create(conf).getClass().equals(mock.getClass()));
  }

  @Test
  public void createFailed() {
    mThrown.expect(RuntimeException.class);

    Configuration conf = new Configuration();
    conf.set(Constants.USER_BLOCK_REMOTE_WRITER, "unknown");
    RemoteBlockWriter.Factory.create(conf);
  }

}
