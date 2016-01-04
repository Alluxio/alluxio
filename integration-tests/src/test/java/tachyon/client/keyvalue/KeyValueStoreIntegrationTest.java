package tachyon.client.keyvalue;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFileSystem;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for {@link KeyValueStore}.
 */
public final class KeyValueStoreIntegrationTest {
  private static final int BLOCK_SIZE = 512 * Constants.MB;
  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();
  private static TachyonFileSystem sTfs;

  private KeyValueStore mKVStore;
  private KeyValueStoreWriter mWriter;
  private KeyValueStoreReader mReader;

  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.GB, Constants.KB, BLOCK_SIZE);

  @BeforeClass
  public static void beforeClass() throws Exception {
    sTfs = sLocalTachyonClusterResource.get().getClient();
  }

  @Test
  public void aTest() throws Exception {
    TachyonURI uri = new TachyonURI(PathUtils.uniqPath());
    mKVStore = KeyValueStore.get();
    mWriter = mKVStore.create(uri);
  }
}
