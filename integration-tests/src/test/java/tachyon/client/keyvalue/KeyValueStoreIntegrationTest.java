package tachyon.client.keyvalue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFileSystem;
import tachyon.util.io.BufferUtils;
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
  private static KeyValueStore sKVStore;

  private KeyValueStoreWriter mWriter;
  private KeyValueStoreReader mReader;
  private TachyonURI mStoreUri;
  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.GB, Constants.KB, BLOCK_SIZE,
      /* ensure key-value service is turned on */
      Constants.KEYVALUE_ENABLED, "true");

  @BeforeClass
  public static void beforeClass() throws Exception {
    sTfs = sLocalTachyonClusterResource.get().getClient();
    sKVStore = KeyValueStore.Factory.create();
  }

  @Before
  public void before() throws Exception {
    mStoreUri = new TachyonURI(PathUtils.uniqPath());
  }

  @Test
  public void createAndOpenEmptyStoreTest() throws Exception {
    mWriter = sKVStore.create(mStoreUri);
    Assert.assertNotNull(mWriter);
    mWriter.close();

    mReader = sKVStore.open(mStoreUri);
    Assert.assertNotNull(mReader);
    mReader.close();
  }

  @Test
  public void createAndOpenStoreWithOneKeyTest() throws Exception {
    mWriter = sKVStore.create(mStoreUri);
    mWriter.put(KEY1, VALUE1);
    mWriter.close();

    mReader = sKVStore.open(mStoreUri);
    Assert.assertArrayEquals(VALUE1, mReader.get(KEY1));
    Assert.assertNull(mReader.get(KEY2));
  }

  @Test
  public void createAndOpenStoreWithMultiKeysTest() throws Exception {
    final int numKeys = 100;
    final int keyLength = 4; // 64 Byte key
    final int valueLength = 5 * Constants.KB; // 5KB value
    mWriter = sKVStore.create(mStoreUri);
    for (int i = 0; i < numKeys; i ++) {
      byte[] key = BufferUtils.getIncreasingByteArray(i, keyLength);
      byte[] value = BufferUtils.getIncreasingByteArray(i, valueLength);
      mWriter.put(key, value);
    }
    mWriter.close();

    mReader = sKVStore.open(mStoreUri);
    for (int i = 0; i < numKeys; i ++) {
      byte[] key = BufferUtils.getIncreasingByteArray(i, keyLength);
      byte[] value = mReader.get(key);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(i, valueLength, value));
    }
  }
}
