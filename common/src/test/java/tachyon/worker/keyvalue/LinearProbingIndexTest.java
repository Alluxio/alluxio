package tachyon.worker.keyvalue;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests of {@link LinearProbingIndex}.
 */
public class LinearProbingIndexTest {
  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();

  @Test
  public void putBasicTest() throws Exception {
    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();
    Assert.assertEquals(0, index.keyCount());
    Assert.assertTrue(index.put(KEY1, 0));
    Assert.assertEquals(1, index.keyCount());
    Assert.assertTrue(index.put(KEY2, 1));
    Assert.assertEquals(2, index.keyCount());
  }

  @Test
  public void putDuplicatedKeyTest() throws Exception {
    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();
    // TODO(binfan): change constant 50 to be LinearProbingIndex.MAX_PROBES
    for (int i = 0; i < 50; i ++) {
      Assert.assertEquals(i, index.keyCount());
      Assert.assertTrue(index.put(KEY1, 0));
    }
    Assert.assertFalse(index.put(KEY1, 0));
  }

  @Test
  public void getInsertedKeysTest() throws Exception {
    // Initialize a batch of key-value pairs
    int test_keys = 100;
    byte[][] keys = new byte[test_keys][];
    byte[][] values = new byte[test_keys][];
    for (int i = 0; i < test_keys; i ++) {
      keys[i] = String.format("test-key:%d", i).getBytes();
      values[i] = String.format("test-val:%d", i).getBytes();
    }

    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();

    // Insert this batch of key-value pairs
    Utils.MockOutputStream mockOutput = new Utils.MockOutputStream(10000);
    PayloadWriter payloadWriter = new PayloadWriter(mockOutput);
    for (int i = 0; i < test_keys; i ++) {
      int offset = mockOutput.getCount();
      payloadWriter.addKeyAndValue(keys[i], values[i]);
      payloadWriter.flush();
      Assert.assertTrue(index.put(keys[i], offset));
      Assert.assertEquals(i + 1, index.keyCount());
    }
    payloadWriter.close();

    // Read all keys back, expect same value as inserted
    PayloadReader payloadReader = new PayloadReader(mockOutput.toByteArray());
    for (int i = 0; i < test_keys; i ++) {
      byte[] value = index.get(keys[i], payloadReader);
      Assert.assertArrayEquals(values[i], value);
    }
  }

  @Test
  public void getNonExistentKeyTest() throws Exception {
    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();
    PayloadReader payloadReaderNotUsed = new PayloadReader(new byte[] {});
    byte[] nonExistentKey = "NotInserted".getBytes();
    Assert.assertNull(index.get(nonExistentKey, payloadReaderNotUsed));
  }
}
