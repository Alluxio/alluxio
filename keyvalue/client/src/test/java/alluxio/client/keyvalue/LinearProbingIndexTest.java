/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.keyvalue;

import alluxio.client.ByteArrayOutStream;
import alluxio.util.io.BufferUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Unit tests of {@link LinearProbingIndex}.
 */
public class LinearProbingIndexTest {
  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();
  private ByteArrayOutStream mOutStream;
  private BasePayloadWriter mPayloadWriter;

  @Before
  public void before() {
    mOutStream = new ByteArrayOutStream();
    mPayloadWriter = new BasePayloadWriter(mOutStream);
  }

  /**
   * Tests {@link LinearProbingIndex#put} to work.
   */
  @Test
  public void putBasicTest() throws Exception {
    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();
    Assert.assertEquals(0, index.keyCount());
    Assert.assertTrue(index.put(KEY1, VALUE1, mPayloadWriter));
    Assert.assertEquals(1, index.keyCount());
    Assert.assertTrue(index.put(KEY2, VALUE2, mPayloadWriter));
    Assert.assertEquals(2, index.keyCount());
  }

  /**
   * Tests {@link LinearProbingIndex#get} to return correct values for inserted keys.
   */
  @Test
  public void getInsertedKeysTest() throws Exception {
    // Initialize a batch of key-value pairs
    int testKeys = 100;
    byte[][] keys = new byte[testKeys][];
    byte[][] values = new byte[testKeys][];
    for (int i = 0; i < testKeys; i++) {
      keys[i] = String.format("test-key:%d", i).getBytes();
      values[i] = String.format("test-val:%d", i).getBytes();
    }

    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();

    // Insert this batch of key-value pairs
    for (int i = 0; i < testKeys; i++) {
      Assert.assertTrue(index.put(keys[i], values[i], mPayloadWriter));
      Assert.assertEquals(i + 1, index.keyCount());
    }
    mPayloadWriter.close();

    // Read all keys back, expect same value as inserted
    BasePayloadReader payloadReader =
        new BasePayloadReader(ByteBuffer.wrap(mOutStream.toByteArray()));
    for (int i = 0; i < testKeys; i++) {
      ByteBuffer value = index.get(ByteBuffer.wrap(keys[i]), payloadReader);
      Assert.assertEquals(ByteBuffer.wrap(values[i]), value);
    }
  }

  /**
   * Tests {@link LinearProbingIndex#get} to return null for non-existent key.
   */
  @Test
  public void getNonExistentKeyTest() throws Exception {
    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();
    BasePayloadReader payloadReaderNotUsed =
        new BasePayloadReader(ByteBuffer.allocate(1));
    ByteBuffer nonExistentKey = ByteBuffer.allocate(10);
    nonExistentKey.put("NoSuchKey".getBytes());
    Assert.assertNull(index.get(nonExistentKey, payloadReaderNotUsed));
  }

  /**
   * Tests that {@link LinearProbingIndex#keyCount()} changes while key-value pairs are inserted,
   * and can be correctly recovered after recovering {@link LinearProbingIndex} from an byte array.
   */
  @Test
  public void keyCountTest() throws Exception {
    // keyCount should increase while inserting key-value pairs.
    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();
    Assert.assertEquals(0, index.keyCount());

    index.put(KEY1, VALUE1, mPayloadWriter);
    Assert.assertEquals(1, index.keyCount());
    index.put(KEY2, VALUE2, mPayloadWriter);
    Assert.assertEquals(2, index.keyCount());
    mPayloadWriter.close();

    // keyCount should be correctly recovered after recovering Index from byte array.
    byte[] indexRawBytes = index.getBytes();
    index = LinearProbingIndex.loadFromByteArray(ByteBuffer.wrap(indexRawBytes));
    Assert.assertEquals(2, index.keyCount());
  }

  /**
   * Tests that {@link LinearProbingIndex#byteCount()} should be correctly recovered after
   * recovering {@link LinearProbingIndex} from byte array.
   */
  @Test
  public void byteCountTest() throws Exception {
    // Empty Index.
    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();
    int count = index.byteCount();
    index = LinearProbingIndex.loadFromByteArray(ByteBuffer.wrap(index.getBytes()));
    Assert.assertEquals(count, index.byteCount());

    // Non-empty Index.
    index.put(KEY1, VALUE1, mPayloadWriter);
    index.put(KEY2, VALUE2, mPayloadWriter);
    mPayloadWriter.close();
    count = index.byteCount();
    index = LinearProbingIndex.loadFromByteArray(ByteBuffer.wrap(index.getBytes()));
    Assert.assertEquals(count, index.byteCount());
  }

  private PayloadReader createPayloadReader() throws IOException {
    return new BasePayloadReader(ByteBuffer.wrap(mOutStream.toByteArray()));
  }

  private byte[] nextKey(LinearProbingIndex index, byte[] key) throws IOException {
    ByteBuffer currentKey = key == null ? null : ByteBuffer.wrap(key);
    ByteBuffer ret = index.nextKey(currentKey, createPayloadReader());
    return ret == null ? null : BufferUtils.newByteArrayFromByteBuffer(ret);
  }

  /**
   * Tests that {@link LinearProbingIndex#nextKey(ByteBuffer, PayloadReader)} works correctly for
   * both empty and non-empty index.
   */
  @Test
  public void nextKeyTest() throws Exception {
    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();
    Assert.assertNull(nextKey(index, null));

    index.put(KEY1, VALUE1, mPayloadWriter);
    Assert.assertArrayEquals(KEY1, nextKey(index, null));

    index.put(KEY2, VALUE2, mPayloadWriter);
    byte[] firstKey = KEY1;
    byte[] secondKey = KEY2;
    // Keys with smaller hash is positioned closer to the beginning of the Index buffer.
    if (index.indexHash(KEY1) > index.indexHash(KEY2)) {
      firstKey = KEY2;
      secondKey = KEY1;
    }
    Assert.assertArrayEquals(firstKey, nextKey(index, null));
    Assert.assertArrayEquals(firstKey, nextKey(index, null));
    Assert.assertArrayEquals(secondKey, nextKey(index, firstKey));
    Assert.assertNull(nextKey(index, secondKey));
  }

  /**
   * Tests that {@link LinearProbingIndex#keyIterator(PayloadReader)} works correctly for both empty
   * and non-empty index.
   */
  @Test
  public void keyIteratorTest() throws Exception {
    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();
    Assert.assertNull(nextKey(index, null));

    Iterator<ByteBuffer> keyIterator = index.keyIterator(createPayloadReader());
    Assert.assertFalse(keyIterator.hasNext());

    index.put(KEY1, VALUE1, mPayloadWriter);
    index.put(KEY2, VALUE2, mPayloadWriter);
    mPayloadWriter.close();

    byte[] firstKey = KEY1;
    byte[] secondKey = KEY2;
    // Keys with smaller hash is positioned closer to the beginning of the Index buffer.
    if (index.indexHash(KEY1) > index.indexHash(KEY2)) {
      firstKey = KEY2;
      secondKey = KEY1;
    }
    keyIterator = index.keyIterator(createPayloadReader());
    Assert.assertArrayEquals(firstKey, BufferUtils.newByteArrayFromByteBuffer(keyIterator.next()));
    Assert.assertArrayEquals(secondKey, BufferUtils.newByteArrayFromByteBuffer(keyIterator.next()));
    Assert.assertFalse(keyIterator.hasNext());
  }
}
