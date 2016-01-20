/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.keyvalue;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.client.ByteArrayOutStream;

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
    for (int i = 0; i < testKeys; i ++) {
      keys[i] = String.format("test-key:%d", i).getBytes();
      values[i] = String.format("test-val:%d", i).getBytes();
    }

    LinearProbingIndex index = LinearProbingIndex.createEmptyIndex();

    // Insert this batch of key-value pairs
    for (int i = 0; i < testKeys; i ++) {
      Assert.assertTrue(index.put(keys[i], values[i], mPayloadWriter));
      Assert.assertEquals(i + 1, index.keyCount());
    }
    mPayloadWriter.close();

    // Read all keys back, expect same value as inserted
    BasePayloadReader payloadReader =
        new BasePayloadReader(ByteBuffer.wrap(mOutStream.toByteArray()));
    for (int i = 0; i < testKeys; i ++) {
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
}
