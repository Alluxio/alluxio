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

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.exception.ExceptionMessage;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Integration tests for {@link KeyValueStores}.
 */
public final class KeyValueStoresIntegrationTest {
  private static final int BLOCK_SIZE = 512 * Constants.MB;
  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static KeyValueStores sKVStores;

  private KeyValueStoreWriter mWriter;
  private KeyValueStoreReader mReader;
  private TachyonURI mStoreUri;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.GB, Constants.KB, BLOCK_SIZE,
          /* ensure key-value service is turned on */
          Constants.KEY_VALUE_ENABLED, "true");

  @BeforeClass
  public static void beforeClass() throws Exception {
    sKVStores = KeyValueStores.Factory.create();
  }

  @Before
  public void before() throws Exception {
    mStoreUri = new TachyonURI(PathUtils.uniqPath());
  }

  /**
   * Tests creating and opening an empty store.
   */
  @Test
  public void createAndOpenEmptyStoreTest() throws Exception {
    mWriter = sKVStores.create(mStoreUri);
    Assert.assertNotNull(mWriter);
    mWriter.close();

    mReader = sKVStores.open(mStoreUri);
    Assert.assertNotNull(mReader);
    mReader.close();
  }

  /**
   * Tests creating and opening a store with one key.
   */
  @Test
  public void createAndOpenStoreWithOneKeyTest() throws Exception {
    mWriter = sKVStores.create(mStoreUri);
    mWriter.put(KEY1, VALUE1);
    mWriter.close();

    mReader = sKVStores.open(mStoreUri);
    Assert.assertArrayEquals(VALUE1, mReader.get(KEY1));
    Assert.assertNull(mReader.get(KEY2));
    mReader.close();
  }

  /**
   * Tests creating and opening a store with a number of key.
   */
  @Test
  public void createAndOpenStoreWithMultiKeysTest() throws Exception {
    final int numKeys = 100;
    final int keyLength = 4; // 4Byte key
    final int valueLength = 5 * Constants.KB; // 5KB value
    mWriter = sKVStores.create(mStoreUri);
    for (int i = 0; i < numKeys; i ++) {
      byte[] key = BufferUtils.getIncreasingByteArray(i, keyLength);
      byte[] value = BufferUtils.getIncreasingByteArray(i, valueLength);
      mWriter.put(key, value);
    }
    mWriter.close();

    mReader = sKVStores.open(mStoreUri);
    for (int i = 0; i < numKeys; i ++) {
      byte[] key = BufferUtils.getIncreasingByteArray(i, keyLength);
      byte[] value = mReader.get(key);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(i, valueLength, value));
    }
    Assert.assertNull(mReader.get(KEY1));
    Assert.assertNull(mReader.get(KEY2));
    mReader.close();
  }

  /**
   * Tests creating and opening a store with a number of keys, while each key-value pair is large
   * enough to take a separate key-value partition.
   */
  @Test
  public void createMultiPartitionsTest() throws Exception {
    final long maxPartitionSize = Constants.MB; // Each partition is at most 1 MB
    final int numKeys = 10;
    final int keyLength = 4; // 4Byte key
    final int valueLength = 500 * Constants.KB; // 500KB value

    FileSystem tfs = FileSystem.Factory.get();

    ClientContext.getConf().set(Constants.KEY_VALUE_PARTITION_SIZE_BYTES_MAX,
        String.valueOf(maxPartitionSize));
    mWriter = sKVStores.create(mStoreUri);
    for (int i = 0; i < numKeys; i ++) {
      byte[] key = BufferUtils.getIncreasingByteArray(i, keyLength);
      byte[] value = BufferUtils.getIncreasingByteArray(i, valueLength);
      mWriter.put(key, value);
    }
    mWriter.close();

    List<URIStatus> files = tfs.listStatus(mStoreUri);
    Assert.assertEquals(numKeys, files.size());
    for (URIStatus info : files) {
      Assert.assertTrue(info.getLength() <= maxPartitionSize);
    }

    mReader = sKVStores.open(mStoreUri);
    for (int i = 0; i < numKeys; i ++) {
      byte[] key = BufferUtils.getIncreasingByteArray(i, keyLength);
      byte[] value = mReader.get(key);
      Assert.assertTrue(BufferUtils.equalIncreasingByteArray(i, valueLength, value));
    }
    Assert.assertNull(mReader.get(KEY1));
    Assert.assertNull(mReader.get(KEY2));
    mReader.close();
  }

  /**
   * Tests putting a key-value pair that is larger than the max key-value partition size,
   * expecting exception thrown.
   */
  @Test
  public void putKeyValueTooLargeTest() throws Exception {
    final long maxPartitionSize = 500 * Constants.KB; // Each partition is at most 500 KB
    final int keyLength = 4; // 4Byte key
    final int valueLength = 500 * Constants.KB; // 500KB value

    ClientContext.getConf().set(Constants.KEY_VALUE_PARTITION_SIZE_BYTES_MAX,
        String.valueOf(maxPartitionSize));
    mWriter = sKVStores.create(mStoreUri);
    byte[] key = BufferUtils.getIncreasingByteArray(0, keyLength);
    byte[] value = BufferUtils.getIncreasingByteArray(0, valueLength);

    mThrown.expect(IOException.class);
    mThrown.expectMessage(ExceptionMessage.KEY_VALUE_TOO_LARGE.getMessage(keyLength, valueLength));
    mWriter.put(key, value);
  }
}
