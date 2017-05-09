/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.keyvalue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.BaseIntegrationTest;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.util.io.PathUtils;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for {@link KeyValuePartitionReader} and {@link KeyValuePartitionWriter}.
 */
public final class KeyValuePartitionIntegrationTest extends BaseIntegrationTest {
  private static final int BLOCK_SIZE = 512 * Constants.MB;
  private static final String BASE_KEY = "base_key";
  private static final String BASE_VALUE = "base_value";
  /** The number of pairs generated by {@link #genKeyValuePairs(int)} can be held by a partition. */
  private static final int BASE_KEY_VALUE_NUMBER = 100;
  private static final byte[] KEY1 = "key1".getBytes();
  private static final byte[] KEY2 = "key2_foo".getBytes();
  private static final byte[] VALUE1 = "value1".getBytes();
  private static final byte[] VALUE2 = "value2_bar".getBytes();
  private static FileSystem sFileSystem;
  private KeyValuePartitionWriter mKeyValuePartitionWriter;
  private KeyValuePartitionReader mKeyValuePartitionReader;
  private AlluxioURI mPartitionUri;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Generate a sequence of key-value pairs in the format like
   * ({@link #BASE_KEY}_{@code i}, {@link #BASE_VALUE}_{@code i}), {@code i} is in the interval
   * [0, {@code length}).
   *
   * @param length the number of key-value pairs
   * @return the list of generated key-value pairs
   */
  private List<KeyValuePair> genKeyValuePairs(int length) {
    List<KeyValuePair> pairs = Lists.newArrayListWithExpectedSize(length);
    for (int i = 0; i < length; i++) {
      String key = String.format("%s_%d", BASE_KEY, i);
      String value = String.format("%s_%d", BASE_VALUE, i);
      pairs.add(new KeyValuePair(key.getBytes(), value.getBytes()));
    }
    return pairs;
  }

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, Constants.GB)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE)
          /* ensure key-value service is turned on */
          .setProperty(PropertyKey.KEY_VALUE_ENABLED, true)
          .build();

  @BeforeClass
  public static void beforeClass() throws Exception {
    sFileSystem = sLocalAlluxioClusterResource.get().getClient();
  }

  private AlluxioURI getUniqURI() {
    return new AlluxioURI(PathUtils.uniqPath());
  }

  @Before
  public void before() {
    mPartitionUri = getUniqURI();
  }

  /**
   * Tests a {@link KeyValuePartitionWriter} can create a partition, write key-value pairs and
   * close. Meanwhile the {@link KeyValuePartitionReader} can open this saved partition and find
   * keys store by the writer.
   */
  @Test
  public void readerWriter() throws Exception {
    mKeyValuePartitionWriter = KeyValuePartitionWriter.Factory.create(mPartitionUri);
    mKeyValuePartitionWriter.put(KEY1, VALUE1);
    mKeyValuePartitionWriter.put(KEY2, VALUE2);
    mKeyValuePartitionWriter.close();
    // Expect the key-value partition exists as an Alluxio file
    Assert.assertTrue(sFileSystem.exists(mPartitionUri));
    mKeyValuePartitionReader = KeyValuePartitionReader.Factory.create(mPartitionUri);
    Assert.assertArrayEquals(VALUE1, mKeyValuePartitionReader.get(KEY1));
    Assert.assertArrayEquals(VALUE2, mKeyValuePartitionReader.get(KEY2));
    Assert.assertNull(mKeyValuePartitionReader.get("NoSuchKey".getBytes()));
    mKeyValuePartitionReader.close();
  }

  /**
   * Tests that {@link KeyValuePartitionReader#size()} is correct when a new reader is created.
   */
  @Test
  public void size() throws Exception {
    byte[][] keys = new byte[][]{KEY1, KEY2};
    byte[][] values = new byte[][]{VALUE1, VALUE2};
    for (int size = 0; size <= 2; size++) {
      mKeyValuePartitionWriter = KeyValuePartitionWriter.Factory.create(mPartitionUri);
      for (int i = 0; i < size; i++) {
        mKeyValuePartitionWriter.put(keys[i], values[i]);
      }
      mKeyValuePartitionWriter.close();

      mKeyValuePartitionReader = KeyValuePartitionReader.Factory.create(mPartitionUri);
      Assert.assertEquals(size, mKeyValuePartitionReader.size());
      mKeyValuePartitionReader.close();

      mPartitionUri = getUniqURI();
    }
  }

  /**
   * Tests that the iterator returned by {@link KeyValuePartitionReader#iterator()} for an empty
   * partition has no elements to be iterated.
   */
  @Test
  public void emptyPartitionIterator() throws Exception {
    // Creates an empty partition.
    KeyValuePartitionWriter.Factory.create(mPartitionUri).close();
    mKeyValuePartitionReader = KeyValuePartitionReader.Factory.create(mPartitionUri);
    Assert.assertFalse(mKeyValuePartitionReader.iterator().hasNext());
    mKeyValuePartitionReader.close();
  }

  /**
   * Tests that {@link KeyValuePartitionReader#iterator()} can iterate over a partition correctly.
   * <p>
   * There is no assumption about the order of iteration, it just makes sure all key-value pairs are
   * iterated.
   */
  @Test
  public void noOrderIterator() throws Exception {
    List<KeyValuePair> pairs = genKeyValuePairs(BASE_KEY_VALUE_NUMBER);
    List<KeyValuePair> iteratedPairs = Lists.newArrayListWithExpectedSize(pairs.size());

    mKeyValuePartitionWriter = KeyValuePartitionWriter.Factory.create(mPartitionUri);
    for (KeyValuePair pair : pairs) {
      mKeyValuePartitionWriter.put(pair.getKey().array(), pair.getValue().array());
    }
    mKeyValuePartitionWriter.close();

    mKeyValuePartitionReader = KeyValuePartitionReader.Factory.create(mPartitionUri);
    KeyValueIterator iterator = mKeyValuePartitionReader.iterator();
    while (iterator.hasNext()) {
      iteratedPairs.add(iterator.next());
    }
    mKeyValuePartitionReader.close();
    Assert.assertEquals(pairs.size(), iteratedPairs.size());

    // Sort both pairs and iteratedPairs, then compare them.
    Collections.sort(pairs);
    Collections.sort(iteratedPairs);
    Assert.assertEquals(pairs, iteratedPairs);
  }

  /**
   * Tests that an exception is thrown when trying to create an instance via
   * {@link KeyValuePartitionWriter.Factory#create(AlluxioURI)} which is null.
   */
  @Test
  public void createWriterUriNotNull() throws IOException, AlluxioException {
    mThrown.expect(NullPointerException.class);
    KeyValuePartitionWriter.Factory.create(null);
  }

  /**
   * Tests that the factory can create an instance of {@link KeyValuePartitionWriter}.
   */
  @Test
  public void createWriter() throws IOException, AlluxioException {
    mKeyValuePartitionWriter = KeyValuePartitionWriter.Factory.create(mPartitionUri);
    Assert.assertNotNull(mKeyValuePartitionWriter);
    mKeyValuePartitionWriter.close();
  }

  /**
   * Tests that an exception is thrown when trying to create an instance via
   * {@link KeyValuePartitionReader.Factory#create(AlluxioURI)} which is null.
   */
  @Test
  public void createReaderUriNotNull() throws IOException, AlluxioException {
    mThrown.expect(NullPointerException.class);
    KeyValuePartitionReader.Factory.create(null);
  }

  /**
   * Tests that the factory can create an instance of {@link KeyValuePartitionReader} with a given
   * {@link AlluxioURI}.
   */
  @Test
  public void createReaderWithUri() throws IOException, AlluxioException {
    mKeyValuePartitionWriter = KeyValuePartitionWriter.Factory.create(mPartitionUri);
    mKeyValuePartitionWriter.close();

    mKeyValuePartitionReader = KeyValuePartitionReader.Factory.create(mPartitionUri);
    Assert.assertNotNull(mKeyValuePartitionReader);
    mKeyValuePartitionReader.close();
  }

  /**
   * Tests that the factory can create an instance of {@link KeyValuePartitionReader} with a given
   * ID of a block.
   */
  @Test
  public void createReaderWithBlockId() throws IOException, AlluxioException {
    mKeyValuePartitionWriter = KeyValuePartitionWriter.Factory.create(mPartitionUri);
    mKeyValuePartitionWriter.close();

    long blockId = sFileSystem.getStatus(mPartitionUri).getBlockIds().get(0);

    KeyValuePartitionReader reader = KeyValuePartitionReader.Factory.create(blockId);
    Assert.assertNotNull(reader);
    reader.close();
  }
}
