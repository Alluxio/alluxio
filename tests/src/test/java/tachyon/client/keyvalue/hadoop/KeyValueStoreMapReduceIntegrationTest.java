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

package tachyon.client.keyvalue.hadoop;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.keyvalue.KeyValueStoreReader;
import tachyon.client.keyvalue.KeyValueStoreWriter;
import tachyon.client.keyvalue.KeyValueStores;
import tachyon.examples.keyvalue.SameKeyValueStoresTest;
import tachyon.examples.keyvalue.hadoop.CloneKeyValueStoreMapReduce;
import tachyon.hadoop.ConfUtils;
import tachyon.hadoop.TFS;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;

/**
 * Tests {@link KeyValueInputFormat}, {@link KeyValueOutputFormat}, and
 * {@link KeyValueOutputCommitter} in MapReduce jobs.
 *
 * NOTE: The MapReduce jobs are run by {@link org.apache.hadoop.mapred.LocalJobRunner}.
 */
public final class KeyValueStoreMapReduceIntegrationTest {
  private static final int BLOCK_SIZE = 512 * Constants.MB;
  private static final int KEY_VALUE_PARTITION_SIZE = Constants.MB;

  private static Configuration sHadoopConf;

  @ClassRule
  public static LocalTachyonClusterResource sLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.GB, Constants.KB, BLOCK_SIZE,
          /* ensure key-value service is turned on */
          Constants.KEY_VALUE_ENABLED, "true",
          Constants.KEY_VALUE_PARTITION_SIZE_BYTES_MAX, Integer.toString(KEY_VALUE_PARTITION_SIZE));

  @BeforeClass
  public static void beforeClass() throws Exception {
    sHadoopConf = new Configuration();

    String tachyonMasterURI = sLocalTachyonClusterResource.get().getMasterUri();
    sHadoopConf.set("fs.defaultFS", tachyonMasterURI);
    sHadoopConf.set("fs.default.name", tachyonMasterURI);
    sHadoopConf.set(String.format("fs.%s.impl", Constants.SCHEME), TFS.class.getName());

    ConfUtils.storeToHadoopConfiguration(sLocalTachyonClusterResource.get().getMasterTachyonConf(),
        sHadoopConf);
  }

  /**
   * Tests that a MapReduce job can clone a key-value store.
   */
  @Test(timeout = 25000)
  public void cloneKeyValueStoreTest() throws Exception {
    final int numKeys = 4;
    final int valueBytes = KEY_VALUE_PARTITION_SIZE / 2;

    // Creates a key-value store.
    TachyonURI originalKVStoreURI = new TachyonURI(PathUtils.uniqPath());
    KeyValueStores kvStores = KeyValueStores.Factory.create();
    KeyValueStoreWriter writer = kvStores.create(originalKVStoreURI);
    for (int i = 1; i <= numKeys; i ++) {
      byte[] key = BufferUtils.getIncreasingByteArray(i);
      byte[] value = BufferUtils.getIncreasingByteArray(valueBytes);
      writer.put(key, value);
    }
    writer.close();

    // Verifies that the newly created store has correct key-value pairs.
    KeyValueStoreReader reader = kvStores.open(originalKVStoreURI);
    Assert.assertEquals(numKeys, reader.size());
    for (int i = 1; i <= numKeys; i ++) {
      byte[] key = BufferUtils.getIncreasingByteArray(i);
      byte[] expectedValue = BufferUtils.getIncreasingByteArray(valueBytes);
      byte[] gotValue = reader.get(key);
      Assert.assertEquals(ByteBuffer.wrap(expectedValue), ByteBuffer.wrap(gotValue));
    }
    reader.close();

    // Clones the store.
    TachyonURI clonedKVStoreURI = new TachyonURI(PathUtils.uniqPath());
    CloneKeyValueStoreMapReduce.run(sHadoopConf, originalKVStoreURI.getPath(),
        clonedKVStoreURI.getPath());

    // Verifies that the cloned store is the same as the original one.
    Assert.assertTrue(new SameKeyValueStoresTest(new TachyonURI(originalKVStoreURI.getPath()),
        new TachyonURI(clonedKVStoreURI.getPath())).call());
  }
}
