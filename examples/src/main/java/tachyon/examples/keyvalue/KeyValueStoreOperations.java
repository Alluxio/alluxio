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

package tachyon.examples.keyvalue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.ClientContext;
import tachyon.client.keyvalue.KeyValueIterator;
import tachyon.client.keyvalue.KeyValuePair;
import tachyon.client.keyvalue.KeyValueStoreReader;
import tachyon.client.keyvalue.KeyValueStoreWriter;
import tachyon.client.keyvalue.KeyValueStores;
import tachyon.conf.TachyonConf;
import tachyon.examples.Utils;
import tachyon.util.io.BufferUtils;

/**
 * This example illustrates how to create a key-value store, put key-value pairs into the store, and
 * read the store afterwards.
 */
public final class KeyValueStoreOperations implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final int mPartitionLength = Constants.MB;
  private final int mValueLength = mPartitionLength / 2;
  private final int mNumKeyValuePairs = 10;

  private TachyonURI mStoreUri;
  private Map<ByteBuffer, ByteBuffer> mKeyValuePairs = Maps.newHashMap();

  /**
   * @param storeUri URI of the key-value store to write to, should not exist before
   * @throws Exception if the instance fails to be created
   */
  public KeyValueStoreOperations(TachyonURI storeUri) throws Exception {
    mStoreUri = storeUri;
  }

  @Override
  public Boolean call() throws Exception {
    TachyonConf tachyonConf = ClientContext.getConf();
    tachyonConf.set(Constants.KEY_VALUE_ENABLED, String.valueOf(true));
    tachyonConf.set(Constants.KEY_VALUE_PARTITION_SIZE_BYTES_MAX, String.valueOf(mPartitionLength));

    KeyValueStores kvStores = KeyValueStores.Factory.create();

    KeyValueStoreWriter writer = kvStores.create(mStoreUri);
    putKeyValuePairs(writer);
    writer.close();

    KeyValueStoreReader reader = kvStores.open(mStoreUri);
    boolean pass = getKeyValuePairs(reader);
    reader.close();

    return pass;
  }

  private void putKeyValuePairs(KeyValueStoreWriter writer) throws Exception {
    LOG.info("Putting key-value pairs...");
    // API: KeyValueStoreWriter#put
    for (int i = 0; i < mNumKeyValuePairs; i ++) {
      // Keys are 0, 1, 2, etc.
      byte[] key = ByteBuffer.allocate(4).putInt(i).array();
      // Values are byte arrays of length {@link #mValueLength}.
      byte[] value = BufferUtils.getIncreasingByteArray(mValueLength);
      writer.put(key, value);
      mKeyValuePairs.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    }
  }

  private boolean getKeyValuePairs(KeyValueStoreReader reader) throws Exception {
    LOG.info("Getting key-value pairs...");
    // API: KeyValueStoreReader#size
    if (reader.size() != mNumKeyValuePairs) {
      LOG.error("The key-value store has the wrong numbers of key-value pairs");
      return false;
    }

    // API: KeyValueStoreReader#get
    for (Map.Entry<ByteBuffer, ByteBuffer> pair : mKeyValuePairs.entrySet()) {
      ByteBuffer expectedValue = pair.getValue();
      ByteBuffer gotValue = reader.get(pair.getKey());
      if (!expectedValue.equals(gotValue)) {
        LOG.error("The value returned from the key-value store iterator is unexpected");
        return false;
      }
    }

    // API: KeyValueStoreReader#iterator
    KeyValueIterator iterator = reader.iterator();
    while (iterator.hasNext()) {
      KeyValuePair kv = iterator.next();
      ByteBuffer expectedValue = mKeyValuePairs.get(kv.getKey());
      ByteBuffer gotValue = kv.getValue();
      if (!expectedValue.equals(gotValue)) {
        LOG.error("The value returned from the key-value store iterator is not expected");
        return false;
      }
    }

    return true;
  }

  /**
   * Starts in a command like {@code java -cp TACHYON_JAR CLASS_NAME <key-value store URI>}.
   *
   * @param args one argument, specifying the URI of the store to be created
   * @throws Exception if unexpected errors happen
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: java -cp " + Version.TACHYON_JAR + " "
          + KeyValueStoreOperations.class.getName() + " <key-value store URI>");
      System.exit(-1);
    }

    Utils.runExample(new KeyValueStoreOperations(new TachyonURI(args[0])));
  }
}
