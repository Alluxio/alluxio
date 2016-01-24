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
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.keyvalue.KeyValueIterator;
import tachyon.client.keyvalue.KeyValuePair;
import tachyon.client.keyvalue.KeyValueStoreReader;
import tachyon.client.keyvalue.KeyValueStores;
import tachyon.examples.Utils;
import tachyon.util.FormatUtils;
import tachyon.util.io.BufferUtils;

/**
 * Tests whether two key-value stores contain the same set of key-value pairs.
 */
public class SameKeyValueStoresTest implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final TachyonURI mStoreUri1;
  private final TachyonURI mStoreUri2;

  /**
   * @param storeUri1 the URI of the first key-value store
   * @param storeUri2 the URI of the second key-value store
   * @throws Exception if the instance fails to be created
   */
  public SameKeyValueStoresTest(TachyonURI storeUri1, TachyonURI storeUri2) throws Exception {
    mStoreUri1 = storeUri1;
    mStoreUri2 = storeUri2;
  }

  @Override
  public Boolean call() throws Exception {
    KeyValueStores kvStores = KeyValueStores.Factory.create();
    KeyValueStoreReader reader1 = kvStores.open(mStoreUri1);
    KeyValueStoreReader reader2 = kvStores.open(mStoreUri2);
    boolean pass = areTheSameStores(reader1, reader2);
    reader1.close();
    reader2.close();
    return pass;
  }

  private boolean areTheSameStores(KeyValueStoreReader reader1, KeyValueStoreReader reader2)
      throws Exception {
    // The two stores are the same if the number of key-value pairs are the same, and the stores
    // have the same value for all the keys in store1.
    int size1 = reader1.size();
    int size2 = reader2.size();
    if (size1 != size2) {
      LOG.error("store {} has {} pairs while another has {} pairs", mStoreUri1, size1, size2);
      return false;
    }

    KeyValueIterator iterator = reader1.iterator();
    while (iterator.hasNext()) {
      KeyValuePair pair1 = iterator.next();
      byte[] key1 = BufferUtils.newByteArrayFromByteBuffer(pair1.getKey());
      byte[] value1 = BufferUtils.newByteArrayFromByteBuffer(pair1.getValue());
      byte[] value2 = reader2.get(key1);
      if (value2 == null) {
        LOG.error("store {} has value for key {} while another has no value for the same key",
            mStoreUri1, FormatUtils.byteArrayToHexString(key1));
        return false;
      }
      if (!ByteBuffer.wrap(value1).equals(ByteBuffer.wrap(value2))) {
        LOG.error("value for key {} are different in the two stores",
            FormatUtils.byteArrayToHexString(key1));
        return false;
      }
    }
    return true;
  }

  /**
   * Starts in a command like
   * {@code java -cp TACHYON_JAR CLASS_NAME <key-value store URI 1> <key-value store URI 2>}.
   *
   * @param args two arguments, specifying the URIs of the stores to be compared
   * @throws Exception if unexpected errors happen
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("java -cp " + Version.TACHYON_JAR + " "
          + SameKeyValueStoresTest.class.getName() + " <key-value store URI 1>"
          + " <key-value store URI 2>");
      System.exit(-1);
    }

    Utils.runExample(new SameKeyValueStoresTest(new TachyonURI(args[0]), new TachyonURI(args[1])));
  }
}
