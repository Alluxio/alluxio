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

package alluxio.examples.keyvalue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.cli.CliUtils;
import alluxio.client.keyvalue.KeyValueIterator;
import alluxio.client.keyvalue.KeyValuePair;
import alluxio.client.keyvalue.KeyValueStoreReader;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.util.FormatUtils;
import alluxio.util.io.BufferUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/**
 * Tests whether two key-value stores contain the same set of key-value pairs.
 */
public final class SameKeyValueStoresTest implements Callable<Boolean> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final AlluxioURI mStoreUri1;
  private final AlluxioURI mStoreUri2;

  /**
   * @param storeUri1 the URI of the first key-value store
   * @param storeUri2 the URI of the second key-value store
   * @throws Exception if the instance fails to be created
   */
  public SameKeyValueStoresTest(AlluxioURI storeUri1, AlluxioURI storeUri2) throws Exception {
    mStoreUri1 = storeUri1;
    mStoreUri2 = storeUri2;
  }

  @Override
  public Boolean call() throws Exception {
    KeyValueSystem kvs = KeyValueSystem.Factory.create();
    KeyValueStoreReader reader1 = kvs.openStore(mStoreUri1);
    KeyValueStoreReader reader2 = kvs.openStore(mStoreUri2);
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
   * {@code java -cp ALLUXIO_JAR CLASS_NAME <key-value store URI 1> <key-value store URI 2>}.
   *
   * @param args two arguments, specifying the URIs of the stores to be compared
   * @throws Exception if unexpected errors happen
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: java -cp " + RuntimeConstants.ALLUXIO_JAR + " "
          + SameKeyValueStoresTest.class.getName() + " <key-value store URI 1>"
          + " <key-value store URI 2>");
      System.exit(-1);
    }

    boolean result = CliUtils.runExample(new SameKeyValueStoresTest(new AlluxioURI(args[0]),
        new AlluxioURI(args[1])));
    System.exit(result ? 0 : 1);
  }
}
