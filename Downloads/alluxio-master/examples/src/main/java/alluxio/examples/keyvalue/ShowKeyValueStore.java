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
import alluxio.RuntimeConstants;
import alluxio.client.keyvalue.KeyValueIterator;
import alluxio.client.keyvalue.KeyValuePair;
import alluxio.client.keyvalue.KeyValueStoreReader;
import alluxio.client.keyvalue.KeyValueSystem;
import alluxio.util.FormatUtils;
import alluxio.util.io.BufferUtils;

/**
 * Prints out (key, value) pairs, or only keys, or only values in a key-value store.
 */
public final class ShowKeyValueStore {
  private static void show(KeyValuePair pair, String scope) {
    String key = FormatUtils.byteArrayToHexString(BufferUtils.newByteArrayFromByteBuffer(
        pair.getKey()));
    String value = FormatUtils.byteArrayToHexString(BufferUtils.newByteArrayFromByteBuffer(
        pair.getValue()));
    switch (scope) {
      case "key":
        System.out.println(key);
        break;
      case "value":
        System.out.println(value);
        break;
      case "all":
        System.out.printf("%s %s%n", key, value);
        break;
      default:
        throw new RuntimeException(
                String.format("Unknown scope: %s, should be one of key/value/all", scope));
    }
  }

  private ShowKeyValueStore() {} // prevent instantiation

  /**
   * @param args two parameters, the first is the key-value store URI, the second is the scope of
   *    the store to be shown ("key" to show only keys, "value" to show only values, and "all" to
   *    show both keys and values)
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: java -cp " + RuntimeConstants.ALLUXIO_JAR + " "
          + ShowKeyValueStore.class.getName() + " <key-value store URI>"
          + " <scope, be one of key/value/all>");
      System.exit(-1);
    }

    KeyValueStoreReader reader = KeyValueSystem.Factory.create().openStore(new AlluxioURI(args[0]));
    KeyValueIterator iterator = reader.iterator();
    while (iterator.hasNext()) {
      show(iterator.next(), args[1]);
    }
    reader.close();
  }
}
