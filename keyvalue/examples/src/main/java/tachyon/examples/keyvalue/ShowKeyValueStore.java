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

import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.keyvalue.KeyValueIterator;
import tachyon.client.keyvalue.KeyValuePair;
import tachyon.client.keyvalue.KeyValueStoreReader;
import tachyon.client.keyvalue.KeyValueStores;
import tachyon.util.FormatUtils;
import tachyon.util.io.BufferUtils;

/**
 * Prints out (key, value) pairs, or only keys, or only values in a key-value store.
 */
public final class ShowKeyValueStore {
  private static void show(KeyValuePair pair, String scope) {
    String key = FormatUtils.byteArrayToHexString(BufferUtils.newByteArrayFromByteBuffer(
        pair.getKey()));
    String value = FormatUtils.byteArrayToHexString(BufferUtils.newByteArrayFromByteBuffer(
        pair.getValue()));
    if (scope.equals("key")) {
      System.out.println(key);
    } else if (scope.equals("value")) {
      System.out.println(value);
    } else if (scope.equals("all")) {
      System.out.printf("%s %s%n", key, value);
    } else {
      throw new RuntimeException(String.format("Unknown scope: %s, should be one of key/value/all",
          scope));
    }
  }

  /**
   * @param args two parameters, the first is the key-value store URI, the second is the scope of
   *    the store to be shown ("key" to show only keys, "value" to show only values, and "all" to
   *    show both keys and values)
   * @throws Exception if any exception happens
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: java -cp " + Version.TACHYON_JAR + " "
          + ShowKeyValueStore.class.getName() + " <key-value store URI>"
          + " <scope, be one of key/value/all>");
      System.exit(-1);
    }

    KeyValueStoreReader reader = KeyValueStores.Factory.create().open(new TachyonURI(args[0]));
    KeyValueIterator iterator = reader.iterator();
    while (iterator.hasNext()) {
      show(iterator.next(), args[1]);
    }
    reader.close();
  }
}
