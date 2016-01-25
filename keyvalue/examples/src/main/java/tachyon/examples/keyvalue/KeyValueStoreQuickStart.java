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
import tachyon.client.keyvalue.KeyValueStoreReader;
import tachyon.client.keyvalue.KeyValueStoreWriter;
import tachyon.client.keyvalue.KeyValueStores;

/**
 * A quick start tutorial for creating a key-value store, putting and getting some key-value pairs.
 */
public class KeyValueStoreQuickStart {
  /**
   * The main program.
   *
   * @param args one argument which is the path of the new key-value store
   * @throws Exception if any exception happens
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("java -cp " + Version.TACHYON_JAR + " "
          + KeyValueStoreQuickStart.class.getName() + " <key-value store URI>");
      System.exit(-1);
    }

    TachyonURI storeUri = new TachyonURI(args[0]);
    KeyValueStores kvStore = KeyValueStores.Factory.create();

    // Creates a store.
    KeyValueStoreWriter writer = kvStore.create(storeUri);

    // Puts a key-value pair ("key", "value").
    String key = "key";
    String value = "value";
    writer.put(key.getBytes(), value.getBytes());
    System.out.println(String.format("(%s, %s) is put into the key-value store", key, value));

    // Completes the store.
    writer.close();

    // Opens a store.
    KeyValueStoreReader reader = kvStore.open(storeUri);

    // Gets the value for "key".
    System.out.println(String.format("Value for key '%s' got from the store is '%s'", key,
        new String(reader.get(key.getBytes()))));

    // Closes the reader.
    reader.close();
  }
}
