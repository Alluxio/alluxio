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
import alluxio.client.keyvalue.KeyValueStoreReader;
import alluxio.client.keyvalue.KeyValueStoreWriter;
import alluxio.client.keyvalue.KeyValueSystem;

/**
 * A quick start tutorial for creating a key-value store, putting and getting some key-value pairs.
 */
public final class KeyValueStoreQuickStart {

  private KeyValueStoreQuickStart() {} // prevent instantiation

  /**
   * The main program.
   *
   * @param args one argument which is the path of the new key-value store
   * @throws Exception if any exception happens
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: java -cp " + RuntimeConstants.ALLUXIO_JAR + " "
          + KeyValueStoreQuickStart.class.getName() + " <key-value store URI>");
      System.exit(-1);
    }

    AlluxioURI storeUri = new AlluxioURI(args[0]);
    KeyValueSystem kvs = KeyValueSystem.Factory.create();

    // Creates a store.
    KeyValueStoreWriter writer = kvs.createStore(storeUri);

    // Puts a key-value pair ("key", "value").
    String key = "key";
    String value = "value";
    writer.put(key.getBytes(), value.getBytes());
    System.out.println(String.format("(%s, %s) is put into the key-value store", key, value));

    // Completes the store.
    writer.close();

    // Opens a store.
    KeyValueStoreReader reader = kvs.openStore(storeUri);

    // Gets the value for "key".
    System.out.println(String.format("Value for key '%s' got from the store is '%s'", key,
        new String(reader.get(key.getBytes()))));

    // Closes the reader.
    reader.close();
  }
}
