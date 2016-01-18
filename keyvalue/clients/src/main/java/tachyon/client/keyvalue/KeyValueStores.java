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

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.exception.TachyonException;

/**
 * Client to access or create key-value stores in Tachyon.
 */
@PublicApi
public interface KeyValueStores {

  /**
   * Factory for the {@link KeyValueStores}.
   */
  final class Factory {
    private static KeyValueStores sKeyValueStores = null;

    private Factory() {} // to prevent initialization

    /**
     * @return a {@link KeyValueStores} instance
     */
    public static synchronized KeyValueStores create() {
      if (sKeyValueStores == null) {
        sKeyValueStores = new BaseKeyValueStores();
      }
      return sKeyValueStores;
    }
  }

  /**
   * Gets a reader to access a key-value store.
   *
   * @param uri {@link TachyonURI} to the store
   * @return {@link BaseKeyValueStoreReader} instance
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  KeyValueStoreReader open(TachyonURI uri) throws IOException, TachyonException;

  /**
   * Gets a writer to create a new key-value store.
   *
   * @param uri {@link TachyonURI} to the store
   * @return {@link BaseKeyValueStoreWriter} instance
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  KeyValueStoreWriter create(TachyonURI uri) throws IOException, TachyonException;
}
