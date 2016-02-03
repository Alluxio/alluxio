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
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;

/**
 * Client to access or create key-value stores in Tachyon.
 */
@PublicApi
public interface KeyValueSystem {

  /**
   * Factory for the {@link KeyValueSystem}.
   */
  final class Factory {
    private static KeyValueSystem sKeyValueSystem = null;

    private Factory() {} // to prevent initialization

    /**
     * @return a (cached) {@link KeyValueStores} instance
     */
    public static synchronized KeyValueSystem create() {
      if (sKeyValueSystem == null) {
        reset();
      }
      return sKeyValueSystem;
    }

    /**
     * {@link tachyon.client.ClientContext} may be reset in different tests running in the same JVM,
     * in this case, the cached {@link KeyValueSystem} needs to be updated.
     */
    public static synchronized void reset() {
      sKeyValueSystem = new BaseKeyValueSystem();
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
  KeyValueStoreReader openStore(TachyonURI uri) throws IOException, TachyonException;

  /**
   * Gets a writer to create a new key-value store.
   *
   * @param uri {@link TachyonURI} to the store
   * @return {@link BaseKeyValueStoreWriter} instance
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if Tachyon error occurs
   */
  KeyValueStoreWriter createStore(TachyonURI uri) throws IOException, TachyonException;

  /**
   * Deletes a completed key-value store.
   *
   * @param uri {@link TachyonURI} to the store
   * @throws IOException if non-Tachyon error occurs
   * @throws InvalidPathException if the uri exists but is not a key-value store
   * @throws FileDoesNotExistException if the uri does not exist
   * @throws TachyonException if other Tachyon error occurs
   */
  void deleteStore(TachyonURI uri)
      throws IOException, InvalidPathException, FileDoesNotExistException, TachyonException;

  /**
   * Merges one completed key-value store to another completed key-value store.
   *
   * If there are the same keys from both stores, they are merged too, for these keys, whose value
   * will be retrieved is undetermined.
   *
   * @param fromUri the {@link TachyonURI} to the store to be merged
   * @param toUri the {@link TachyonURI} to the store to be merged to
   * @throws IOException if non-Tachyon error occurs
   * @throws TachyonException if other Tachyon error occurs
   */
  void mergeStore(TachyonURI fromUri, TachyonURI toUri) throws IOException, TachyonException;
}
