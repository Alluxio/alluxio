/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.keyvalue;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;

import java.io.IOException;

/**
 * Client to access or create key-value stores in Alluxio.
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
     * @return a {@link KeyValueSystem} instance
     */
    public static synchronized KeyValueSystem create() {
      if (sKeyValueSystem == null) {
        sKeyValueSystem = new BaseKeyValueSystem();
      }
      return sKeyValueSystem;
    }
  }

  /**
   * Gets a reader to access a key-value store.
   *
   * @param uri {@link AlluxioURI} to the store
   * @return {@link BaseKeyValueStoreReader} instance
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  KeyValueStoreReader openStore(AlluxioURI uri) throws IOException, AlluxioException;

  /**
   * Gets a writer to create a new key-value store.
   *
   * @param uri {@link AlluxioURI} to the store
   * @return {@link BaseKeyValueStoreWriter} instance
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if Alluxio error occurs
   */
  KeyValueStoreWriter createStore(AlluxioURI uri) throws IOException, AlluxioException;

  /**
   * Rename a compelted key-value store.
   *
   * @param oldUri the old {@link AlluxioURI} to the store
   * @param newUri the new {@link AlluxioURI} to the store
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if other Alluxio error occurs
   */
  void renameStore(AlluxioURI oldUri, AlluxioURI newUri) throws IOException, AlluxioException;

  /**
   * Deletes a completed key-value store.
   *
   * @param uri {@link AlluxioURI} to the store
   * @throws IOException if non-Alluxio error occurs
   * @throws InvalidPathException if the uri exists but is not a key-value store
   * @throws FileDoesNotExistException if the uri does not exist
   * @throws AlluxioException if other Alluxio error occurs
   */
  void deleteStore(AlluxioURI uri)
      throws IOException, InvalidPathException, FileDoesNotExistException, AlluxioException;

  /**
   * Merges one completed key-value store to another completed key-value store.
   *
   * If there are the same keys from both stores, they are merged too, for these keys, whose value
   * will be retrieved is undetermined.
   *
   * @param fromUri the {@link AlluxioURI} to the store to be merged
   * @param toUri the {@link AlluxioURI} to the store to be merged to
   * @throws IOException if non-Alluxio error occurs
   * @throws AlluxioException if other Alluxio error occurs
   */
  void mergeStore(AlluxioURI fromUri, AlluxioURI toUri) throws IOException, AlluxioException;
}
