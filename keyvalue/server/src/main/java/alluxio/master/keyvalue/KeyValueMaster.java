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

package alluxio.master.keyvalue;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.Master;
import alluxio.thrift.PartitionInfo;

import java.io.IOException;
import java.util.List;

/**
 * Interface of key-value master that stores key-value store information in Alluxio,
 * including the partitions of each key-value store.
 */
public interface KeyValueMaster extends Master {

  /**
   * Marks a partition complete and adds it to an incomplete key-value store.
   *
   * @param path URI of the key-value store
   * @param info information of this completed partition
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   * @throws InvalidPathException if the path is invalid
   */
  void completePartition(AlluxioURI path, PartitionInfo info)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException;

  /**
   * Marks a key-value store complete.
   *
   * @param path URI of the key-value store
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   * @throws InvalidPathException if the path is not valid
   * @throws AccessControlException if permission checking fails
   */
  void completeStore(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;

  /**
   * Creates a new key-value store.
   *
   * @param path URI of the key-value store
   * @throws FileAlreadyExistsException if a key-value store URI exists
   * @throws InvalidPathException if the given path is invalid
   * @throws AccessControlException if permission checking fails
   */
  void createStore(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, AccessControlException;

  /**
   * Deletes a completed key-value store.
   *
   * @param uri {@link AlluxioURI} to the store
   * @throws InvalidPathException if the uri exists but is not a key-value store
   * @throws FileDoesNotExistException if the uri does not exist
   */
  void deleteStore(AlluxioURI uri)
      throws IOException, InvalidPathException, FileDoesNotExistException, AlluxioException;

  /**
   * Renames one completed key-value store.
   *
   * @param oldUri the old {@link AlluxioURI} to the store
   * @param newUri the {@link AlluxioURI} to the store
   */
  void renameStore(AlluxioURI oldUri, AlluxioURI newUri) throws IOException, AlluxioException;

  /**
   * Merges one completed key-value store to another completed key-value store.
   *
   * @param fromUri the {@link AlluxioURI} to the store to be merged
   * @param toUri the {@link AlluxioURI} to the store to be merged to
   * @throws InvalidPathException if the uri exists but is not a key-value store
   * @throws FileDoesNotExistException if the uri does not exist
   */
  void mergeStore(AlluxioURI fromUri, AlluxioURI toUri)
      throws IOException, FileDoesNotExistException, InvalidPathException, AlluxioException;

  /**
   * Gets a list of partitions of a given key-value store.
   *
   * @param path URI of the key-value store
   * @return a list of partition information
   * @throws FileDoesNotExistException if the key-value store URI does not exists
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  List<PartitionInfo> getPartitionInfo(AlluxioURI path)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException;
}
