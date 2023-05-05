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

package alluxio.worker.dora;

import alluxio.proto.meta.DoraMeta.FileStatus;

import java.util.Optional;

/**
 * The Dora metadata Store.
 */
public interface DoraMetaStore {
  /**
   * queries dora metadata from the dora meta store.
   *
   * @param path the full path of this file
   * @return if found, the meta is returned
   */
  Optional<FileStatus> getDoraMeta(String path);

  /**
   * Adds dora metadata to the dora meta store. If the dora meta already exists,
   * its metadata will be updated to the given metadata.
   *
   * @param path the full path of this file
   * @param meta the block metadata
   */
  void putDoraMeta(String path, FileStatus meta);

  /**
   * Removes a dora meta, or does nothing if the dora meta does not exist.
   *
   * @param path the full path of the file
   */
  void removeDoraMeta(String path);

  /**
   * Removes all metadata from the dora meta store.
   */
  void clear();

  /**
   * Closes the block store and releases all resources.
   */
  void close();

  /**
   * @return number of metadata records currently stored in this store
   */
  Optional<Long> size();
}
