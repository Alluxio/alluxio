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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.Inode;

import java.io.IOException;

/**
 * Interface for deleting persisted entries from the UFS.
 */
public interface UfsDeleter {
  /**
   * Deletes a path if not covered by a recursive delete.
   *
   * @param alluxioUri Alluxio path to delete
   * @param inode to delete
   */
  void delete(AlluxioURI alluxioUri, Inode inode) throws IOException, InvalidPathException;
}
