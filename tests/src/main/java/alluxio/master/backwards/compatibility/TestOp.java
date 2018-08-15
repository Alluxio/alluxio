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

package alluxio.master.backwards.compatibility;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemMasterClient;

public interface TestOp {
  /**
   * Applies the test operation.
   *
   * @param clients Alluxio clients for performing the operation
   */
  void apply(Clients clients) throws Exception;

  /**
   * Verifies the result of the test operation.
   *
   * @param clients Alluxio clients for performing the verification
   */
  void check(Clients clients) throws Exception;

  /**
   * @param version a version, e.g. 1.8.0, 1.9.0, etc
   * @return whether this operation is supported in the given version
   */
  default boolean supportsVersion(Version version) {
    // Support all versions by default
    return true;
  }

  /**
   * Container for various Alluxio clients.
   */
  class Clients {
    private final FileSystem mFs;
    private final FileSystemMasterClient mFsMaster;

    public Clients(FileSystem fs, FileSystemMasterClient fsm) {
      mFs = fs;
      mFsMaster = fsm;
    }

    public FileSystem getFs() {
      return mFs;
    }

    public FileSystemMasterClient getFileSystemMaster() {
      return mFsMaster;
    }
  }
}
