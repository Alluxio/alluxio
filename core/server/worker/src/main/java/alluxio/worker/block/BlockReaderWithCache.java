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

package alluxio.worker.block;

import alluxio.worker.block.io.BlockReader;

/**
 * A reader interface to access the data of a block and optionally cache the block to Alluxio.
 * <p>
 * This class does not provide thread-safety.
 */
public interface BlockReaderWithCache extends BlockReader {
  /**
   * Checks whether the block is pending to be committed to Alluxio. This method can be accessed
   * after the block is closed.
   *
   * @return true if the block is pending to be committed to Alluxio
   */
  boolean isCommitPending();
}
