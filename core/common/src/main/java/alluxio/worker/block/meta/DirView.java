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

package alluxio.worker.block.meta;

/**
 * A view of {@link StorageDir} to provide more limited access.
 */
public interface DirView {
  /**
   * Creates a {@link TempBlockMeta} given sessionId, blockId, and initialBlockSize.
   *
   * @param sessionId of the owning session
   * @param blockId of the new block
   * @param initialBlockSize of the new block
   * @return a new {@link TempBlockMeta} under the underlying directory
   */
  TempBlockMeta createTempBlockMeta(long sessionId, long blockId, long initialBlockSize);
}
