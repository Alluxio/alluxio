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

package tachyon.worker.hierarchy;

/**
 * Block information on worker side.
 */
public final class BlockInfo {
  private final StorageDir mDir;
  private final long mBlockId;
  private final long mBlockSize;

  public BlockInfo(StorageDir storageDir, long blockId, long blockSize) {
    mDir = storageDir;
    mBlockId = blockId;
    mBlockSize = blockSize;
  }

  /**
   * Get Id of the block
   * 
   * @return Id of the block
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * Get size of the block
   * 
   * @return size of the block in bytes
   */
  public long getSize() {
    return mBlockSize;
  }

  /**
   * Get the StorageDir which contains the block
   * 
   * @return StorageDir which contains the block
   */
  public StorageDir getStorageDir() {
    return mDir;
  }
}
