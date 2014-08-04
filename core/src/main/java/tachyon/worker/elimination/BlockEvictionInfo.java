/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker.elimination;

/**
 * it is used for recording block information that will be used in block eviction.
 */
public class BlockEvictionInfo {
  int mDirIndex;
  long mBlockId;
  long mBlockSize;

  public BlockEvictionInfo(int dirIndex, long blockId, long blockSize) {
    mDirIndex = dirIndex;
    mBlockId = blockId;
    mBlockSize = blockSize;
  }

  /**
   * get id of the block
   * 
   * @return id of the block
   */
  public long getBlockId() {
    return mBlockId;
  }

  /**
   * get size of the block
   * 
   * @return size of the block
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * get index of the storage dir containing the block
   * 
   * @return index of the storage dir
   */
  public int getDirIndex() {
    return mDirIndex;
  }
}
