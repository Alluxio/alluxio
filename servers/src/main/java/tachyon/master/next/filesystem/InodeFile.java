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

package tachyon.master.next.filesystem;

import java.util.ArrayList;
import java.util.List;

import tachyon.master.next.block.meta.BlockId;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon file system's file representation in master.
 */
public class InodeFile extends Inode {
  private final long mBlockContainerId;
  private final long mBlockSizeBytes;

  // list of block ids.
  private final List<FileBlockInfo> mBlocks = new ArrayList<FileBlockInfo>(3);

  // length of inode file in bytes.
  private long mLength = 0;
  private boolean mIsComplete = false;
  private boolean mCache = false;
  private String mUfsPath = "";

  /**
   * Create a new InodeFile.
   *
   * @param name The name of the file
   * @param blockContainerId The block container id for this file. All blocks for this file will
   *                         belong to this block container.
   * @param parentId The inode id of the parent of the file
   * @param blockSizeBytes The block size of the file, in bytes
   * @param creationTimeMs The creation time of the file, in milliseconds
   */
  public InodeFile(String name, long blockContainerId, long parentId, long blockSizeBytes,
                   long creationTimeMs) {
    super(name, BlockId.createBlockId(blockContainerId, BlockId.getMaxSequenceNumber()), parentId,
        false, creationTimeMs);
    mBlockContainerId = blockContainerId;
    mBlockSizeBytes = blockSizeBytes;
  }

  /**
   * Commit a block to the file. It will check the legality. Cannot add the block if the file is
   * complete or the block's information doesn't match the file's information.
   *
   * @param fileBlockInfo The block to be added
   * @throws BlockInfoException
   */
  public synchronized void commitBlock(FileBlockInfo fileBlockInfo) throws BlockInfoException {
    if (mIsComplete) {
      throw new BlockInfoException("The file is complete: " + this);
    }
    if (mBlocks.size() > 0 && mBlocks.get(mBlocks.size() - 1).mLength != mBlockSizeBytes) {
      throw new BlockInfoException("mBlockSizeBytes is " + mBlockSizeBytes + ", but the "
          + "previous block size is " + mBlocks.get(mBlocks.size() - 1).mLength);
    }
    if (fileBlockInfo.mBlockIndex != mBlocks.size()) {
      throw new BlockInfoException("BLOCK_INDEX mismatch: " + mBlocks.size() + " != "
          + fileBlockInfo);
    }
    if (fileBlockInfo.mOffset != mBlocks.size() * mBlockSizeBytes) {
      throw new BlockInfoException("OFFSET mismatch: " + mBlocks.size() * mBlockSizeBytes + " != "
          + fileBlockInfo);
    }
    if (fileBlockInfo.mLength > mBlockSizeBytes) {
      throw new BlockInfoException("LENGTH too big: " + mBlockSizeBytes + " " + fileBlockInfo);
    }
    mLength += fileBlockInfo.mLength;
    mBlocks.add(fileBlockInfo);
  }

  @Override
  public ClientFileInfo generateClientFileInfo(String path) {
    ClientFileInfo ret = new ClientFileInfo();

    // TODO: change id to long.
    ret.id = (int) getId();
    ret.name = getName();
    ret.path = path;
    ret.ufsPath = mUfsPath;
    ret.length = mLength;
    ret.blockSizeByte = mBlockSizeBytes;
    ret.creationTimeMs = getCreationTimeMs();
    ret.isComplete = isComplete();
    ret.isFolder = false;
    ret.isPinned = isPinned();
    ret.isCache = mCache;
    ret.blockIds = getBlockIds();
    ret.inMemoryPercentage = getInMemoryPercentage();
    ret.lastModificationTimeMs = getLastModificationTimeMs();

    return ret;
  }

  /**
   * Get all the blocks of the file. It will return a duplication of the block list.
   *
   * @return a duplication of all the blocks' ids of the file
   */
  public synchronized List<Long> getBlockIds() {
    List<Long> ret = new ArrayList<Long>(mBlocks.size());
    for (FileBlockInfo fileBlockInfo : mBlocks) {
      ret.add(fileBlockInfo.mBlockId);
    }
    return ret;
  }

  public long getBlockContainerId() {
    return mBlockContainerId;
  }

  /**
   * Get the block size of the file
   *
   * @return the block size in bytes
   */
  public long getBlockSizeByte() {
    return mBlockSizeBytes;
  }

  /**
   * Get the path of the file in under file system
   *
   * @return the path of the file in under file system
   */
  public synchronized String getUfsPath() {
    return mUfsPath;
  }

  /**
   * Get the percentage of the file in memory. For a file that has all blocks in memory, it returns
   * 100; for a file that has no block in memory, it returns 0.
   *
   * @return the in memory percentage
   */
  private synchronized int getInMemoryPercentage() {
    if (mLength == 0) {
      return 100;
    }

    long inMemoryLength = 0;
    for (FileBlockInfo info : mBlocks) {
      if (info.isInMemory()) {
        inMemoryLength += info.mLength;
      }
    }
    return (int) (inMemoryLength * 100 / mLength);
  }

  /**
   * Get the length of the file in bytes.
   *
   * @return the length of the file in bytes
   */
  public synchronized long getLength() {
    return mLength;
  }

  /**
   * Get the id for a new block of the file. Also the id of the next block added into the file.
   *
   * @return the id of a new block of the file
   */
  public synchronized long getNewBlockId() {
    return BlockId.createBlockId(mBlockContainerId, mBlocks.size());
  }

  /**
   * Get the number of the blocks of the file
   *
   * @return the number of the blocks
   */
  public synchronized int getNumberOfBlocks() {
    return mBlocks.size();
  }

  /**
   * Return whether the file has checkpointed or not. Note that the file has checkpointed only
   * if the under file system path is not empty.
   *
   * @return true if the file has checkpointed, false otherwise
   */
  public synchronized boolean hasCheckpointed() {
    return !mUfsPath.equals("");
  }

  /**
   * Return whether the file is cacheable or not.
   *
   * @return true if the file is cacheable, false otherwise
   */
  public synchronized boolean isCache() {
    return mCache;
  }

  /**
   * Return whether the file is complete or not.
   *
   * @return true if the file is complete, false otherwise
   */
  public synchronized boolean isComplete() {
    return mIsComplete;
  }

  /**
   * Return whether the file is fully in memory or not. The file is fully in memory only if all the
   * blocks of the file are in memory, in other words, the in memory percentage is 100.
   *
   * @return true if the file is fully in memory, false otherwise
   */
  public synchronized boolean isFullyInMemory() {
    return getInMemoryPercentage() == 100;
  }

  /**
   * Set whether the file is cacheable or not.
   *
   * @param cache If true, the file is cacheable
   */
  public synchronized void setCache(boolean cache) {
    // TODO this related logic is not complete right. fix this.
    mCache = cache;
  }

  /**
   * Set the path of the file in under file system.
   *
   * @param ufsPath The new path of the file in under file system
   */
  public synchronized void setUfsPath(String ufsPath) {
    mUfsPath = ufsPath;
  }

  /**
   * The file is complete. Set the complete flag true.
   */
  public synchronized void setComplete() {
    mIsComplete = true;
  }

  /**
   * Set the complete flag of the file
   *
   * @param complete If true, the file is complete
   */
  public synchronized void setComplete(boolean complete) {
    mIsComplete = complete;
  }

  /**
   * Set the length of the file. Cannot set the length if the file is complete or the length is
   * negative.
   *
   * @param length The new length of the file, cannot be negative
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   */
  public synchronized void setLength(long length) throws SuspectedFileSizeException,
      BlockInfoException {
    if (isComplete()) {
      throw new SuspectedFileSizeException("InodeFile length was set previously.");
    }
    if (length < 0) {
      throw new SuspectedFileSizeException("InodeFile new length " + length + " is illegal.");
    }
    mLength = 0;
    while (length >= mBlockSizeBytes) {
      commitBlock(new FileBlockInfo(this, mBlocks.size(), mBlockSizeBytes));
      length -= mBlockSizeBytes;
    }
    if (length > 0) {
      commitBlock(new FileBlockInfo(this, mBlocks.size(), (int) length));
    }
    mIsComplete = true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFile(");
    sb.append(super.toString()).append(", LENGTH: ").append(mLength);
    sb.append(", UfsPath: ").append(mUfsPath);
    sb.append(", mBlocks: ").append(mBlocks);
    return sb.toString();
  }
}
