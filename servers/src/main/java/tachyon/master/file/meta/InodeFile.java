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

package tachyon.master.file.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.master.block.BlockId;
import tachyon.master.file.journal.InodeFileEntry;
import tachyon.master.journal.JournalOutputStream;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon file system's file representation in master.
 */
public final class InodeFile extends Inode {
  private final long mBlockContainerId;
  private final long mBlockSizeBytes;

  // list of block ids.
  private List<Long> mBlocks;

  // length of inode file in bytes.
  private long mLength = 0;

  private boolean mIsComplete = false;
  private boolean mCache = false;
  private String mUfsPath = "";

  /**
   * Creates a new InodeFile.
   *
   * @param name The name of the file
   * @param blockContainerId The block container id for this file. All blocks for this file will
   *        belong to this block container.
   * @param parentId The inode id of the parent of the file
   * @param blockSizeBytes The block size of the file, in bytes
   * @param creationTimeMs The creation time of the file, in milliseconds
   */
  public InodeFile(String name, long blockContainerId, long parentId, long blockSizeBytes,
      long creationTimeMs) {
    super(name, BlockId.createBlockId(blockContainerId, BlockId.getMaxSequenceNumber()), parentId,
        false, creationTimeMs);
    mBlocks = new ArrayList<Long>(3);
    mBlockContainerId = blockContainerId;
    mBlockSizeBytes = blockSizeBytes;
  }

  @Override
  public FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();

    // note: in-memory percentage is NOT calculated here, because it needs blocks info stored in
    // block master
    ret.fileId = getId();
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
    ret.lastModificationTimeMs = getLastModificationTimeMs();

    return ret;
  }

  /**
   * Gets all the blocks of the file. It will return a duplication of the block list.
   *
   * @return a duplication of all the blocks' ids of the file
   */
  public synchronized List<Long> getBlockIds() {
    return new ArrayList<Long>(mBlocks);
  }

  /**
   * Gets the block size of the file
   *
   * @return the block size in bytes
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * Gets the path of the file in under file system
   *
   * @return the path of the file in under file system
   */
  public synchronized String getUfsPath() {
    return mUfsPath;
  }

  /**
   * Gets the length of the file in bytes. This is not accurate before the file is closed.
   *
   * @return the length of the file in bytes
   */
  public synchronized long getLength() {
    return mLength;
  }

  /**
   * Gets the id for a new block of the file.
   *
   * @return the id of a new block of the file
   */
  public synchronized long getNewBlockId() {
    long blockId = BlockId.createBlockId(mBlockContainerId, mBlocks.size());
    // TODO: check for max block sequence number, and sanity check the sequence number.
    // TODO: check isComplete?
    // TODO: This will not work with existing lineage implementation, since a new writer will not be
    // able to get the same block ids (to write the same block ids).
    mBlocks.add(blockId);
    return blockId;
  }

  public synchronized long getBlockIdByIndex(int blockIndex) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex >= mBlocks.size()) {
      throw new BlockInfoException(
          "blockIndex " + blockIndex + " is out of range. File blocks: " + mBlocks.size());
    }
    return mBlocks.get(blockIndex);
  }

  /**
   * Gets the number of the blocks of the file
   *
   * @return the number of the blocks
   */
  public synchronized int getNumberOfBlocks() {
    return mBlocks.size();
  }

  /**
   * Returns whether the file has checkpointed or not. Note that the file has checkpointed only if
   * the under file system path is not empty.
   *
   * @return true if the file has checkpointed, false otherwise
   */
  public synchronized boolean hasCheckpointed() {
    return !mUfsPath.equals("");
  }

  /**
   * Returns whether the file is cacheable or not.
   *
   * @return true if the file is cacheable, false otherwise
   */
  public synchronized boolean isCache() {
    return mCache;
  }

  /**
   * Returns whether the file is complete or not.
   *
   * @return true if the file is complete, false otherwise
   */
  public synchronized boolean isComplete() {
    return mIsComplete;
  }

  public synchronized void setBlockIds(List<Long> blockIds) {
    mBlocks = Preconditions.checkNotNull(blockIds);
  }

  /**
   * Sets whether the file is cacheable or not.
   *
   * @param cache If true, the file is cacheable
   */
  public synchronized void setCache(boolean cache) {
    // TODO this related logic is not complete right. fix this.
    mCache = cache;
  }

  /**
   * The file is complete. Set the complete flag true, and set the length
   *
   * @param length the length of the complete file
   */
  public synchronized void setComplete(long length) {
    mIsComplete = true;
    mLength = length;
  }

  /**
   * Sets the path of the file in under file system.
   *
   * @param ufsPath The new path of the file in under file system
   */
  public synchronized void setUfsPath(String ufsPath) {
    mUfsPath = ufsPath;
  }

  /**
   * Sets the length of the file. Cannot set the length if the file is complete or the length is
   * negative.
   *
   * @param length The new length of the file, cannot be negative
   * @throws SuspectedFileSizeException
   * @throws BlockInfoException
   */
  public synchronized void setLength(long length)
      throws SuspectedFileSizeException, BlockInfoException {
    if (isComplete()) {
      throw new SuspectedFileSizeException("InodeFile length was set previously.");
    }
    if (length < 0) {
      throw new SuspectedFileSizeException("InodeFile new length " + length + " is negative.");
    }
    mLength = length;
    mBlocks.clear();
    while (length > 0) {
      long blockSize = Math.min(length, mBlockSizeBytes);
      getNewBlockId();
      length -= blockSize;
    }
    setComplete(mLength);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFile(");
    sb.append(super.toString()).append(", LENGTH: ").append(mLength);
    sb.append(", UfsPath: ").append(mUfsPath);
    sb.append(", mBlocks: ").append(mBlocks);
    return sb.toString();
  }

  @Override
  public synchronized void writeToJournal(JournalOutputStream outputStream)
      throws IOException {
    outputStream.writeEntry(new InodeFileEntry(getCreationTimeMs(), getId(), getName(),
        getParentId(), isPinned(), getLastModificationTimeMs(), getBlockSizeBytes(), getLength(),
        isComplete(), isCache(), getUfsPath(), mBlocks));
  }
}
