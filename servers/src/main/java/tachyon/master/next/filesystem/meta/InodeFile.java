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

package tachyon.master.next.filesystem.meta;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import tachyon.master.block.BlockId;
import tachyon.master.next.serialize.Serializer;
import tachyon.master.next.serialize.json.ImageElement;
import tachyon.master.next.serialize.json.ImageElementType;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon file system's file representation in master.
 */
public class InodeFile extends Inode {
  static class JsonSerializer implements Serializer<InodeFile> {
    @Override
    public void serialize(InodeFile o, OutputStream os) throws IOException {
      new ImageElement(ImageElementType.InodeFile)
          .withParameter("creationTimeMs", o.getCreationTimeMs()).withParameter("id", o.getId())
          .withParameter("name", o.getName()).withParameter("parentId", o.getParentId())
          .withParameter("blockSizeBytes", o.getBlockSizeBytes())
          .withParameter("length", o.getLength()).withParameter("complete", o.isComplete())
          .withParameter("pin", o.isPinned()).withParameter("cache", o.isCache())
          .withParameter("ufsPath", o.getUfsPath())
          .withParameter("lastModificationTimeMs", o.getLastModificationTimeMs())
          .dump(os);
    }

    public static InodeFile deserialize(ImageElement ele) throws IOException {
      final long creationTimeMs = ele.getLong("creationTimeMs");
      final int fileId = ele.getInt("id");
      final String fileName = ele.getString("name");
      final int parentId = ele.getInt("parentId");
      final long blockSizeByte = ele.getLong("blockSizeBytes");
      final long length = ele.getLong("length");
      final boolean isComplete = ele.getBoolean("complete");
      final boolean isPinned = ele.getBoolean("pin");
      final boolean isCache = ele.getBoolean("cache");
      final String ufsPath = ele.getString("ufsPath");
      final long lastModificationTimeMs = ele.getLong("lastModificationTimeMs");

      InodeFile inode = new InodeFile(fileName, fileId, parentId, blockSizeByte, creationTimeMs);

      try {
        inode.setLength(length);
      } catch (Exception e) {
        throw new IOException(e);
      }
      if (isComplete) {
        inode.setComplete();
      }
      inode.setPinned(isPinned);
      inode.setCache(isCache);
      inode.setUfsPath(ufsPath);
      inode.setLastModificationTimeMs(lastModificationTimeMs);
      return inode;
    }
  }

  private final static Serializer<InodeFile> mSerializer = new JsonSerializer();

  private final long mBlockContainerId;
  private final long mBlockSizeBytes;

  // list of block ids.
  private final List<Long> mBlocks;

  // length of inode file in bytes.
  private long mLength = 0;

  // The length in bytes of the last block in this file.
  private long mLastBlockSizeBytes = -1;

  private boolean mIsComplete = false;
  private boolean mCache = false;
  private String mUfsPath = "";

  /**
   * Create a new InodeFile.
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

    // The assumption is that only the last block is allowed to be smaller than the file block size.
    mLastBlockSizeBytes = mBlockSizeBytes;
  }

  /**
   * Commit a block to the file. It will check the legality. Cannot add the block if the file is
   * complete or the block's information doesn't match the file's information.
   *
   * @param blockId the id of the block to commit to this file
   * @param lengthBytes the length of the block in bytes
   * @throws BlockInfoException
   */
  public synchronized void commitBlock(long blockId, long lengthBytes) throws BlockInfoException {
    if (mIsComplete) {
      throw new BlockInfoException("The file is complete: " + this);
    }
    if (mBlocks.size() > 0 && mLastBlockSizeBytes != mBlockSizeBytes) {
      throw new BlockInfoException("Only the last file block can be less than the file block size. "
          + "file block size: " + mBlockSizeBytes + ", previous block size: " + mLastBlockSizeBytes
          + ", this block size: " + lengthBytes);
    }
    long blockIndex = BlockId.getSequenceNumber(blockId);
    if (blockIndex != mBlocks.size()) {
      throw new BlockInfoException(
          "block index mismatch: expected index: " + mBlocks.size() + " this index: " + blockIndex);
    }
    if (lengthBytes > mBlockSizeBytes) {
      throw new BlockInfoException("Block length is too large: file block size: " + mBlockSizeBytes
          + " this block size: " + lengthBytes);
    }
    mLength += lengthBytes;
    mLastBlockSizeBytes = lengthBytes;
    mBlocks.add(blockId);
  }

  @Override
  public FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();

    // TODO: make this a long.
    ret.fileId = (int) getId();
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
    return new ArrayList<Long>(mBlocks);
  }

  /**
   * Get the block size of the file
   *
   * @return the block size in bytes
   */
  public long getBlockSizeBytes() {
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
    // TODO: access the block master for this information.
    for (long blockId : mBlocks) {
    }
    return (int) (inMemoryLength * 100 / mLength);
  }

  /**
   * Get the length of the file in bytes. This is not accurate before the file is closed.
   *
   * @return the length of the file in bytes
   */
  public synchronized long getLength() {
    return mLength;
  }

  /**
   * Get the id for a new block of the file.
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
   * Get the number of the blocks of the file
   *
   * @return the number of the blocks
   */
  public synchronized int getNumberOfBlocks() {
    return mBlocks.size();
  }

  /**
   * Return whether the file has checkpointed or not. Note that the file has checkpointed only if
   * the under file system path is not empty.
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
   * Set the length of the file. Cannot set the length if the file is complete or the length is
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
      throw new SuspectedFileSizeException("InodeFile new length " + length + " is illegal.");
    }
    mLength = 0;
    while (length > 0) {
      long blockSize = Math.min(length, mBlockSizeBytes);
      commitBlock(BlockId.createBlockId(mBlockContainerId, mBlocks.size()), blockSize);
      length -= blockSize;
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

  @Override
  public void serialize(OutputStream os) throws IOException {
    mSerializer.serialize(this, os);
  }
}
