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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.master.block.BlockId;
import tachyon.master.file.journal.InodeFileEntry;
import tachyon.master.journal.JournalEntry;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Tachyon file system's file representation in the file system master.
 */
public final class InodeFile extends Inode {
  public static class Builder extends Inode.Builder {
    private long mBlockContainerId;
    private long mBlockSize;
    private long mTTL;

    public Builder() {
      super();
      mDirectory = false;
    }

    public Builder setBlockContainerId(long blockContainerId) {
      mBlockContainerId = blockContainerId;
      mId = BlockId.createBlockId(mBlockContainerId, BlockId.getMaxSequenceNumber());
      return this;
    }

    public Builder setBlockSize(long blockSize) {
      mBlockSize = blockSize;
      return this;
    }

    @Override
    public Builder setCreationTimeMs(long creationTimeMs) {
      // needed to prevent upcast when chaining
      return (Builder) super.setCreationTimeMs(creationTimeMs);
    }

    @Override
    public Builder setId(long id) {
      // id is computed based on block container id
      return this;
    }

    @Override
    public Builder setParentId(long parentId) {
      // needed to prevent upcast when chaining
      return (Builder) super.setParentId(parentId);
    }

    @Override
    public Builder setPersisted(boolean persisted) {
      // needed to prevent upcast when chaining
      return (Builder) super.setPersisted(persisted);
    }

    @Override
    public Builder setName(String name) {
      // needed to prevent upcast when chaining
      return (Builder) super.setName(name);
    }

    public Builder setTTL(long ttl) {
      mTTL = ttl;
      return this;
    }

    /**
     * Builds a new instance of {@link InodeFile}.
     *
     * @return a {@link InodeFile} instance
     */
    public InodeFile build() {
      return new InodeFile(this);
    }
  }

  private final long mBlockContainerId;
  private final long mBlockSizeBytes;
  private List<Long> mBlocks = new ArrayList<Long>(3); // list of block ids
  private boolean mCacheable = false;
  private boolean mCompleted = false;
  private long mLength = 0; // length of inode file in bytes
  private long mTTL;

  private InodeFile(InodeFile.Builder builder) {
    super(builder);
    mBlockContainerId = builder.mBlockContainerId;
    mBlockSizeBytes = builder.mBlockSize;
    mTTL = builder.mTTL;
  }

  @Override
  public FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();
    // note: in-memory percentage is NOT calculated here, because it needs blocks info stored in
    // block master
    ret.fileId = getId();
    ret.name = getName();
    ret.path = path;
    ret.length = getLength();
    ret.blockSizeBytes = getBlockSizeBytes();
    ret.creationTimeMs = getCreationTimeMs();
    ret.isCacheable = isCacheable();
    ret.isFolder = false;
    ret.isPinned = isPinned();
    ret.isCompleted = isCompleted();
    ret.isPersisted = isPersisted();
    ret.blockIds = getBlockIds();
    ret.lastModificationTimeMs = getLastModificationTimeMs();
    ret.ttl = mTTL;
    return ret;
  }

  /**
   * @return a duplication of all the block ids of the file
   */
  public synchronized List<Long> getBlockIds() {
    return new ArrayList<Long>(mBlocks);
  }

  /**
   * @return the block size in bytes
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the length of the file in bytes. This is not accurate before the file is closed.
   */
  public synchronized long getLength() {
    return mLength;
  }

  /**
   * @return the id of a new block of the file
   */
  public synchronized long getNewBlockId() {
    long blockId = BlockId.createBlockId(mBlockContainerId, mBlocks.size());
    // TODO(gene): Check for max block sequence number, and sanity check the sequence number.
    // TODO(gene): Check isComplete?
    // TODO(gene): This will not work with existing lineage implementation, since a new writer will
    // not be able to get the same block ids (to write the same block ids).
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
   * @return true if the file is cacheable, false otherwise
   */
  public synchronized boolean isCacheable() {
    return mCacheable;
  }

  /**
   * @return true if the file is complete, false otherwise
   */
  public synchronized boolean isCompleted() {
    return mCompleted;
  }

  /**
   * @param blockIds the block ids to use
   */
  public synchronized void setBlockIds(List<Long> blockIds) {
    mBlocks = Preconditions.checkNotNull(blockIds);
  }

  /**
   * @param cacheable the cacheable value to use
   */
  public synchronized void setCacheable(boolean cacheable) {
    // TODO(gene). This related logic is not complete right. Fix this.
    mCacheable = cacheable;
  }

  /**
   * The file is complete. Sets the complete flag true, and sets the length.
   *
   * @param length the length of the complete file
   */
  public synchronized void setCompleted(long length) {
    mCompleted = true;
    mLength = length;
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
    if (mCompleted) {
      throw new SuspectedFileSizeException("InodeFile has been completed.");
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
    setCompleted(mLength);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InodeFile(");
    sb.append(super.toString()).append(", LENGTH: ").append(mLength);
    sb.append(", Cacheable: ").append(mCacheable);
    sb.append(", Completed: ").append(mCompleted);
    sb.append(", Cacheable: ").append(mCacheable);
    sb.append(", mBlocks: ").append(mBlocks);
    return sb.toString();
  }

  @Override
  public synchronized JournalEntry toJournalEntry() {
    return new InodeFileEntry(getCreationTimeMs(), getId(), getName(), getParentId(), isPersisted(),
        isPinned(), getLastModificationTimeMs(), getBlockSizeBytes(), getLength(), isCompleted(),
        isCacheable(), mBlocks, mTTL);
  }

  /**
   * @return the ttl of the file
   */
  public synchronized long getTTL() {
    return mTTL;
  }
}
