/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import alluxio.Constants;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.master.block.BlockId;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.FileSystemPermission;
import alluxio.security.authorization.PermissionStatus;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio file system's file representation in the file system master.
 */
@ThreadSafe
public final class InodeFile extends Inode<InodeFile> {
  /** This default umask is used to calculate file permission from directory permission. */
  private static final FileSystemPermission UMASK =
      new FileSystemPermission(Constants.FILE_DIR_PERMISSION_DIFF);

  private List<Long> mBlocks;
  private long mBlockContainerId;
  private long mBlockSizeBytes;
  private boolean mCacheable;
  private boolean mCompleted;
  private long mLength;
  private long mTtl;

  /**
   * Creates a new instance of {@link InodeFile}.
   *
   * @param id the block container id to use
   */
  public InodeFile(long id) {
    super(0);
    mBlocks = new ArrayList<Long>(3);
    mBlockContainerId = id;
    mBlockSizeBytes = 0;
    mCacheable = false;
    mCompleted = false;
    mDirectory = false;
    mId = BlockId.createBlockId(mBlockContainerId, BlockId.getMaxSequenceNumber());
    mLength = 0;
    mTtl = Constants.NO_TTL;
  }

  private InodeFile(long id, long creationTimeMs) {
    this(id);
    mCreationTimeMs = creationTimeMs;
  }

  @Override
  protected InodeFile getThis() {
    return this;
  }

  @Override
  public synchronized FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();
    // note: in-memory percentage is NOT calculated here, because it needs blocks info stored in
    // block master
    ret.setFileId(getId());
    ret.setName(getName());
    ret.setPath(path);
    ret.setLength(getLength());
    ret.setBlockSizeBytes(getBlockSizeBytes());
    ret.setCreationTimeMs(getCreationTimeMs());
    ret.setCacheable(isCacheable());
    ret.setFolder(isDirectory());
    ret.setPinned(isPinned());
    ret.setCompleted(isCompleted());
    ret.setPersisted(isPersisted());
    ret.setBlockIds(getBlockIds());
    ret.setLastModificationTimeMs(getLastModificationTimeMs());
    ret.setTtl(mTtl);
    ret.setUserName(getUserName());
    ret.setGroupName(getGroupName());
    ret.setPermission(getPermission());
    ret.setPersistenceState(getPersistenceState().toString());
    ret.setMountPoint(false);
    return ret;
  }

  /**
   * Resets the file inode.
   */
  public synchronized void reset() {
    mBlocks = Lists.newArrayList();
    mLength = 0;
    mCompleted = false;
    mCacheable = false;
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
  public synchronized long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the length of the file in bytes. This is not accurate before the file is closed
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

  /**
   * Gets the block id for a given index.
   *
   * @param blockIndex the index to get the block id for
   * @return the block id for the index
   * @throws BlockInfoException if the index of the block is out of range
   */
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
   * @param blockSizeBytes the block size to use
   * @return the updated object
   */
  public synchronized InodeFile setBlockSizeBytes(long blockSizeBytes) {
    Preconditions.checkArgument(blockSizeBytes >= 0, "Block size cannot be negative");
    mBlockSizeBytes = blockSizeBytes;
    return getThis();
  }

  /**
   * @param blockIds the id's of the block
   * @return the updated object
   */
  public synchronized InodeFile setBlockIds(List<Long> blockIds) {
    mBlocks = Lists.newArrayList(Preconditions.checkNotNull(blockIds));
    return getThis();
  }

  /**
   * @param cacheable the cacheable flag value to use
   * @return the updated object
   */
  public synchronized InodeFile setCacheable(boolean cacheable) {
    // TODO(gene). This related logic is not complete right. Fix this.
    mCacheable = cacheable;
    return getThis();
  }

  /**
   * @param completed the complete flag value to use
   * @return the updated object
   */
  public synchronized InodeFile setCompleted(boolean completed) {
    mCompleted = completed;
    return getThis();
  }

  /**
   * @param length the length to use
   * @return the updated object
   */
  public synchronized InodeFile setLength(long length) {
    mLength = length;
    return getThis();
  }

  @Override
  public InodeFile setPermissionStatus(PermissionStatus permissionStatus) {
    Preconditions.checkNotNull(permissionStatus, "Permission status is not set");
    return super.setPermissionStatus(permissionStatus.applyUMask(UMASK));
  }

  /**
   * @param ttl the TTL to use, in milliseconds
   * @return the updated object
   */
  public synchronized InodeFile setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  /**
   * Completes the file. Cannot set the length if the file is already completed or the length is
   * negative.
   *
   * @param length The new length of the file, cannot be negative
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   */
  public synchronized void complete(long length)
      throws InvalidFileSizeException, FileAlreadyCompletedException {
    if (mCompleted) {
      throw new FileAlreadyCompletedException("File " + getName() + " has already been completed.");
    }
    if (length < 0) {
      throw new InvalidFileSizeException("File " + getName() + " cannot have negative length.");
    }
    mCompleted = true;
    mLength = length;
    mBlocks.clear();
    while (length > 0) {
      long blockSize = Math.min(length, mBlockSizeBytes);
      getNewBlockId();
      length -= blockSize;
    }
  }

  @Override
  public synchronized String toString() {
    return toStringHelper().add("blocks", mBlocks).add("blockContainerId", mBlockContainerId)
        .add("blockSizeBytes", mBlockSizeBytes).add("cacheable", mCacheable)
        .add("completed", mCompleted).add("length", mLength).add("ttl", mTtl).toString();
  }

  /**
   * Converts the entry to an {@link InodeFile}.
   *
   * @param entry the entry to convert
   * @return the {@link InodeFile} representation
   */
  public static InodeFile fromJournalEntry(InodeFileEntry entry) {
    PermissionStatus permissionStatus = new PermissionStatus(entry.getUserName(),
        entry.getGroupName(), (short) entry.getPermission());
    InodeFile inode =
        new InodeFile(BlockId.getContainerId(entry.getId()), entry.getCreationTimeMs())
            .setName(entry.getName())
            .setBlockIds(entry.getBlocksList())
            .setBlockSizeBytes(entry.getBlockSizeBytes())
            .setCacheable(entry.getCacheable())
            .setCompleted(entry.getCompleted())
            .setLastModificationTimeMs(entry.getLastModificationTimeMs())
            .setLength(entry.getLength())
            .setParentId(entry.getParentId())
            .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
            .setPinned(entry.getPinned())
            .setTtl(entry.getTtl())
            .setPermissionStatus(permissionStatus);
    return inode;
  }

  @Override
  public synchronized JournalEntry toJournalEntry() {
    InodeFileEntry inodeFile = InodeFileEntry.newBuilder()
        .setCreationTimeMs(getCreationTimeMs())
        .setId(getId())
        .setName(getName())
        .setParentId(getParentId())
        .setPersistenceState(getPersistenceState().name())
        .setPinned(isPinned())
        .setLastModificationTimeMs(getLastModificationTimeMs())
        .setBlockSizeBytes(getBlockSizeBytes())
        .setLength(getLength())
        .setCompleted(isCompleted())
        .setCacheable(isCacheable())
        .addAllBlocks(mBlocks)
        .setTtl(mTtl)
        .setUserName(getUserName())
        .setGroupName(getGroupName())
        .setPermission(getPermission())
        .build();
    return JournalEntry.newBuilder().setInodeFile(inodeFile).build();
  }

  /**
   * @return the ttl of the file
   */
  public synchronized long getTtl() {
    return mTtl;
  }
}
