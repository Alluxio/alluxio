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

package alluxio.master.file.meta;

import alluxio.Constants;
import alluxio.exception.BlockInfoException;
import alluxio.master.ProtobufUtils;
import alluxio.master.block.BlockId;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio file system's file representation in the file system master. The inode must be locked
 * ({@link #lockRead()} or {@link #lockWrite()}) before methods are called.
 */
@NotThreadSafe
public final class InodeFile extends Inode<InodeFile> implements InodeFileView {
  private List<Long> mBlocks;
  private long mBlockContainerId;
  private long mBlockSizeBytes;
  private boolean mCacheable;
  private boolean mCompleted;
  private long mLength;

  /**
   * Creates a new instance of {@link InodeFile}.
   *
   * @param blockContainerId the block container id to use
   */
  private InodeFile(long blockContainerId) {
    super(BlockId.createBlockId(blockContainerId, BlockId.getMaxSequenceNumber()), false);
    mBlocks = new ArrayList<>(1);
    mBlockContainerId = blockContainerId;
    mBlockSizeBytes = 0;
    mCacheable = false;
    mCompleted = false;
    mLength = 0;
  }

  @Override
  protected InodeFile getThis() {
    return this;
  }

  @Override
  public FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();
    // note: in-Alluxio percentage is NOT calculated here, because it needs blocks info stored in
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
    ret.setTtlAction(mTtlAction);
    ret.setOwner(getOwner());
    ret.setGroup(getGroup());
    ret.setMode(getMode());
    ret.setPersistenceState(getPersistenceState().toString());
    ret.setMountPoint(false);
    ret.setUfsFingerprint(getUfsFingerprint());
    ret.setAcl(mAcl);
    return ret;
  }

  /**
   * Resets the file inode.
   */
  public void reset() {
    mBlocks = new ArrayList<>();
    mLength = 0;
    mCompleted = false;
    mCacheable = false;
  }

  @Override
  public DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("getDefaultACL: File does not have default ACL");
  }

  @Override
  public void setDefaultACL(DefaultAccessControlList acl) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("setDefaultACL: File does not have default ACL");
  }

  @Override
  public List<Long> getBlockIds() {
    return new ArrayList<>(mBlocks);
  }

  @Override
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  @Override
  public long getLength() {
    return mLength;
  }

  @Override
  public long getBlockContainerId() {
    return mBlockContainerId;
  }

  @Override
  public long getBlockIdByIndex(int blockIndex) throws BlockInfoException {
    if (blockIndex < 0 || blockIndex >= mBlocks.size()) {
      throw new BlockInfoException(
          "blockIndex " + blockIndex + " is out of range. File blocks: " + mBlocks.size());
    }
    return mBlocks.get(blockIndex);
  }

  @Override
  public boolean isCacheable() {
    return mCacheable;
  }

  @Override
  public boolean isCompleted() {
    return mCompleted;
  }

  /**
   * @return the id of a new block of the file
   */
  public long getNewBlockId() {
    long blockId = BlockId.createBlockId(mBlockContainerId, mBlocks.size());
    // TODO(gene): Check for max block sequence number, and sanity check the sequence number.
    // TODO(gene): Check isComplete?
    // TODO(gene): This will not work with existing lineage implementation, since a new writer will
    // not be able to get the same block ids (to write the same block ids).
    mBlocks.add(blockId);
    return blockId;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated object
   */
  public InodeFile setBlockSizeBytes(long blockSizeBytes) {
    Preconditions.checkArgument(blockSizeBytes >= 0, "Block size cannot be negative");
    mBlockSizeBytes = blockSizeBytes;
    return getThis();
  }

  /**
   * @param blockIds the id's of the block
   * @return the updated object
   */
  public InodeFile setBlockIds(List<Long> blockIds) {
    mBlocks = new ArrayList<>(Preconditions.checkNotNull(blockIds, "blockIds"));
    return getThis();
  }

  /**
   * @param cacheable the cacheable flag value to use
   * @return the updated object
   */
  public InodeFile setCacheable(boolean cacheable) {
    // TODO(gene). This related logic is not complete right. Fix this.
    mCacheable = cacheable;
    return getThis();
  }

  /**
   * @param completed the complete flag value to use
   * @return the updated object
   */
  public InodeFile setCompleted(boolean completed) {
    mCompleted = completed;
    return getThis();
  }

  /**
   * @param length the length to use
   * @return the updated object
   */
  public InodeFile setLength(long length) {
    mLength = length;
    return getThis();
  }

  /**
   * Updates this inode file's state from the given entry.
   *
   * @param entry the entry
   */
  public void updateFromEntry(UpdateInodeFileEntry entry) {
    if (entry.hasBlockSizeBytes()) {
      setBlockSizeBytes(entry.getBlockSizeBytes());
    }
    if (entry.hasCacheable()) {
      setCacheable(entry.getCacheable());
    }
    if (entry.hasCompleted()) {
      setCompleted(entry.getCompleted());
    }
    if (entry.hasLength()) {
      setLength(entry.getLength());
    }
    if (entry.getSetBlocksCount() > 0) {
      setBlockIds(entry.getSetBlocksList());
    }
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("blocks", mBlocks)
        .add("blockContainerId", mBlockContainerId)
        .add("blockSizeBytes", mBlockSizeBytes)
        .add("cacheable", mCacheable)
        .add("completed", mCompleted)
        .add("length", mLength).toString();
  }

  /**
   * Converts the entry to an {@link InodeFile}.
   *
   * @param entry the entry to convert
   * @return the {@link InodeFile} representation
   */
  public static InodeFile fromJournalEntry(InodeFileEntry entry) {
    // If journal entry has no mode set, set default mode for backwards-compatibility.
    InodeFile ret = new InodeFile(BlockId.getContainerId(entry.getId()))
        .setName(entry.getName())
        .setBlockIds(entry.getBlocksList())
        .setBlockSizeBytes(entry.getBlockSizeBytes())
        .setCacheable(entry.getCacheable())
        .setCompleted(entry.getCompleted())
        .setCreationTimeMs(entry.getCreationTimeMs())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs(), true)
        .setLength(entry.getLength())
        .setParentId(entry.getParentId())
        .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
        .setPinned(entry.getPinned())
        .setTtl(entry.getTtl())
        .setTtlAction((ProtobufUtils.fromProtobuf(entry.getTtlAction())))
        .setUfsFingerprint(entry.hasUfsFingerprint() ? entry.getUfsFingerprint() :
            Constants.INVALID_UFS_FINGERPRINT);
    if (entry.hasAcl()) {
      ret.mAcl = AccessControlList.fromProtoBuf(entry.getAcl());
    } else {
      // Backward compatibility.
      AccessControlList acl = new AccessControlList();
      acl.setOwningUser(entry.getOwner());
      acl.setOwningGroup(entry.getGroup());
      short mode = entry.hasMode() ? (short) entry.getMode() : Constants.DEFAULT_FILE_SYSTEM_MODE;
      acl.setMode(mode);
      ret.mAcl = acl;
    }
    return ret;
  }

  /**
   * Creates an {@link InodeFile}.
   *
   * @param blockContainerId block container id of this inode
   * @param parentId id of the parent of this inode
   * @param name name of this inode
   * @param creationTimeMs the creation time for this inode
   * @param options options to create this file
   * @return the {@link InodeFile} representation
   */
  public static InodeFile create(long blockContainerId, long parentId, String name,
      long creationTimeMs, CreateFileOptions options) {
    return new InodeFile(blockContainerId)
        .setBlockSizeBytes(options.getBlockSizeBytes())
        .setCreationTimeMs(creationTimeMs)
        .setName(name)
        .setTtl(options.getTtl())
        .setTtlAction(options.getTtlAction())
        .setParentId(parentId)
        .setLastModificationTimeMs(options.getOperationTimeMs(), true)
        .setOwner(options.getOwner())
        .setGroup(options.getGroup())
        .setMode(options.getMode().toShort())
        .setAcl(options.getAcl())
        .setPersistenceState(options.isPersisted() ? PersistenceState.PERSISTED
            : PersistenceState.NOT_PERSISTED);

  }

  @Override
  public JournalEntry toJournalEntry() {
    InodeFileEntry inodeFile = InodeFileEntry.newBuilder()
        .addAllBlocks(getBlockIds())
        .setBlockSizeBytes(getBlockSizeBytes())
        .setCacheable(isCacheable())
        .setCompleted(isCompleted())
        .setCreationTimeMs(getCreationTimeMs())
        .setId(getId())
        .setLastModificationTimeMs(getLastModificationTimeMs())
        .setLength(getLength())
        .setName(getName())
        .setParentId(getParentId())
        .setPersistenceState(getPersistenceState().name())
        .setPinned(isPinned())
        .setTtl(getTtl())
        .setTtlAction(ProtobufUtils.toProtobuf(getTtlAction()))
        .setUfsFingerprint(getUfsFingerprint())
        .setAcl(AccessControlList.toProtoBuf(mAcl))
        .build();
    return JournalEntry.newBuilder().setInodeFile(inodeFile).build();
  }
}
