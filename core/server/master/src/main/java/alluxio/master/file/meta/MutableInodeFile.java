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
import alluxio.grpc.CreateFilePOptionsOrBuilder;
import alluxio.master.ProtobufUtils;
import alluxio.master.block.BlockId;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.meta.InodeMeta;
import alluxio.proto.meta.InodeMeta.InodeOrBuilder;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio file system's file representation in the file system master.
 */
@NotThreadSafe
public final class MutableInodeFile extends MutableInode<MutableInodeFile>
    implements InodeFileView {
  private List<Long> mBlocks;
  private long mBlockContainerId;
  private long mBlockSizeBytes;
  private boolean mCacheable;
  private boolean mCompleted;
  private long mLength;
  private long mPersistJobId;
  private long mShouldPersistTime;
  private int mReplicationDurable;
  private int mReplicationMax;
  private int mReplicationMin;
  private String mTempUfsPath;

  /**
   * Creates a new instance of {@link MutableInodeFile}.
   *
   * @param blockContainerId the block container id to use
   */
  private MutableInodeFile(long blockContainerId) {
    super(BlockId.createBlockId(blockContainerId, BlockId.getMaxSequenceNumber()), false);
    mBlocks = new ArrayList<>(1);
    mBlockContainerId = blockContainerId;
    mBlockSizeBytes = 0;
    mCacheable = false;
    mCompleted = false;
    mLength = 0;
    mPersistJobId = Constants.PERSISTENCE_INVALID_JOB_ID;
    mShouldPersistTime = 0;
    mReplicationDurable = 0;
    mReplicationMax = Constants.REPLICATION_MAX_INFINITY;
    mReplicationMin = 0;
    mTempUfsPath = Constants.PERSISTENCE_INVALID_UFS_PATH;
  }

  @Override
  protected MutableInodeFile getThis() {
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
    ret.setMediumTypes(getMediumTypes());
    ret.setCompleted(isCompleted());
    ret.setPersisted(isPersisted());
    ret.setBlockIds(getBlockIds());
    ret.setLastModificationTimeMs(getLastModificationTimeMs());
    ret.setLastAccessTimeMs(getLastAccessTimeMs());
    ret.setTtl(mTtl);
    ret.setTtlAction(mTtlAction);
    ret.setOwner(getOwner());
    ret.setGroup(getGroup());
    ret.setMode(getMode());
    ret.setPersistenceState(getPersistenceState().toString());
    ret.setMountPoint(false);
    ret.setReplicationMax(getReplicationMax());
    ret.setReplicationMin(getReplicationMin());
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
  public MutableInodeFile setDefaultACL(DefaultAccessControlList acl)
      throws UnsupportedOperationException {
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
  public long getPersistJobId() {
    return mPersistJobId;
  }

  @Override
  public long getShouldPersistTime() {
    return mShouldPersistTime;
  }

  @Override
  public int getReplicationDurable() {
    return mReplicationDurable;
  }

  @Override
  public int getReplicationMax() {
    return mReplicationMax;
  }

  @Override
  public int getReplicationMin() {
    return mReplicationMin;
  }

  @Override
  public String getTempUfsPath() {
    return mTempUfsPath;
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
    mBlocks.add(blockId);
    return blockId;
  }

  /**
   * @param blockSizeBytes the block size to use
   * @return the updated object
   */
  public MutableInodeFile setBlockSizeBytes(long blockSizeBytes) {
    Preconditions.checkArgument(blockSizeBytes >= 0, "Block size cannot be negative");
    mBlockSizeBytes = blockSizeBytes;
    return getThis();
  }

  /**
   * @param blockIds the id's of the block
   * @return the updated object
   */
  public MutableInodeFile setBlockIds(List<Long> blockIds) {
    mBlocks = new ArrayList<>(Preconditions.checkNotNull(blockIds, "blockIds"));
    return getThis();
  }

  /**
   * @param cacheable the cacheable flag value to use
   * @return the updated object
   */
  public MutableInodeFile setCacheable(boolean cacheable) {
    // TODO(gene). This related logic is not complete right. Fix this.
    mCacheable = cacheable;
    return getThis();
  }

  /**
   * @param completed the complete flag value to use
   * @return the updated object
   */
  public MutableInodeFile setCompleted(boolean completed) {
    mCompleted = completed;
    return getThis();
  }

  /**
   * @param length the length to use
   * @return the updated object
   */
  public MutableInodeFile setLength(long length) {
    mLength = length;
    return getThis();
  }

  /**
   * @param persistJobId the id of the job persisting this file
   * @return the updated object
   */
  public MutableInodeFile setPersistJobId(long persistJobId) {
    mPersistJobId = persistJobId;
    return getThis();
  }

  /**
   * @param shouldPersistTime the time that this file should start persisting
   * @return the updated object
   */
  public MutableInodeFile setShouldPersistTime(long shouldPersistTime) {
    mShouldPersistTime = shouldPersistTime;
    return getThis();
  }

  /**
   * @param replicationDurable the durable number of block replication
   * @return the updated object
   */
  public MutableInodeFile setReplicationDurable(int replicationDurable) {
    mReplicationDurable = replicationDurable;
    return getThis();
  }

  /**
   * @param replicationMax the maximum number of block replication
   * @return the updated object
   */
  public MutableInodeFile setReplicationMax(int replicationMax) {
    mReplicationMax = replicationMax;
    return getThis();
  }

  /**
   * @param replicationMin the minimum number of block replication
   * @return the updated object
   */
  public MutableInodeFile setReplicationMin(int replicationMin) {
    mReplicationMin = replicationMin;
    return getThis();
  }

  /**
   * @param tempUfsPath the temporary UFS path this file is persisted to
   * @return the updated object
   */
  public MutableInodeFile setTempUfsPath(String tempUfsPath) {
    mTempUfsPath = tempUfsPath;
    return getThis();
  }

  /**
   * Updates this inode file's state from the given entry.
   *
   * @param entry the entry
   */
  public void updateFromEntry(UpdateInodeFileEntry entry) {
    if (entry.hasPersistJobId()) {
      setPersistJobId(entry.getPersistJobId());
    }
    if (entry.hasReplicationMax()) {
      setReplicationMax(entry.getReplicationMax());
    }
    if (entry.hasReplicationMin()) {
      setReplicationMin(entry.getReplicationMin());
    }
    if (entry.hasTempUfsPath()) {
      setTempUfsPath(entry.getTempUfsPath());
    }
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
        .add("persistJobId", mPersistJobId)
        .add("persistenceWaitTime", mShouldPersistTime)
        .add("replicationDurable", mReplicationDurable)
        .add("replicationMax", mReplicationMax)
        .add("replicationMin", mReplicationMin)
        .add("tempUfsPath", mTempUfsPath)
        .add("length", mLength).toString();
  }

  /**
   * Converts the entry to an {@link MutableInodeFile}.
   *
   * @param entry the entry to convert
   * @return the {@link MutableInodeFile} representation
   */
  public static MutableInodeFile fromJournalEntry(InodeFileEntry entry) {
    // If journal entry has no mode set, set default mode for backwards-compatibility.
    MutableInodeFile ret = new MutableInodeFile(BlockId.getContainerId(entry.getId()))
        .setName(entry.getName())
        .setBlockIds(entry.getBlocksList())
        .setBlockSizeBytes(entry.getBlockSizeBytes())
        .setCacheable(entry.getCacheable())
        .setCompleted(entry.getCompleted())
        .setCreationTimeMs(entry.getCreationTimeMs())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs(), true)
        // for backward compatibility, set access time to modification time if it is not in journal
        .setLastAccessTimeMs(entry.hasLastAccessTimeMs()
            ? entry.getLastAccessTimeMs() : entry.getLastModificationTimeMs(), true)
        .setLength(entry.getLength())
        .setParentId(entry.getParentId())
        .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
        .setPinned(entry.getPinned())
        .setPersistJobId(entry.getPersistJobId())
        .setShouldPersistTime(entry.getShouldPersistTime())
        .setReplicationDurable(entry.getReplicationDurable())
        .setReplicationMax(entry.getReplicationMax())
        .setReplicationMin(entry.getReplicationMin())
        .setTempUfsPath(entry.getTempUfsPath())
        .setTtl(entry.getTtl())
        .setTtlAction((ProtobufUtils.fromProtobuf(entry.getTtlAction())))
        .setUfsFingerprint(entry.hasUfsFingerprint() ? entry.getUfsFingerprint() :
            Constants.INVALID_UFS_FINGERPRINT);
    if (entry.hasAcl()) {
      ret.mAcl = ProtoUtils.fromProto(entry.getAcl());
    } else {
      // Backward compatibility.
      AccessControlList acl = new AccessControlList();
      acl.setOwningUser(entry.getOwner());
      acl.setOwningGroup(entry.getGroup());
      short mode = entry.hasMode() ? (short) entry.getMode() : Constants.DEFAULT_FILE_SYSTEM_MODE;
      acl.setMode(mode);
      ret.mAcl = acl;
    }

    if (entry.getXAttrCount() > 0) {
      ret.setXAttr(CommonUtils.convertFromByteString(entry.getXAttrMap()));
    }

    ret.setMediumTypes(new HashSet<>(entry.getMediumTypeList()));
    return ret;
  }

  /**
   * Creates an {@link MutableInodeFile}.
   *
   * @param blockContainerId block container id of this inode
   * @param parentId id of the parent of this inode
   * @param name name of this inode
   * @param creationTimeMs the creation time for this inode
   * @param context context to create this file
   * @return the {@link MutableInodeFile} representation
   */
  public static MutableInodeFile create(long blockContainerId, long parentId, String name,
      long creationTimeMs, CreateFileContext context) {
    CreateFilePOptionsOrBuilder options = context.getOptions();
    Preconditions.checkArgument(
        options.getReplicationMax() == Constants.REPLICATION_MAX_INFINITY
        || options.getReplicationMax() >= options.getReplicationMin());
    return new MutableInodeFile(blockContainerId)
        .setBlockSizeBytes(options.getBlockSizeBytes())
        .setCreationTimeMs(creationTimeMs)
        .setName(name)
        .setReplicationDurable(options.getReplicationDurable())
        .setReplicationMax(options.getReplicationMax())
        .setReplicationMin(options.getReplicationMin())
        .setTtl(options.getCommonOptions().getTtl())
        .setTtlAction(options.getCommonOptions().getTtlAction())
        .setParentId(parentId)
        .setLastModificationTimeMs(context.getOperationTimeMs(), true)
        .setLastAccessTimeMs(context.getOperationTimeMs(), true)
        .setOwner(context.getOwner())
        .setGroup(context.getGroup())
        .setMode(context.getMode().toShort())
        .setAcl(context.getAcl())
        .setPersistenceState(context.isPersisted() ? PersistenceState.PERSISTED
            : PersistenceState.NOT_PERSISTED)
        .setShouldPersistTime(options.getPersistenceWaitTime() == Constants.NO_AUTO_PERSIST
            ? Constants.NO_AUTO_PERSIST :
            System.currentTimeMillis() + options.getPersistenceWaitTime())
        .setXAttr(context.getXAttr());
  }

  @Override
  public JournalEntry toJournalEntry() {
    InodeFileEntry.Builder inodeFile = InodeFileEntry.newBuilder()
        .addAllBlocks(getBlockIds())
        .addAllMediumType(getMediumTypes())
        .setBlockSizeBytes(getBlockSizeBytes())
        .setCacheable(isCacheable())
        .setCompleted(isCompleted())
        .setCreationTimeMs(getCreationTimeMs())
        .setId(getId())
        .setLastModificationTimeMs(getLastModificationTimeMs())
        .setLastAccessTimeMs(getLastAccessTimeMs())
        .setLength(getLength())
        .setName(getName())
        .setParentId(getParentId())
        .setPersistenceState(getPersistenceState().name())
        .setPinned(isPinned())
        .setReplicationDurable(getReplicationDurable())
        .setReplicationMax(getReplicationMax())
        .setReplicationMin(getReplicationMin())
        .setPersistJobId(getPersistJobId())
        .setTempUfsPath(getTempUfsPath())
        .setTtl(getTtl())
        .setTtlAction(ProtobufUtils.toProtobuf(getTtlAction()))
        .setUfsFingerprint(getUfsFingerprint())
        .setAcl(ProtoUtils.toProto(mAcl));
    if (getXAttr() != null) {
      inodeFile.putAllXAttr(CommonUtils.convertToByteString(getXAttr()));
    }
    return JournalEntry.newBuilder().setInodeFile(inodeFile).build();
  }

  @Override
  public JournalEntry toJournalEntry(String path) {
    return JournalEntry.newBuilder().setInodeFile(
        toJournalEntry().toBuilder().getInodeFileBuilder().setPath(path)).build();
  }

  @Override
  public InodeMeta.Inode toProto() {
    return super.toProtoBuilder()
        .setBlockSizeBytes(getBlockSizeBytes())
        .addAllBlocks(getBlockIds())
        .setIsCacheable(isCacheable())
        .setIsCompleted(isCompleted())
        .setLength(getLength())
        .setReplicationDurable(getReplicationDurable())
        .setReplicationMax(getReplicationMax())
        .setReplicationMin(getReplicationMin())
        .setPersistJobId(getPersistJobId())
        .setPersistJobTempUfsPath(getTempUfsPath())
        .build();
  }

  /**
   * @param inode a protocol buffer inode
   * @return the {@link MutableInodeFile} for the inode
   */
  public static MutableInodeFile fromProto(InodeOrBuilder inode) {
    MutableInodeFile f = new MutableInodeFile(BlockId.getContainerId(inode.getId()))
        .setCreationTimeMs(inode.getCreationTimeMs())
        .setLastModificationTimeMs(inode.getLastModifiedMs(), true)
        .setLastAccessTimeMs(inode.getLastAccessedMs(), true)
        .setTtl(inode.getTtl())
        .setTtlAction(inode.getTtlAction())
        .setName(inode.getName())
        .setParentId(inode.getParentId())
        .setPersistenceState(PersistenceState.valueOf(inode.getPersistenceState()))
        .setPinned(inode.getIsPinned())
        .setInternalAcl(ProtoUtils.fromProto(inode.getAccessAcl()))
        .setUfsFingerprint(inode.getUfsFingerprint())
        .setBlockSizeBytes(inode.getBlockSizeBytes())
        .setBlockIds(inode.getBlocksList())
        .setCacheable(inode.getIsCacheable())
        .setCompleted(inode.getIsCompleted())
        .setLength(inode.getLength())
        .setReplicationDurable(inode.getReplicationDurable())
        .setReplicationMax(inode.getReplicationMax())
        .setReplicationMin(inode.getReplicationMin())
        .setPersistJobId(inode.getPersistJobId())
        .setShouldPersistTime(inode.getShouldPersistTime())
        .setTempUfsPath(inode.getPersistJobTempUfsPath())
        .setMediumTypes(new HashSet<>(inode.getMediumTypeList()));
    if (inode.getXAttrCount() > 0) {
      f.setXAttr(CommonUtils.convertFromByteString(inode.getXAttrMap()));
    }
    return f;
  }
}
