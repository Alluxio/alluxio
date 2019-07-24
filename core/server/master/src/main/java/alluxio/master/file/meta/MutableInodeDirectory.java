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
import alluxio.master.ProtobufUtils;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.UpdateInodeDirectoryEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.meta.InodeMeta;
import alluxio.proto.meta.InodeMeta.InodeOrBuilder;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.FileInfo;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashSet;

/**
 * Alluxio file system's directory representation in the file system master.
 */
@NotThreadSafe
public final class MutableInodeDirectory extends MutableInode<MutableInodeDirectory>
    implements InodeDirectoryView {
  private boolean mMountPoint;
  private boolean mDirectChildrenLoaded;
  private long mChildCount;
  private DefaultAccessControlList mDefaultAcl;

  /**
   * Creates a new instance of {@link MutableInodeDirectory}.
   *
   * @param id the id to use
   */
  private MutableInodeDirectory(long id) {
    super(id, true);
    mMountPoint = false;
    mDirectChildrenLoaded = false;
    mChildCount = 0;
    mDefaultAcl = new DefaultAccessControlList(mAcl);
  }

  @Override
  protected MutableInodeDirectory getThis() {
    return this;
  }

  @Override
  public boolean isMountPoint() {
    return mMountPoint;
  }

  @Override
  public synchronized boolean isDirectChildrenLoaded() {
    return mDirectChildrenLoaded;
  }

  @Override
  public long getChildCount() {
    return mChildCount;
  }

  @Override
  public DefaultAccessControlList getDefaultACL() {
    return mDefaultAcl;
  }

  /**
   * @param mountPoint the mount point flag value to use
   * @return the updated object
   */
  public MutableInodeDirectory setMountPoint(boolean mountPoint) {
    mMountPoint = mountPoint;
    return getThis();
  }

  /**
   * @param directChildrenLoaded whether to load the direct children if they were not loaded before
   * @return the updated object
   */
  public synchronized MutableInodeDirectory setDirectChildrenLoaded(boolean directChildrenLoaded) {
    mDirectChildrenLoaded = directChildrenLoaded;
    return getThis();
  }

  /**
   * @param childCount the child count to set
   * @return the updated object
   */
  public MutableInodeDirectory setChildCount(long childCount) {
    mChildCount = childCount;
    return getThis();
  }

  @Override
  public MutableInodeDirectory setDefaultACL(DefaultAccessControlList acl) {
    mDefaultAcl = acl;
    return getThis();
  }

  /**
   * Generates client file info for a folder.
   *
   * xAttr is intentionally left out as the information is not yet exposed to clients
   *
   * @param path the path of the folder in the filesystem
   * @return the generated {@link FileInfo}
   */
  @Override
  public FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();
    ret.setFileId(getId());
    ret.setName(getName());
    ret.setPath(path);
    ret.setBlockSizeBytes(0);
    ret.setCreationTimeMs(getCreationTimeMs());
    ret.setCompleted(true);
    ret.setFolder(isDirectory());
    ret.setPinned(isPinned());
    ret.setCacheable(false);
    ret.setPersisted(isPersisted());
    ret.setLastModificationTimeMs(getLastModificationTimeMs());
    ret.setLastAccessTimeMs(getLastAccessTimeMs());
    ret.setTtl(mTtl);
    ret.setTtlAction(mTtlAction);
    ret.setOwner(getOwner());
    ret.setGroup(getGroup());
    ret.setMode(getMode());
    ret.setPersistenceState(getPersistenceState().toString());
    ret.setMountPoint(isMountPoint());
    ret.setUfsFingerprint(Constants.INVALID_UFS_FINGERPRINT);
    ret.setAcl(mAcl);
    ret.setDefaultAcl(mDefaultAcl);
    ret.setMediumTypes(getMediumTypes());
    return ret;
  }

  /**
   * Updates this inode directory's state from the given entry.
   *
   * @param entry the entry
   */
  public void updateFromEntry(UpdateInodeDirectoryEntry entry) {
    if (entry.hasDefaultAcl()) {
      setDefaultACL(
          (DefaultAccessControlList) ProtoUtils.fromProto(entry.getDefaultAcl()));
    }
    if (entry.hasDirectChildrenLoaded()) {
      setDirectChildrenLoaded(entry.getDirectChildrenLoaded());
    }
    if (entry.hasMountPoint()) {
      setMountPoint(entry.getMountPoint());
    }
  }

  @Override
  public String toString() {
    return toStringHelper().add("mountPoint", mMountPoint)
        .add("directChildrenLoaded", mDirectChildrenLoaded).toString();
  }

  /**
   * Converts the entry to an {@link MutableInodeDirectory}.
   *
   * @param entry the entry to convert
   * @return the {@link MutableInodeDirectory} representation
   */
  public static MutableInodeDirectory fromJournalEntry(InodeDirectoryEntry entry) {
    // If journal entry has no mode set, set default mode for backwards-compatibility.
    MutableInodeDirectory ret = new MutableInodeDirectory(entry.getId())
        .setCreationTimeMs(entry.getCreationTimeMs())
        .setName(entry.getName())
        .setParentId(entry.getParentId())
        .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
        .setPinned(entry.getPinned())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs(), true)
        // for backward compatibility, set access time to modification time if it is not in journal
        .setLastAccessTimeMs(entry.hasLastAccessTimeMs()
            ? entry.getLastAccessTimeMs() : entry.getLastModificationTimeMs(), true)
        .setMountPoint(entry.getMountPoint())
        .setTtl(entry.getTtl())
        .setTtlAction(ProtobufUtils.fromProtobuf(entry.getTtlAction()))
        .setDirectChildrenLoaded(entry.getDirectChildrenLoaded());
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
    if (entry.hasDefaultAcl()) {
      ret.mDefaultAcl = (DefaultAccessControlList) ProtoUtils.fromProto(entry.getDefaultAcl());
    } else {
      ret.mDefaultAcl = new DefaultAccessControlList();
    }
    ret.setMediumTypes(new HashSet<>(entry.getMediumTypeList()));
    if (entry.getXAttrCount() > 0) {
      ret.setXAttr(CommonUtils.convertFromByteString(entry.getXAttrMap()));
    }

    return ret;
  }

  /**
   * Creates an {@link MutableInodeDirectory}.
   *
   * @param id id of this inode
   * @param parentId id of the parent of this inode
   * @param name name of this inode
   * @param context context to create this directory
   * @return the {@link MutableInodeDirectory} representation
   */
  public static MutableInodeDirectory create(long id, long parentId, String name,
      CreateDirectoryContext context) {
    return new MutableInodeDirectory(id)
        .setParentId(parentId)
        .setName(name)
        .setTtl(context.getTtl())
        .setTtlAction(context.getTtlAction())
        .setOwner(context.getOwner())
        .setGroup(context.getGroup())
        .setMode(context.getMode().toShort())
        .setAcl(context.getAcl())
        // SetAcl call is also setting default AclEntries
        .setAcl(context.getDefaultAcl())
        .setMountPoint(context.isMountPoint())
        .setXAttr(context.getXAttr());
  }

  @Override
  public JournalEntry toJournalEntry() {
    InodeDirectoryEntry.Builder inodeDirectory = InodeDirectoryEntry.newBuilder()
        .setCreationTimeMs(getCreationTimeMs())
        .setId(getId())
        .setName(getName())
        .setParentId(getParentId())
        .setPersistenceState(getPersistenceState().name())
        .setPinned(isPinned())
        .setLastModificationTimeMs(getLastModificationTimeMs())
        .setLastAccessTimeMs(getLastAccessTimeMs())
        .setMountPoint(isMountPoint())
        .setTtl(getTtl())
        .setTtlAction(ProtobufUtils.toProtobuf(getTtlAction()))
        .setDirectChildrenLoaded(isDirectChildrenLoaded())
        .setAcl(ProtoUtils.toProto(mAcl))
        .setDefaultAcl(ProtoUtils.toProto(mDefaultAcl))
        .addAllMediumType(getMediumTypes());
    if (getXAttr() != null) {
      inodeDirectory.putAllXAttr(CommonUtils.convertToByteString(getXAttr()));
    }
    return JournalEntry.newBuilder().setInodeDirectory(inodeDirectory).build();
  }

  @Override
  public JournalEntry toJournalEntry(String path) {
    return JournalEntry.newBuilder().setInodeDirectory(
        toJournalEntry().toBuilder().getInodeDirectoryBuilder().setPath(path)).build();
  }

  @Override
  public InodeMeta.Inode toProto() {
    return super.toProtoBuilder()
        .setIsMountPoint(isMountPoint())
        .setHasDirectChildrenLoaded(isDirectChildrenLoaded())
        .setChildCount(getChildCount())
        .setDefaultAcl(ProtoUtils.toProto(getDefaultACL()))
        .build();
  }

  /**
   * @param inode a protocol buffer inode
   * @return the {@link MutableInodeDirectory} representation for the inode
   */
  public static MutableInodeDirectory fromProto(InodeOrBuilder inode) {
    MutableInodeDirectory d = new MutableInodeDirectory(inode.getId())
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
        .setMountPoint(inode.getIsMountPoint())
        .setDirectChildrenLoaded(inode.getHasDirectChildrenLoaded())
        .setChildCount(inode.getChildCount())
        .setDefaultACL((DefaultAccessControlList) ProtoUtils.fromProto(inode.getDefaultAcl()))
        .setMediumTypes(new HashSet<>(inode.getMediumTypeList()));
    if (inode.getXAttrCount() > 0) {
      d.setXAttr(CommonUtils.convertFromByteString(inode.getXAttrMap()));
    }
    return d;
  }
}
