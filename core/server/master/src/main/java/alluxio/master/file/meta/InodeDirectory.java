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
import alluxio.exception.InvalidPathException;
import alluxio.master.ProtobufUtils;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.UpdateInodeDirectoryEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.wire.FileInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio file system's directory representation in the file system master. The inode must be
 * locked ({@link #lockRead()} or {@link #lockWrite()}) before methods are called.
 */
@NotThreadSafe
public final class InodeDirectory extends Inode<InodeDirectory> implements InodeDirectoryView {
  // Profiling shows that most of the file lists are between 1 and 4 elements.
  // Thus allocate the corresponding ArrayLists with a small initial capacity.
  private static final int DEFAULT_FILES_PER_DIRECTORY = 2;

  private int searchChildren(String name) {
    return mChildren == null ? -1 : Collections.binarySearch(mChildren, name);
  }

  private List<Inode<?>> mChildren = null;

  private boolean mMountPoint;

  private boolean mDirectChildrenLoaded;

  private DefaultAccessControlList mDefaultAcl;

  /**
   * Creates a new instance of {@link InodeDirectory}.
   *
   * @param id the id to use
   */
  private InodeDirectory(long id) {
    super(id, true);
    mMountPoint = false;
    mDirectChildrenLoaded = false;
    mDefaultAcl = new DefaultAccessControlList(mAcl);
  }

  @Override
  protected InodeDirectory getThis() {
    return this;
  }

  /**
   * Adds the given inode to the set of children.
   *
   * @param child the inode to add
   * @return true if inode was added successfully, false otherwise
   */
  public boolean addChild(Inode<?> child) {
    final int low = searchChildren(child.getName());
    if (low >= 0) {
      return false;
    }
    addChild(child, low);
    return true;
  }

  /**
   * Add the node to the mChildren list at the given insertion point.
   * The basic add method which actually calls mChildren.add(..).
   */
  private void addChild(final Inode<?> node, final int insertionPoint) {
    if (mChildren == null) {
      mChildren = new ArrayList<>(DEFAULT_FILES_PER_DIRECTORY);
    }
    mChildren.add(-insertionPoint - 1, node);
  }

  @Override
  public Inode<?> getChild(String name) {
    final int i = searchChildren(name);
    return i < 0 ? null : mChildren.get(i);
  }

  @Override
  @Nullable
  public InodeView getChildReadLock(String name, InodeLockList lockList) throws
      InvalidPathException {
    while (true) {
      Inode child = getChild(name);
      if (child == null) {
        return null;
      }
      lockList.lockReadAndCheckParent(child, this);
      if (getChild(name) != child) {
        // The locked child has changed, so unlock and try again.
        lockList.unlockLast();
        continue;
      }
      return child;
    }
  }

  @Override
  @Nullable
  public InodeView getChildWriteLock(String name, InodeLockList lockList) throws
      InvalidPathException {
    while (true) {
      Inode child = getChild(name);
      if (child == null) {
        return null;
      }
      lockList.lockWriteAndCheckParent(child, this);
      if (getChild(name) != child) {
        // The locked child has changed, so unlock and try again.
        lockList.unlockLast();
        continue;
      }
      return child;
    }
  }

  @Override
  public Set<InodeView> getChildren() {
    return ImmutableSet.copyOf(mChildren);
  }

  @Override
  public Set<Long> getChildrenIds() {
    Set<Long> ret = new HashSet<>(mChildren.size());
    for (Inode<?> child : mChildren) {
      ret.add(child.getId());
    }
    return ret;
  }

  @Override
  public int getNumberOfChildren() {
    return mChildren.size();
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
  public synchronized boolean areDescendantsLoaded() {
    if (!isDirectChildrenLoaded()) {
      return false;
    }
    for (InodeView inode : getChildren()) {
      if (inode.isDirectory()) {
        InodeDirectory inodeDirectory = (InodeDirectory) inode;
        if (!inodeDirectory.areDescendantsLoaded()) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public DefaultAccessControlList getDefaultACL() {
    return mDefaultAcl;
  }

  /**
   * Removes the given inode from the directory.
   *
   * @param child the Inode to remove
   * @return true if the inode was removed, false otherwise
   */
  public boolean removeChild(Inode<?> child) {
    final int i = searchChildren(child.getName());
    if (i < 0) {
      return false;
    }

    final Inode<?> removed = mChildren.remove(i);
    Preconditions.checkState(removed == child);
    return true;
  }

  /**
   * Removes the given child by its name from the directory.
   *
   * @param name the name of the Inode to remove
   * @return true if the inode was removed, false otherwise
   */
  public boolean removeChild(String name) {
    Inode<?> child = getChild(name);
    return removeChild(child);
  }

  /**
   * @param mountPoint the mount point flag value to use
   * @return the updated object
   */
  public InodeDirectory setMountPoint(boolean mountPoint) {
    mMountPoint = mountPoint;
    return getThis();
  }

  /**
   * @param directChildrenLoaded whether to load the direct children if they were not loaded before
   * @return the updated object
   */
  public synchronized InodeDirectory setDirectChildrenLoaded(boolean directChildrenLoaded) {
    mDirectChildrenLoaded = directChildrenLoaded;
    return getThis();
  }

  @Override
  public InodeDirectory setDefaultACL(DefaultAccessControlList acl) {
    mDefaultAcl = acl;
    return getThis();
  }

  /**
   * Generates client file info for a folder.
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
    ret.setLength(mChildren == null ? 0 : mChildren.size());
    ret.setBlockSizeBytes(0);
    ret.setCreationTimeMs(getCreationTimeMs());
    ret.setCompleted(true);
    ret.setFolder(isDirectory());
    ret.setPinned(isPinned());
    ret.setCacheable(false);
    ret.setPersisted(isPersisted());
    ret.setLastModificationTimeMs(getLastModificationTimeMs());
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
          (DefaultAccessControlList) DefaultAccessControlList.fromProtoBuf(entry.getDefaultAcl()));
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
    return toStringHelper().add("mountPoint", mMountPoint).add("children", mChildren).toString();
  }

  /**
   * Converts the entry to an {@link InodeDirectory}.
   *
   * @param entry the entry to convert
   * @return the {@link InodeDirectory} representation
   */
  public static InodeDirectory fromJournalEntry(InodeDirectoryEntry entry) {
    // If journal entry has no mode set, set default mode for backwards-compatibility.
    InodeDirectory ret = new InodeDirectory(entry.getId())
        .setCreationTimeMs(entry.getCreationTimeMs())
        .setName(entry.getName())
        .setParentId(entry.getParentId())
        .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
        .setPinned(entry.getPinned())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs(), true)
        .setMountPoint(entry.getMountPoint())
        .setTtl(entry.getTtl())
        .setTtlAction(ProtobufUtils.fromProtobuf(entry.getTtlAction()))
        .setDirectChildrenLoaded(entry.getDirectChildrenLoaded());
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
    if (entry.hasDefaultAcl()) {
      ret.mDefaultAcl = (DefaultAccessControlList) AccessControlList
          .fromProtoBuf(entry.getDefaultAcl());
    } else {
      ret.mDefaultAcl = new DefaultAccessControlList();
    }
    return ret;
  }

  /**
   * Creates an {@link InodeDirectory}.
   *
   * @param id id of this inode
   * @param parentId id of the parent of this inode
   * @param name name of this inode
   * @param options options to create this directory
   * @return the {@link InodeDirectory} representation
   */
  public static InodeDirectory create(long id, long parentId, String name,
      CreateDirectoryOptions options) {
    return new InodeDirectory(id)
        .setParentId(parentId)
        .setName(name)
        .setTtl(options.getTtl())
        .setTtlAction(options.getTtlAction())
        .setOwner(options.getOwner())
        .setGroup(options.getGroup())
        .setMode(options.getMode().toShort())
        .setAcl(options.getAcl())
        // SetAcl call is also setting default AclEntries
        .setAcl(options.getDefaultAcl())
        .setMountPoint(options.isMountPoint());
  }

  @Override
  public JournalEntry toJournalEntry() {
    InodeDirectoryEntry inodeDirectory = InodeDirectoryEntry.newBuilder()
        .setCreationTimeMs(getCreationTimeMs())
        .setId(getId())
        .setName(getName())
        .setParentId(getParentId())
        .setPersistenceState(getPersistenceState().name())
        .setPinned(isPinned())
        .setLastModificationTimeMs(getLastModificationTimeMs())
        .setMountPoint(isMountPoint())
        .setTtl(getTtl())
        .setTtlAction(ProtobufUtils.toProtobuf(getTtlAction()))
        .setDirectChildrenLoaded(isDirectChildrenLoaded())
        .setAcl(AccessControlList.toProtoBuf(mAcl))
        .setDefaultAcl(AccessControlList.toProtoBuf(mDefaultAcl))
        .build();
    return JournalEntry.newBuilder().setInodeDirectory(inodeDirectory).build();
  }
}
