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
import alluxio.collections.IndexedSet;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.PermissionStatus;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Alluxio file system's directory representation in the file system master.
 */
@ThreadSafe
public final class InodeDirectory extends Inode<InodeDirectory> {
  private IndexedSet.FieldIndex<Inode<?>> mIdIndex = new IndexedSet.FieldIndex<Inode<?>>() {
    @Override
    public Object getFieldValue(Inode<?> o) {
      return o.getId();
    }
  };

  private IndexedSet.FieldIndex<Inode<?>> mNameIndex = new IndexedSet.FieldIndex<Inode<?>>() {
    @Override
    public Object getFieldValue(Inode<?> o) {
      return o.getName();
    }
  };

  @SuppressWarnings("unchecked")
  private IndexedSet<Inode<?>> mChildren = new IndexedSet<Inode<?>>(mIdIndex, mNameIndex);

  private boolean mMountPoint;

  /**
   * Creates a new instance of {@link InodeDirectory}.
   *
   * @param id the id to use
   */
  public InodeDirectory(long id) {
    super(id);
    mDirectory = true;
    mMountPoint = false;
  }

  private InodeDirectory(long id, long creationTimeMs) {
    this(id);
    mCreationTimeMs = creationTimeMs;
  }

  @Override
  protected InodeDirectory getThis() {
    return this;
  }

  /**
   * Adds the given inode to the set of children.
   *
   * @param child the inode to add
   */
  public synchronized void addChild(Inode<?> child) {
    mChildren.add(child);
  }

  /**
   * @param id the inode id of the child
   * @return the inode with the given id, or null if there is no child with that id
   */
  public synchronized Inode<?> getChild(long id) {
    return mChildren.getFirstByField(mIdIndex, id);
  }

  /**
   * @param name the name of the child
   * @return the inode with the given name, or null if there is no child with that name
   */
  public synchronized Inode<?> getChild(String name) {
    return mChildren.getFirstByField(mNameIndex, name);
  }

  /**
   * @return an unmodifiable set of the children inodes
   */
  public synchronized Set<Inode<?>> getChildren() {
    return ImmutableSet.copyOf(mChildren.iterator());
  }

  /**
   * @return the ids of the children
   */
  public synchronized Set<Long> getChildrenIds() {
    Set<Long> ret = new HashSet<Long>(mChildren.size());
    for (Inode<?> child : mChildren) {
      ret.add(child.getId());
    }
    return ret;
  }

  /**
   * @return the number of children in the directory
   */
  public synchronized int getNumberOfChildren() {
    return mChildren.size();
  }

  /**
   * @return true if the inode is a mount point, false otherwise
   */
  public synchronized boolean isMountPoint() {
    return mMountPoint;
  }

  /**
   * Removes the given inode from the directory.
   *
   * @param child the Inode to remove
   * @return true if the inode was removed, false otherwise
   */
  public synchronized boolean removeChild(Inode<?> child) {
    return mChildren.remove(child);
  }

  /**
   * Removes the given child by its name from the directory.
   *
   * @param name the name of the Inode to remove
   * @return true if the inode was removed, false otherwise
   */
  public synchronized boolean removeChild(String name) {
    return mChildren.removeByField(mNameIndex, name);
  }

  /**
   * @param mountPoint the mount point flag value to use
   * @return the updated object
   */
  public synchronized InodeDirectory setMountPoint(boolean mountPoint) {
    mMountPoint = mountPoint;
    return this;
  }

  /**
   * Generates client file info for the folder.
   *
   * @param path the path of the folder in the filesystem
   * @return the generated {@link FileInfo}
   */
  @Override
  public synchronized FileInfo generateClientFileInfo(String path) {
    FileInfo ret = new FileInfo();
    ret.setFileId(getId());
    ret.setName(getName());
    ret.setPath(path);
    ret.setLength(mChildren.size());
    ret.setBlockSizeBytes(0);
    ret.setCreationTimeMs(getCreationTimeMs());
    ret.setCompleted(true);
    ret.setFolder(isDirectory());
    ret.setPinned(isPinned());
    ret.setCacheable(false);
    ret.setPersisted(isPersisted());
    ret.setLastModificationTimeMs(getLastModificationTimeMs());
    ret.setTtl(Constants.NO_TTL);
    ret.setUserName(getUserName());
    ret.setGroupName(getGroupName());
    ret.setPermission(getPermission());
    ret.setPersistenceState(getPersistenceState().toString());
    ret.setMountPoint(isMountPoint());
    return ret;
  }

  @Override
  public synchronized String toString() {
    return toStringHelper().add("mountPoint", mMountPoint).add("children", mChildren).toString();
  }

  /**
   * Converts the entry to an {@link InodeDirectory}.
   *
   * @param entry the entry to convert
   * @return the {@link InodeDirectory} representation
   */
  public static InodeDirectory fromJournalEntry(InodeDirectoryEntry entry) {
    PermissionStatus permissionStatus = new PermissionStatus(entry.getUserName(),
        entry.getGroupName(), (short) entry.getPermission());
    InodeDirectory inode =
        new InodeDirectory(entry.getId(), entry.getCreationTimeMs())
            .setName(entry.getName())
            .setParentId(entry.getParentId())
            .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
            .setPinned(entry.getPinned())
            .setLastModificationTimeMs(entry.getLastModificationTimeMs())
            .setPermissionStatus(permissionStatus)
            .setMountPoint(entry.getMountPoint());
    return inode;
  }

  @Override
  public synchronized JournalEntry toJournalEntry() {
    InodeDirectoryEntry inodeDirectory = InodeDirectoryEntry.newBuilder()
        .setCreationTimeMs(getCreationTimeMs())
        .setId(getId())
        .setName(getName())
        .setParentId(getParentId())
        .setPersistenceState(getPersistenceState().name())
        .setPinned(isPinned())
        .setLastModificationTimeMs(getLastModificationTimeMs())
        .setUserName(getUserName())
        .setGroupName(getGroupName())
        .setPermission(getPermission())
        .setMountPoint(isMountPoint())
        .build();
    return JournalEntry.newBuilder().setInodeDirectory(inodeDirectory).build();
  }
}
