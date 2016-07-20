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
import alluxio.collections.FieldIndex;
import alluxio.collections.IndexDefinition;
import alluxio.collections.UniqueFieldIndex;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.security.authorization.Permission;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio file system's directory representation in the file system master. The inode must be
 * locked ({@link #lockRead()} or {@link #lockWrite()}) before methods are called.
 */
@NotThreadSafe
public final class InodeDirectory extends Inode<InodeDirectory> {
  private static final IndexDefinition<Inode<?>> NAME_INDEX = new IndexDefinition<Inode<?>>(true) {
    @Override
    public Object getFieldValue(Inode<?> o) {
      return o.getName();
    }
  };

  @SuppressWarnings("unchecked")
  /** use UniqueFieldIndex directly for name index rather than using IndexedSet */
  private FieldIndex<Inode<?>> mChildren = new UniqueFieldIndex<Inode<?>>(NAME_INDEX);

  private boolean mMountPoint;

  private boolean mDirectChildrenLoaded;

  /**
   * Creates a new instance of {@link InodeDirectory}.
   *
   * @param id the id to use
   */
  private InodeDirectory(long id) {
    super(id);
    mDirectory = true;
    mMountPoint = false;
  }

  private InodeDirectory(long id, long creationTimeMs) {
    this(id);
    mCreationTimeMs = creationTimeMs;

    mDirectChildrenLoaded = false;
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
  public void addChild(Inode<?> child) {
    mChildren.add(child);
  }

  /**
   * @param name the name of the child
   * @return the inode with the given name, or null if there is no child with that name
   */
  public Inode<?> getChild(String name) {
    return mChildren.getFirst(name);
  }

  /**
   * @return an unmodifiable set of the children inodes
   */
  public Set<Inode<?>> getChildren() {
    return ImmutableSet.copyOf(mChildren.iterator());
  }

  /**
   * @return the ids of the children
   */
  public Set<Long> getChildrenIds() {
    Set<Long> ret = new HashSet<>(mChildren.size());
    for (Inode<?> child : mChildren) {
      ret.add(child.getId());
    }
    return ret;
  }

  /**
   * @return the number of children in the directory
   */
  public int getNumberOfChildren() {
    return mChildren.size();
  }

  /**
   * @return true if the inode is a mount point, false otherwise
   */
  public boolean isMountPoint() {
    return mMountPoint;
  }

  /**
   * @return true if we have loaded all the direct children's metadata once
   */
  public synchronized boolean isDirectChildrenLoaded() {
    return mDirectChildrenLoaded;
  }

  /**
   * Removes the given inode from the directory.
   *
   * @param child the Inode to remove
   * @return true if the inode was removed, false otherwise
   */
  public boolean removeChild(Inode<?> child) {
    return mChildren.remove(child);
  }

  /**
   * Removes the given child by its name from the directory.
   *
   * @param name the name of the Inode to remove
   * @return true if the inode was removed, false otherwise
   */
  public boolean removeChild(String name) {
    Inode<?> child = mChildren.getFirst(name);
    return mChildren.remove(child);
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
    ret.setOwner(getOwner());
    ret.setGroup(getGroup());
    ret.setMode(getMode());
    ret.setPersistenceState(getPersistenceState().toString());
    ret.setMountPoint(isMountPoint());
    return ret;
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
    Permission permission =
        new Permission(entry.getOwner(), entry.getGroup(), (short) entry.getMode());
    return new InodeDirectory(entry.getId(), entry.getCreationTimeMs())
        .setName(entry.getName())
        .setParentId(entry.getParentId())
        .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
        .setPinned(entry.getPinned())
        .setLastModificationTimeMs(entry.getLastModificationTimeMs())
        .setPermission(permission)
        .setMountPoint(entry.getMountPoint())
        .setDirectChildrenLoaded(entry.getDirectChildrenLoaded());
  }

  /**
   * Creates an {@link InodeDirectory}.
   *
   * @param id id of this inode
   * @param parentId id of the parent of this inode
   * @param name name of this inode
   * @param directoryOptions options to create this directory
   * @return the {@link InodeDirectory} representation
   */
  public static InodeDirectory create(long id, long parentId, String name,
      CreateDirectoryOptions directoryOptions) {
    Permission permission = new Permission(directoryOptions.getPermission()).applyDirectoryUMask();
    return new InodeDirectory(id)
        .setParentId(parentId)
        .setName(name)
        .setPermission(permission)
        .setMountPoint(directoryOptions.isMountPoint());
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
        .setDirectChildrenLoaded(isDirectChildrenLoaded())
        .setOwner(getOwner()).setGroup(getGroup()).setMode(getMode())
        .build();
    return JournalEntry.newBuilder().setInodeDirectory(inodeDirectory).build();
  }
}
