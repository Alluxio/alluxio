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

import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableSet;

import tachyon.Constants;
import tachyon.collections.IndexedSet;
import tachyon.proto.journal.File.InodeDirectoryEntry;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.security.authorization.PermissionStatus;
import tachyon.thrift.FileInfo;

/**
 * Tachyon file system's directory representation in the file system master.
 */
@ThreadSafe
public final class InodeDirectory extends Inode {

  /**
   * Builder for {@link InodeDirectory}.
   */
  public static class Builder extends Inode.Builder<InodeDirectory.Builder> {

    /**
     * Creates a new builder for {@link InodeDirectory}.
     */
    public Builder() {
      super();
      mDirectory = true;
    }

    /**
     * Builds a new instance of {@link InodeDirectory}.
     *
     * @return a {@link InodeDirectory} instance
     */
    @Override
    public InodeDirectory build() {
      return new InodeDirectory(this);
    }

    @Override
    protected Builder getThis() {
      return this;
    }
  }

  private IndexedSet.FieldIndex<Inode> mIdIndex = new IndexedSet.FieldIndex<Inode>() {
    @Override
    public Object getFieldValue(Inode o) {
      return o.getId();
    }
  };

  private IndexedSet.FieldIndex<Inode> mNameIndex = new IndexedSet.FieldIndex<Inode>() {
    @Override
    public Object getFieldValue(Inode o) {
      return o.getName();
    }
  };

  @SuppressWarnings("unchecked")
  private IndexedSet<Inode> mChildren = new IndexedSet<Inode>(mIdIndex, mNameIndex);

  private InodeDirectory(InodeDirectory.Builder builder) {
    super(builder);
  }

  /**
   * Adds the given inode to the set of children.
   *
   * @param child the inode to add
   */
  public synchronized void addChild(Inode child) {
    mChildren.add(child);
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
    ret.fileId = getId();
    ret.name = getName();
    ret.path = path;
    ret.length = 0;
    ret.blockSizeBytes = 0;
    ret.creationTimeMs = getCreationTimeMs();
    ret.isCompleted = true;
    ret.isFolder = true;
    ret.isPinned = isPinned();
    ret.isCacheable = false;
    ret.isPersisted = isPersisted();
    ret.blockIds = null;
    ret.lastModificationTimeMs = getLastModificationTimeMs();
    ret.ttl = Constants.NO_TTL;
    ret.userName = getUserName();
    ret.groupName = getGroupName();
    ret.permission = getPermission();
    ret.persistenceState = getPersistenceState().toString();
    return ret;
  }

  /**
   * @param id the inode id of the child
   * @return the inode with the given id, or null if there is no child with that id
   */
  public synchronized Inode getChild(long id) {
    return mChildren.getFirstByField(mIdIndex, id);
  }

  /**
   * @param name the name of the child
   * @return the inode with the given name, or null if there is no child with that name
   */
  public synchronized Inode getChild(String name) {
    return mChildren.getFirstByField(mNameIndex, name);
  }

  /**
   * @return an unmodifiable set of the children inodes
   */
  public synchronized Set<Inode> getChildren() {
    return ImmutableSet.copyOf(mChildren.iterator());
  }

  /**
   * @return the ids of the children
   */
  public synchronized Set<Long> getChildrenIds() {
    Set<Long> ret = new HashSet<Long>(mChildren.size());
    for (Inode child : mChildren) {
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
   * Removes the given inode from the directory.
   *
   * @param child the Inode to remove
   * @return true if the inode was removed, false otherwise
   */
  public synchronized boolean removeChild(Inode child) {
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

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("InodeDirectory(");
    sb.append(super.toString()).append(",").append(getChildren()).append(")");
    return sb.toString();
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
        new InodeDirectory.Builder()
            .setName(entry.getName())
            .setId(entry.getId())
            .setParentId(entry.getParentId())
            .setCreationTimeMs(entry.getCreationTimeMs())
            .setPersistenceState(PersistenceState.valueOf(entry.getPersistenceState()))
            .setPinned(entry.getPinned())
            .setLastModificationTimeMs(entry.getLastModificationTimeMs())
            .setPermissionStatus(permissionStatus)
            .build();
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
        .build();
    return JournalEntry.newBuilder().setInodeDirectory(inodeDirectory).build();
  }
}
