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

import tachyon.master.journal.JournalEntryRepresentable;
import tachyon.security.authorization.PermissionStatus;
import tachyon.thrift.FileInfo;

/**
 * {@link Inode} is an abstract class, with information shared by all types of Inodes.
 */
public abstract class Inode implements JournalEntryRepresentable {

  /**
   * Builder for {@link Inode}.
   */
  public abstract static class Builder<T extends Builder<T>> {
    private long mCreationTimeMs;
    protected boolean mDirectory;
    protected long mId;
    private long mLastModificationTimeMs;
    private String mName;
    private long mParentId;
    private PersistenceState mPersistenceState;
    private PermissionStatus mPermissionStatus;
    private boolean mPinned;

    /**
     * Creates a new builder for {@link Inode}.
     */
    public Builder() {
      mCreationTimeMs = System.currentTimeMillis();
      mDirectory = false;
      mId = 0;
      mLastModificationTimeMs = mCreationTimeMs;
      mName = null;
      mParentId = InodeTree.NO_PARENT;
      mPersistenceState = PersistenceState.NOT_PERSISTED;
      mPinned = false;
      mPermissionStatus = null;
    }

    /**
     * @param creationTimeMs the creation time to use
     * @return the builder
     */
    public T setCreationTimeMs(long creationTimeMs) {
      mCreationTimeMs = creationTimeMs;
      return getThis();
    }

    /**
     * @param id the inode id to use
     * @return the builder
     */
    public T setId(long id) {
      mId = id;
      return getThis();
    }

    /**
     * @param lastModificationTimeMs the last modification time to use
     * @return the builder
     */
    public T setLastModificationTimeMs(long lastModificationTimeMs) {
      mLastModificationTimeMs = lastModificationTimeMs;
      return getThis();
    }

    /**
     * @param name the name to use
     * @return the builder
     */
    public T setName(String name) {
      mName = name;
      return getThis();
    }

    /**
     * @param parentId the parent id to use
     * @return the builder
     */
    public T setParentId(long parentId) {
      mParentId = parentId;
      return getThis();
    }

    /**
     * @param persistenceState the {@link PersistenceState} to use
     * @return the builder
     */
    public T setPersistenceState(PersistenceState persistenceState) {
      mPersistenceState = persistenceState;
      return getThis();
    }

    /**
     * @param ps the {@link PermissionStatus} to use
     * @return the builder
     */
    public T setPermissionStatus(PermissionStatus ps) {
      mPermissionStatus = ps;
      return getThis();
    }

    /**
     * @param pinned the pinned flag to use
     * @return the builder
     */
    public T setPinned(boolean pinned) {
      mPinned = pinned;
      return getThis();
    }

    /**
     * Builds a new instance of {@link Inode}.
     *
     * @return a {@link Inode} instance
     */
    public abstract Inode build();

    /**
     * Returns `this` so that the abstract class can use the fluent builder pattern.
     */
    protected abstract T getThis();
  }

  private final long mCreationTimeMs;

  private String mUserName;
  private String mGroupName;
  private short mPermission;

  /**
   * Indicates whether an inode is deleted or not.
   */
  private boolean mDeleted;

  protected final boolean mDirectory;

  private final long mId;

  /**
   * The last modification time of this inode, in milliseconds.
   */
  private long mLastModificationTimeMs;

  private String mName;

  private long mParentId;
  /**
   * A pinned file is never evicted from memory. Folders are not pinned in memory; however, new
   * files and folders will inherit this flag from their parents.
   */
  private boolean mPinned;

  private PersistenceState mPersistenceState;

  protected Inode(Builder<?> builder) {
    mCreationTimeMs = builder.mCreationTimeMs;
    mDeleted = false;
    mDirectory = builder.mDirectory;
    mLastModificationTimeMs = builder.mCreationTimeMs;
    mId = builder.mId;
    mLastModificationTimeMs = builder.mLastModificationTimeMs;
    mName = builder.mName;
    mPersistenceState = builder.mPersistenceState;
    mParentId = builder.mParentId;
    mPinned = builder.mPinned;
    if (builder.mPermissionStatus != null) {
      mUserName = builder.mPermissionStatus.getUserName();
      mGroupName = builder.mPermissionStatus.getGroupName();
      mPermission = builder.mPermissionStatus.getPermission().toShort();
    }
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Inode)) {
      return false;
    }
    Inode that = (Inode) o;
    return mId == that.mId;
  }

  /**
   * Generates a {@link FileInfo} of the file or folder.
   *
   * @param path The path of the file
   * @return generated FileInfo
   */
  public abstract FileInfo generateClientFileInfo(String path);

  /**
   * @return the create time, in milliseconds
   */
  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  /**
   * @return the id of the inode
   */
  public synchronized long getId() {
    return mId;
  }

  /**
   * @return the last modification time, in milliseconds
   */
  public synchronized long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  /**
   * @return the name of the inode
   */
  public synchronized String getName() {
    return mName;
  }

  /**
   * @return the {@link PersistenceState} of the inode
   */
  public synchronized PersistenceState getPersistenceState() {
    return mPersistenceState;
  }

  /**
   * @return the id of the parent folder
   */
  public synchronized long getParentId() {
    return mParentId;
  }

  @Override
  public synchronized int hashCode() {
    return ((Long) mId).hashCode();
  }

  /**
   * @return true if the inode is deleted, false otherwise
   */
  public boolean isDeleted() {
    return mDeleted;
  }

  /**
   * @return true if the inode is a directory, false otherwise
   */
  public boolean isDirectory() {
    return mDirectory;
  }

  /**
   * @return true if the inode is a file, false otherwise
   */
  public boolean isFile() {
    return !mDirectory;
  }

  /**
   * @return true if the inode is pinned, false otherwise
   */
  public synchronized boolean isPinned() {
    return mPinned;
  }

  /**
   * @return true if the file has persisted, false otherwise
   */
  public synchronized boolean isPersisted() {
    return mPersistenceState == PersistenceState.PERSISTED;
  }

  /**
   * Sets the deleted flag of the inode.
   *
   * @param deleted the deleted flag to use
   */
  public synchronized void setDeleted(boolean deleted) {
    mDeleted = deleted;
  }

  /**
   * Sets the last modification time of the inode.
   *
   * @param lastModificationTimeMs the last modification time, in milliseconds
   */
  public synchronized void setLastModificationTimeMs(long lastModificationTimeMs) {
    mLastModificationTimeMs = lastModificationTimeMs;
  }

  /**
   * Sets the name of the inode.
   *
   * @param name the new name of the inode
   */
  public synchronized void setName(String name) {
    mName = name;
  }

  /**
   * Sets the parent folder of the inode.
   *
   * @param parentId the new parent
   */
  public synchronized void setParentId(long parentId) {
    mParentId = parentId;
  }

  /**
   * Sets the {@link PersistenceState} of the file.
   *
   * @param persistenceState the {@link PersistenceState} to use
   */
  public synchronized void setPersistenceState(PersistenceState persistenceState) {
    mPersistenceState = persistenceState;
  }

  /**
   * Sets the pinned flag of the inode.
   *
   * @param pinned If true, the inode need pinned, and a pinned file is never evicted from memory
   */
  public synchronized void setPinned(boolean pinned) {
    mPinned = pinned;
  }

  /**
   * @return the user name of the inode
   */
  public synchronized String getUserName() {
    return mUserName;
  }

  /**
   * @param userName the user name of the inode
   */
  public synchronized void setUserName(String userName) {
    mUserName = userName;
  }

  /**
   * @return the group name of the inode
   */
  public synchronized String getGroupName() {
    return mGroupName;
  }

  /**
   * @param groupName the group name of the inode
   */
  public synchronized void setGroupName(String groupName) {
    mGroupName = groupName;
  }

  /**
   * @return the permission of the inode
   */
  public synchronized short getPermission() {
    return mPermission;
  }

  /**
   * @param permission the permission of the inode
   */
  public synchronized void setPermission(short permission) {
    mPermission = permission;
  }

  @Override
  public synchronized String toString() {
    return new StringBuilder("Inode(")
        .append("ID:").append(mId)
        .append(", NAME:").append(mName)
        .append(", PARENT_ID:").append(mParentId)
        .append(", CREATION_TIME_MS:").append(mCreationTimeMs)
        .append(", PINNED:").append(mPinned).append("DELETED:")
        .append(mDeleted).append(", LAST_MODIFICATION_TIME_MS:").append(mLastModificationTimeMs)
        .append(", USER_NAME:").append(mUserName).append(", GROUP_NAME:").append(mGroupName)
        .append(", PERMISSION:").append(")").toString();
  }
}
