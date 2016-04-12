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

import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.security.authorization.PermissionStatus;
import alluxio.wire.FileInfo;

import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link Inode} is an abstract class, with information shared by all types of Inodes.
 *
 * @param <T> the concrete subclass of this object
 */
@ThreadSafe
public abstract class Inode<T> implements JournalEntryRepresentable {
  protected long mCreationTimeMs;
  private boolean mDeleted;
  protected boolean mDirectory;
  private String mGroupName;
  protected long mId;
  private long mLastModificationTimeMs;
  private String mName;
  private long mParentId;
  private short mPermission;
  private PersistenceState mPersistenceState;
  private boolean mPinned;
  private String mUserName;

  protected Inode(long id) {
    mCreationTimeMs = System.currentTimeMillis();
    mDeleted = false;
    mDirectory = false;
    mGroupName = "";
    mId = id;
    mLastModificationTimeMs = mCreationTimeMs;
    mName = null;
    mParentId = InodeTree.NO_PARENT;
    mPermission = 0;
    mPersistenceState = PersistenceState.NOT_PERSISTED;
    mPinned = false;
    mUserName = "";
  }

  /**
   * @return the create time, in milliseconds
   */
  public synchronized long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  /**
   * @return the group name of the inode
   */
  public synchronized String getGroupName() {
    return mGroupName;
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
   * @return the permission of the inode
   */
  public synchronized short getPermission() {
    return mPermission;
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

  /**
   * @return the user name of the inode
   */
  public synchronized String getUserName() {
    return mUserName;
  }

  /**
   * @return true if the inode is deleted, false otherwise
   */
  public synchronized boolean isDeleted() {
    return mDeleted;
  }

  /**
   * @return true if the inode is a directory, false otherwise
   */
  public synchronized boolean isDirectory() {
    return mDirectory;
  }

  /**
   * @return true if the inode is a file, false otherwise
   */
  public synchronized boolean isFile() {
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
   * @param deleted the deleted flag to use
   * @return the updated object
   */
  public synchronized T setDeleted(boolean deleted) {
    mDeleted = deleted;
    return getThis();
  }

  /**
   * @param groupName the group name of the inode
   * @return the updated object
   */
  public synchronized T setGroupName(String groupName) {
    mGroupName = groupName;
    return getThis();
  }

  /**
   * @param lastModificationTimeMs the last modification time to use
   * @return the updated object
   */
  public synchronized T setLastModificationTimeMs(long lastModificationTimeMs) {
    mLastModificationTimeMs = lastModificationTimeMs;
    return getThis();
  }

  /**
   * @param name the name to use
   * @return the updated object
   */
  public synchronized T setName(String name) {
    mName = name;
    return getThis();
  }

  /**
   * @param parentId the parent id to use
   * @return the updated object
   */
  public synchronized T setParentId(long parentId) {
    mParentId = parentId;
    return getThis();
  }

  /**
   * @param persistenceState the {@link PersistenceState} to use
   * @return the updated object
   */
  public synchronized T setPersistenceState(PersistenceState persistenceState) {
    mPersistenceState = persistenceState;
    return getThis();
  }

  /**
   * @param permissionStatus the {@link PermissionStatus} to use
   * @return the updated object
   */
  public synchronized T setPermissionStatus(PermissionStatus permissionStatus) {
    if (permissionStatus != null) {
      mGroupName = permissionStatus.getGroupName();
      mPermission = permissionStatus.getPermission().toShort();
      mUserName = permissionStatus.getUserName();
    }
    return getThis();
  }

  /**
   * @param permission the permission of the inode
   * @return the updated object
   */
  public synchronized T setPermission(short permission) {
    mPermission = permission;
    return getThis();
  }

  /**
   * @param pinned the pinned flag value to use
   * @return the updated object
   */
  public synchronized T setPinned(boolean pinned) {
    mPinned = pinned;
    return getThis();
  }

  /**
   * @param userName the user name of the inode
   * @return the updated object
   */
  public synchronized T setUserName(String userName) {
    mUserName = userName;
    return getThis();
  }

  /**
   * Generates a {@link FileInfo} of the file or folder.
   *
   * @param path the path of the file
   * @return generated {@link FileInfo}
   */
  public abstract FileInfo generateClientFileInfo(String path);

  /**
   * @return {@code this} so that the abstract class can use the fluent builder pattern
   */
  protected abstract T getThis();

  @Override
  public synchronized int hashCode() {
    return ((Long) mId).hashCode();
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Inode<?>)) {
      return false;
    }
    Inode<?> that = (Inode<?>) o;
    return mId == that.mId;
  }

  protected synchronized Objects.ToStringHelper toStringHelper() {
    return Objects.toStringHelper(this).add("id", mId).add("name", mName).add("parentId", mParentId)
        .add("creationTimeMs", mCreationTimeMs).add("pinned", mPinned).add("deleted", mDeleted)
        .add("directory", mDirectory).add("persistenceState", mPersistenceState)
        .add("lastModificationTimeMs", mLastModificationTimeMs).add("userName", mUserName)
        .add("groupName", mGroupName).add("permission", mPermission);
  }
}
