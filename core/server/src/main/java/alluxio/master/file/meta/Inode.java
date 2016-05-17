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

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * {@link Inode} is an abstract class, with information shared by all types of Inodes. The inode
 * must be locked ({@link #lockRead()} or {@link #lockWrite()}) before methods are called.
 *
 * @param <T> the concrete subclass of this object
 */
@NotThreadSafe
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

  private final ReentrantReadWriteLock mLock;
  private final Lock mReadLock;
  private final Lock mWriteLock;

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
    mLock = new ReentrantReadWriteLock();
    mReadLock = mLock.readLock();
    mWriteLock = mLock.writeLock();
  }

  /**
   * @return the create time, in milliseconds
   */
  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  /**
   * @return the group name of the inode
   */
  public String getGroupName() {
    return mGroupName;
  }

  /**
   * @return the id of the inode
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the last modification time, in milliseconds
   */
  public long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  /**
   * @return the name of the inode
   */
  public String getName() {
    return mName;
  }

  /**
   * @return the permission of the inode
   */
  public short getPermission() {
    return mPermission;
  }

  /**
   * @return the {@link PersistenceState} of the inode
   */
  public PersistenceState getPersistenceState() {
    return mPersistenceState;
  }

  /**
   * @return the id of the parent folder
   */
  public long getParentId() {
    return mParentId;
  }

  /**
   * @return the user name of the inode
   */
  public String getUserName() {
    return mUserName;
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
  public boolean isPinned() {
    return mPinned;
  }

  /**
   * @return true if the file has persisted, false otherwise
   */
  public boolean isPersisted() {
    return mPersistenceState == PersistenceState.PERSISTED;
  }

  /**
   * @param deleted the deleted flag to use
   * @return the updated object
   */
  public T setDeleted(boolean deleted) {
    mDeleted = deleted;
    return getThis();
  }

  /**
   * @param groupName the group name of the inode
   * @return the updated object
   */
  public T setGroupName(String groupName) {
    mGroupName = groupName;
    return getThis();
  }

  /**
   * @param lastModificationTimeMs the last modification time to use
   * @return the updated object
   */
  public T setLastModificationTimeMs(long lastModificationTimeMs) {
    mLastModificationTimeMs = lastModificationTimeMs;
    return getThis();
  }

  /**
   * @param name the name to use
   * @return the updated object
   */
  public T setName(String name) {
    mName = name;
    return getThis();
  }

  /**
   * @param parentId the parent id to use
   * @return the updated object
   */
  public T setParentId(long parentId) {
    mParentId = parentId;
    return getThis();
  }

  /**
   * @param persistenceState the {@link PersistenceState} to use
   * @return the updated object
   */
  public T setPersistenceState(PersistenceState persistenceState) {
    mPersistenceState = persistenceState;
    return getThis();
  }

  /**
   * @param permissionStatus the {@link PermissionStatus} to use
   * @return the updated object
   */
  public T setPermissionStatus(PermissionStatus permissionStatus) {
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
  public T setPermission(short permission) {
    mPermission = permission;
    return getThis();
  }

  /**
   * @param pinned the pinned flag value to use
   * @return the updated object
   */
  public T setPinned(boolean pinned) {
    mPinned = pinned;
    return getThis();
  }

  /**
   * @param userName the user name of the inode
   * @return the updated object
   */
  public T setUserName(String userName) {
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

  /**
   * Acquires the read lock for this inode.
   */
  public void lockRead() {
    mReadLock.lock();
  }

  /**
   * Releases the read lock for this inode.
   */
  public void unlockRead() {
    mReadLock.unlock();
  }

  /**
   * Acquires the write lock for this inode.
   */
  public void lockWrite() {
    mWriteLock.lock();
  }

  /**
   * Releases the write lock for this inode.
   */
  public void unlockWrite() {
    mWriteLock.unlock();
  }

  /**
   * @return returns true if the current thread holds a write lock on this inode, false otherwise
   */
  public boolean isWriteLocked() {
    return mLock.isWriteLockedByCurrentThread();
  }

  /**
   * @return returns true if the current thread holds a read lock on this inode, false otherwise
   */
  public boolean isReadLocked() {
    return mLock.getReadHoldCount() > 0;
  }

  @Override
  public int hashCode() {
    return ((Long) mId).hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Inode<?>)) {
      return false;
    }
    Inode<?> that = (Inode<?>) o;
    return mId == that.mId;
  }

  protected Objects.ToStringHelper toStringHelper() {
    return Objects.toStringHelper(this).add("id", mId).add("name", mName).add("parentId", mParentId)
        .add("creationTimeMs", mCreationTimeMs).add("pinned", mPinned).add("deleted", mDeleted)
        .add("directory", mDirectory).add("persistenceState", mPersistenceState)
        .add("lastModificationTimeMs", mLastModificationTimeMs).add("userName", mUserName)
        .add("groupName", mGroupName).add("permission", mPermission);
  }
}
