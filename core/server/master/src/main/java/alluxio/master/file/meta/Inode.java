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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclActions;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.wire.FileInfo;
import alluxio.wire.TtlAction;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
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
  private static final Logger LOG = LoggerFactory.getLogger(Inode.class);
  protected long mCreationTimeMs;
  private boolean mDeleted;
  protected final boolean mDirectory;
  protected final long mId;
  protected long mTtl;
  protected TtlAction mTtlAction;
  private long mLastModificationTimeMs;
  private String mName;
  private long mParentId;
  private PersistenceState mPersistenceState;
  private boolean mPinned;
  protected AccessControlList mAcl;
  private String mUfsFingerprint;

  private final ReentrantReadWriteLock mLock;

  protected Inode(long id, boolean isDirectory) {
    mCreationTimeMs = System.currentTimeMillis();
    mDeleted = false;
    mDirectory = isDirectory;
    mId = id;
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
    mLastModificationTimeMs = mCreationTimeMs;
    mName = null;
    mParentId = InodeTree.NO_PARENT;
    mPersistenceState = PersistenceState.NOT_PERSISTED;
    mPinned = false;
    mAcl = new AccessControlList();
    mUfsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
    mLock = new ReentrantReadWriteLock();
  }

  /**
   * @return the create time, in milliseconds
   */
  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  /**
   * @return the group of the inode
   */
  public String getGroup() {
    return mAcl.getOwningGroup();
  }

  /**
   * @return the id of the inode
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the ttl of the file
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return the {@link TtlAction}
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
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
   * @return the mode of the inode
   */
  public short getMode() {
    return mAcl.getMode();
  }

  /**
   * @return the {@link PersistenceState} of the inode
   */
  public PersistenceState getPersistenceState() {
    return mPersistenceState;
  }

  /**
   * Compare-and-swaps the persistence state.
   *
   * @param oldState the old {@link PersistenceState}
   * @param newState the new {@link PersistenceState} to set
   * @return true if the old state matches and the new state was set
   */
  public boolean compareAndSwap(PersistenceState oldState, PersistenceState newState) {
    synchronized (this) {
      if (mPersistenceState == oldState) {
        mPersistenceState = newState;
        return true;
      }
      return false;
    }
  }

  /**
   * @return the id of the parent folder
   */
  public long getParentId() {
    return mParentId;
  }

  /**
   * @return the owner of the inode
   */
  public String getOwner() {
    return mAcl.getOwningUser();
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
   * @return the UFS fingerprint
   */
  public String getUfsFingerprint() {
    return mUfsFingerprint;
  }

  /**
   * Removes the extended ACL entries. The base entries are retained.
   *
   * @return the updated object
   */
  public T removeExtendedAcl() {
    mAcl.removeExtendedEntries();
    return getThis();
  }

  /**
   * Removes ACL entries.
   *
   * @param entries the ACL entries to remove
   * @return the updated object
   */
  public T removeAcl(List<AclEntry> entries) throws IOException {
    for (AclEntry entry : entries) {
      if (entry.isDefault()) {
        AccessControlList defaultAcl = getDefaultACL();
        defaultAcl.removeEntry(entry);
      } else {
        mAcl.removeEntry(entry);
      }
    }
    return getThis();
  }

  /**
   * Replaces all existing ACL entries with a new list of entries.
   *
   * @param entries the new list of ACL entries
   * @return the updated object
   */
  public T replaceAcl(List<AclEntry> entries) {
    boolean clearACL = false;
    for (AclEntry entry : entries) {
      /**
       * if we are only setting default ACLs, we do not need to clear access ACL entries
       * observed same behavior on linux
       */
      if (!entry.isDefault()) {
        clearACL = true;
      }
    }
    if (clearACL) {
      mAcl.clearEntries();
    }
    return setAcl(entries);
  }

  /**
   * @param creationTimeMs the creation time to use (in milliseconds)
   * @return the updated object
   */
  public T setCreationTimeMs(long creationTimeMs) {
    mCreationTimeMs = creationTimeMs;
    return getThis();
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
   * @param group the group of the inode
   * @return the updated object
   */
  public T setGroup(String group) {
    mAcl.setOwningGroup(group);
    return getThis();
  }

  /**
   * Sets the last modification time of the inode to the new time if the new time is more recent.
   * This method can be called concurrently with deterministic results.
   *
   * @param lastModificationTimeMs the last modification time to use
   * @return the updated object
   */
  public T setLastModificationTimeMs(long lastModificationTimeMs) {
    return setLastModificationTimeMs(lastModificationTimeMs, false);
  }

  /**
   * @param lastModificationTimeMs the last modification time to use
   * @param override if true, sets the value regardless of the previous last modified time,
   *                 should be set to true for journal replay
   * @return the updated object
   */
  public T setLastModificationTimeMs(long lastModificationTimeMs, boolean override) {
    synchronized (this) {
      if (override || mLastModificationTimeMs < lastModificationTimeMs) {
        mLastModificationTimeMs = lastModificationTimeMs;
      }
      return getThis();
    }
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
   * @param ttl the TTL to use, in milliseconds
   * @return the updated object
   */
  public T setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  /**
   * @param ttlAction the {@link TtlAction} to use
   * @return the updated options object
   */
  public T setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
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
   * @param pinned the pinned flag value to use
   * @return the updated object
   */
  public T setPinned(boolean pinned) {
    mPinned = pinned;
    return getThis();
  }

  /**
   * @param owner the owner name of the inode
   * @return the updated object
   */
  public T setOwner(String owner) {
    mAcl.setOwningUser(owner);
    return getThis();
  }

  /**
   * @param mode the mode of the inode
   * @return the updated object
   */
  public T setMode(short mode) {
    mAcl.setMode(mode);
    return getThis();
  }

  /**
   * @return the default ACL associated with this inode
   * @throws UnsupportedOperationException if the inode is a file
   */
  public abstract DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException;

  /**
   * @param acl set the default ACL associated with this inode
   * @throws UnsupportedOperationException if the inode is a file
   */
  public abstract void setDefaultACL(DefaultAccessControlList acl)
      throws UnsupportedOperationException;

  /**
   * Sets ACL entries into the internal ACL.
   * The entries will overwrite any existing correspondent entries in the internal ACL.
   *
   * @param entries the ACL entries
   * @return the updated object
   */
  public T setAcl(List<AclEntry> entries) {
    for (AclEntry entry : entries) {
      if (entry.isDefault()) {
        getDefaultACL().setEntry(entry);
      } else {
        mAcl.setEntry(entry);
      }
    }
    return getThis();
  }

  /**
   * @param ufsFingerprint the ufs fingerprint to use
   * @return the updated object
   */
  public T setUfsFingerprint(String ufsFingerprint) {
    mUfsFingerprint = ufsFingerprint;
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
   * Obtains a read lock on the inode. This call should only be used when locking the root or an
   * inode by id and not path or parent.
   */
  public void lockRead() {
    mLock.readLock().lock();
  }

  /**
   * Obtains a read lock on the inode. Afterward, checks the inode state:
   *   - parent is consistent with what the caller is expecting
   *   - the inode is not marked as deleted
   * If the state is inconsistent, an exception will be thrown and the lock will be released.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param parent the expected parent inode
   * @throws InvalidPathException if the parent is not as expected
   */
  public void lockReadAndCheckParent(Inode parent) throws InvalidPathException {
    lockRead();
    if (mDeleted) {
      unlockRead();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_DELETE.getMessage());
    }
    if (mParentId != InodeTree.NO_PARENT && mParentId != parent.getId()) {
      unlockRead();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
  }

  /**
   * Obtains a read lock on the inode. Afterward, checks the inode state to ensure the full inode
   * path is consistent with what the caller is expecting. If the state is inconsistent, an
   * exception will be thrown and the lock will be released.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param parent the expected parent inode
   * @param name the expected name of the inode to be locked
   * @throws InvalidPathException if the parent and/or name is not as expected
   */
  public void lockReadAndCheckNameAndParent(Inode parent, String name) throws InvalidPathException {
    lockReadAndCheckParent(parent);
    if (!mName.equals(name)) {
      unlockRead();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
  }

  /**
   * Obtains a write lock on the inode. This call should only be used when locking the root or an
   * inode by id and not path or parent.
   */
  public void lockWrite() {
    mLock.writeLock().lock();
  }

  /**
   * Obtains a write lock on the inode. Afterward, checks the inode state:
   *   - parent is consistent with what the caller is expecting
   *   - the inode is not marked as deleted
   * If the state is inconsistent, an exception will be thrown and the lock will be released.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param parent the expected parent inode
   * @throws InvalidPathException if the parent is not as expected
   */
  public void lockWriteAndCheckParent(Inode parent) throws InvalidPathException {
    lockWrite();
    if (mDeleted) {
      unlockWrite();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_DELETE.getMessage());
    }
    if (mParentId != InodeTree.NO_PARENT && mParentId != parent.getId()) {
      unlockWrite();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
  }

  /**
   * Obtains a write lock on the inode. Afterward, checks the inode state to ensure the full inode
   * path is consistent with what the caller is expecting. If the state is inconsistent, an
   * exception will be thrown and the lock will be released.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param parent the expected parent inode
   * @param name the expected name of the inode to be locked
   * @throws InvalidPathException if the parent and/or name is not as expected
   */
  public void lockWriteAndCheckNameAndParent(Inode parent, String name)
      throws InvalidPathException {
    lockWriteAndCheckParent(parent);
    if (!mName.equals(name)) {
      unlockWrite();
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
  }

  /**
   * Releases the read lock for this inode.
   */
  public void unlockRead() {
    mLock.readLock().unlock();
  }

  /**
   * Releases the write lock for this inode.
   */
  public void unlockWrite() {
    mLock.writeLock().unlock();
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

  /**
   * Checks whether the user or one of the groups has the permission to take the action.
   *
   *
   * @param user the user checking permission
   * @param groups the groups the user belongs to
   * @param action the action to take
   * @return whether permitted to take the action
   * @see AccessControlList#checkPermission(String, List, AclAction)
   */
  public boolean checkPermission(String user, List<String> groups, AclAction action) {
    return mAcl.checkPermission(user, groups, action);
  }

  /**
   * Gets the permitted actions for a user.
   *
   * @param user the user
   * @param groups the groups the user belongs to
   * @return the permitted actions
   * @see AccessControlList#getPermission(String, List)
   */
  public AclActions getPermission(String user, List<String> groups) {
    return mAcl.getPermission(user, groups);
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
        .add("ttl", mTtl).add("ttlAction", mTtlAction)
        .add("directory", mDirectory).add("persistenceState", mPersistenceState)
        .add("lastModificationTimeMs", mLastModificationTimeMs)
        .add("owner", mAcl.getOwningUser())
        .add("group", mAcl.getOwningGroup())
        .add("permission", mAcl.getMode())
        .add("ufsFingerprint", mUfsFingerprint);
  }
}
