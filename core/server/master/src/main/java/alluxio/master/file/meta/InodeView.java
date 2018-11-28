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

import alluxio.exception.InvalidPathException;
import alluxio.master.journal.JournalEntryRepresentable;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclAction;
import alluxio.security.authorization.AclActions;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.util.interfaces.Scoped;
import alluxio.wire.FileInfo;
import alluxio.wire.TtlAction;

import java.util.List;
import java.util.Optional;

/**
 * Read-only view of an inode.
 */
public interface InodeView extends JournalEntryRepresentable, Comparable<String> {

  /**
   * @return the create time, in milliseconds
   */
  long getCreationTimeMs();

  /**
   * @return the group of the inode
   */
  String getGroup();

  /**
   * @return the id of the inode
   */
  long getId();

  /**
   * @return the ttl of the file
   */
  long getTtl();

  /**
   * @return the {@link TtlAction}
   */
  TtlAction getTtlAction();

  /**
   * @return the last modification time, in milliseconds
   */
  long getLastModificationTimeMs();

  /**
   * @return the name of the inode
   */
  String getName();

  /**
   * @return the mode of the inode
   */
  short getMode();

  /**
   * @return the {@link PersistenceState} of the inode
   */
  PersistenceState getPersistenceState();

  /**
   * Tries to acquire a lock on persisting the inode.
   *
   * @return Empty if the lock is already taken, otherwise return an AutoCloseable which will
   *         release ownership of the inode's persistence state
   */
  Optional<Scoped> tryAcquirePersistingLock();

  /**
   * @return the id of the parent folder
   */
  long getParentId();

  /**
   * @return the owner of the inode
   */
  String getOwner();

  /**
   * @return true if the inode is deleted, false otherwise
   */
  boolean isDeleted();

  /**
   * @return true if the inode is a directory, false otherwise
   */
  boolean isDirectory();

  /**
   * @return true if the inode is a file, false otherwise
   */
  boolean isFile();

  /**
   * @return true if the inode is pinned, false otherwise
   */
  boolean isPinned();

  /**
   * @return true if the file has persisted, false otherwise
   */
  boolean isPersisted();

  /**
   * @return the UFS fingerprint
   */
  String getUfsFingerprint();

  /**
   * @return the access control list
   */
  AccessControlList getACL();

  /**
   * @return the default ACL associated with this inode
   * @throws UnsupportedOperationException if the inode is a file
   */
  DefaultAccessControlList getDefaultACL() throws UnsupportedOperationException;

  /**
   * Generates a {@link FileInfo} of the file or folder.
   *
   * @param path the path of the file
   * @return generated {@link FileInfo}
   */
  FileInfo generateClientFileInfo(String path);

  /**
   * Obtains a read lock on the inode. This call should only be used when locking the root or an
   * inode by id and not path or parent.
   */
  void lockRead();

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
  void lockReadAndCheckParent(InodeView parent) throws InvalidPathException;

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
  void lockReadAndCheckNameAndParent(InodeView parent, String name) throws InvalidPathException;

  /**
   * Obtains a write lock on the inode. This call should only be used when locking the root or an
   * inode by id and not path or parent.
   */
  void lockWrite();

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
  void lockWriteAndCheckParent(InodeView parent) throws InvalidPathException;

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
  void lockWriteAndCheckNameAndParent(InodeView parent, String name)
      throws InvalidPathException;

  /**
   * Releases the read lock for this inode.
   */
  void unlockRead();

  /**
   * Releases the write lock for this inode.
   */
  void unlockWrite();

  /**
   * @return returns true if the current thread holds a write lock on this inode, false otherwise
   */
  boolean isWriteLocked();

  /**
   * @return returns true if the current thread holds a read lock on this inode, false otherwise
   */
  boolean isReadLocked();

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
  boolean checkPermission(String user, List<String> groups, AclAction action);

  /**
   * Gets the permitted actions for a user.
   *
   * @param user the user
   * @param groups the groups the user belongs to
   * @return the permitted actions
   * @see AccessControlList#getPermission(String, List)
   */
  AclActions getPermission(String user, List<String> groups);
}
