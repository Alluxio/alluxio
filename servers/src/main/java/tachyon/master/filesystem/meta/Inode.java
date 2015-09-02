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

package tachyon.master.filesystem.meta;

import java.io.IOException;

import tachyon.master.journal.JournalOutputStream;
import tachyon.master.journal.JournalSerializable;
import tachyon.thrift.FileInfo;

/**
 * <code>Inode</code> is an abstract class, with information shared by all types of Inodes.
 */
public abstract class Inode implements JournalSerializable {
  private final long mCreationTimeMs;
  protected final boolean mIsFolder;

  private final long mId;
  private String mName;
  private long mParentId;

  /**
   * A pinned file is never evicted from memory. Folders are not pinned in memory; however, new
   * files and folders will inherit this flag from their parents.
   */
  private boolean mPinned = false;

  /**
   * The last modification time of this inode, in milliseconds.
   */
  private long mLastModificationTimeMs;

  /**
   * Indicates whether an inode is deleted or not.
   */
  private boolean mDeleted = false;

  /**
   * Create an inode.
   *
   * @param name the name of the inode.
   * @param id the id of the inode, which is globally unique.
   * @param parentId the id of the parent inode. -1 if there is no parent.
   * @param isFolder if the inode presents a folder
   * @param creationTimeMs the creation time of the inode, in milliseconds.
   */
  protected Inode(String name, long id, long parentId, boolean isFolder, long creationTimeMs) {
    mCreationTimeMs = creationTimeMs;
    mIsFolder = isFolder;
    mId = id;
    mName = name;
    mParentId = parentId;
    mLastModificationTimeMs = creationTimeMs;
  }

  /**
   * Marks the inode as deleted
   */
  public synchronized void delete() {
    mDeleted = true;
  }

  @Override
  public synchronized boolean equals(Object o) {
    if (!(o instanceof Inode)) {
      return false;
    }
    return mId == ((Inode) o).mId;
  }

  /**
   * Generate a FileInfo of the file or folder.
   *
   * @param path The path of the file
   * @return generated FileInfo
   */
  public abstract FileInfo generateClientFileInfo(String path);

  /**
   * Get the create time of the inode.
   *
   * @return the create time, in milliseconds
   */
  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  /**
   * Get the id of the inode
   *
   * @return the id of the inode
   */
  public synchronized long getId() {
    return mId;
  }

  /**
   * Get the last modification time of the inode
   *
   * @return the last modification time, in milliseconds
   */
  public synchronized long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  /**
   * Get the name of the inode
   *
   * @return the name of the inode
   */
  public synchronized String getName() {
    return mName;
  }

  /**
   * Get the id of the parent folder
   *
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
   * Return whether the inode is deleted or not.
   *
   * @return true if the inode is deleted, false otherwise
   */
  public boolean isDeleted() {
    return mDeleted;
  }

  /**
   * Return whether the inode is a directory or not
   *
   * @return true if the inode is a directory, false otherwise
   */
  public boolean isDirectory() {
    return mIsFolder;
  }

  /**
   * Return whether the inode is a file or not
   *
   * @return true if the inode is a file, false otherwise
   */
  public boolean isFile() {
    return !mIsFolder;
  }

  /**
   * Get the pinned flag of the inode
   *
   * @return true if the inode is pinned, false otherwise
   */
  public synchronized boolean isPinned() {
    return mPinned;
  }

  /**
   * Restore a deleted inode.
   */
  public synchronized void restore() {
    mDeleted = false;
  }

  /**
   * Set the last modification time of the inode
   *
   * @param lastModificationTimeMs The last modification time, in milliseconds
   */
  public synchronized void setLastModificationTimeMs(long lastModificationTimeMs) {
    mLastModificationTimeMs = lastModificationTimeMs;
  }

  /**
   * Set the name of the inode
   *
   * @param name The new name of the inode
   */
  public synchronized void setName(String name) {
    mName = name;
  }

  /**
   * Set the parent folder of the inode
   *
   * @param parentId The new parent
   */
  public synchronized void setParentId(long parentId) {
    mParentId = parentId;
  }

  /**
   * Set the pinned flag of the inode
   *
   * @param pinned If true, the inode need pinned, and a pinned file is never evicted from memory
   */
  public synchronized void setPinned(boolean pinned) {
    mPinned = pinned;
  }

  @Override
  public synchronized String toString() {
    return new StringBuilder("Inode(").append("ID:").append(mId).append(", NAME:").append(mName)
        .append(", PARENT_ID:").append(mParentId).append(", CREATION_TIME_MS:")
        .append(mCreationTimeMs).append(", PINNED:").append(mPinned).append("DELETED:")
        .append(mDeleted).append(", LAST_MODIFICATION_TIME_MS:").append(mLastModificationTimeMs)
        .append(")").toString();
  }

  @Override
  public abstract void writeToJournal(JournalOutputStream outputStream) throws IOException;
}
