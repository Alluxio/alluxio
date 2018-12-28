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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.java.HeapInodeStore;

import org.junit.After;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Base class for inode locking tests. This class provides utility methods and fields to help with
 * testing.
 *
 * Subclasses should call super.after() in their @After methods to verify that all locks are
 * released at the end of each test.
 */
public class BaseInodeLockingTest {
  protected InodeLockManager mInodeLockManager = new InodeLockManager();
  protected InodeStore mInodeStore = new HeapInodeStore();

  // Directory structure is /a/b/c
  protected ReadOnlyInodeFile mFileC = inodeFile(3, 2, "c");
  protected ReadOnlyInodeDirectory mDirB = inodeDir(2, 1, "b", mFileC);
  protected ReadOnlyInodeDirectory mDirA = inodeDir(1, 0, "a", mDirB);
  protected ReadOnlyInodeDirectory mRootDir = inodeDir(0, -1, "", mDirA);

  protected List<ReadOnlyInode> mAllInodes = Arrays.asList(mRootDir, mDirA, mDirB, mFileC);

  @After
  public void after() {
    // Make sure all locks are released.
    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked();
    checkOnlyIncomingEdgesWriteLocked();
  }

  /**
   * Checks that only the specified inodes are read-locked.
   */
  protected void checkOnlyNodesReadLocked(ReadOnlyInode... inodes) {
    HashSet<ReadOnlyInode> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (ReadOnlyInode inode : inodes) {
      assertTrue("Expected inode " + inode.getId() + " to be read locked",
          mInodeLockManager.inodeReadLockedByCurrentThread(inode.getId()));
    }
    for (ReadOnlyInode inode : mAllInodes) {
      if (!shouldBeLocked.contains(inode)) {
        assertFalse("Expected inode " + inode.getId() + " to not be read locked",
            mInodeLockManager.inodeReadLockedByCurrentThread(inode.getId()));
      }
    }
  }

  /**
   * Checks that only the specified inodes are write-locked.
   */
  protected void checkOnlyNodesWriteLocked(ReadOnlyInode... inodes) {
    HashSet<ReadOnlyInode> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (ReadOnlyInode inode : inodes) {
      assertTrue("Expected inode " + inode.getId() + " to be write locked",
          mInodeLockManager.inodeWriteLockedByCurrentThread(inode.getId()));
    }
    for (ReadOnlyInode inode : mAllInodes) {
      if (!shouldBeLocked.contains(inode)) {
        assertFalse("Expected inode " + inode.getId() + " to not be write locked",
            mInodeLockManager.inodeWriteLockedByCurrentThread(inode.getId()));
      }
    }
  }

  /**
   * Checks that only the edges leading to the specified inodes are read-locked.
   */
  protected void checkOnlyIncomingEdgesReadLocked(ReadOnlyInode... inodes) {
    HashSet<ReadOnlyInode> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (ReadOnlyInode inode : inodes) {
      Edge edge = new Edge(inode.getParentId(), inode.getName());
      assertTrue("Expected edge " + edge + " to be read locked",
          mInodeLockManager.edgeReadLockedByCurrentThread(edge));
    }
    for (ReadOnlyInode inode : mAllInodes) {
      if (!shouldBeLocked.contains(inode)) {
        Edge edge = new Edge(inode.getParentId(), inode.getName());
        assertFalse("Expected edge " + edge + " to not be read locked",
            mInodeLockManager.edgeReadLockedByCurrentThread(edge));
      }
    }
  }

  /**
   * Checks that only the edges leading to the specified inodes are write-locked.
   */
  protected void checkOnlyIncomingEdgesWriteLocked(ReadOnlyInode... inodes) {
    HashSet<ReadOnlyInode> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (ReadOnlyInode inode : inodes) {
      Edge edge = new Edge(inode.getParentId(), inode.getName());
      assertTrue("Expected edge " + edge + " to be write locked",
          mInodeLockManager.edgeWriteLockedByCurrentThread(edge));
    }
    for (ReadOnlyInode inode : mAllInodes) {
      if (!shouldBeLocked.contains(inode)) {
        Edge edge = new Edge(inode.getParentId(), inode.getName());
        assertFalse("Expected edge " + edge + " to not be write locked",
            mInodeLockManager.edgeWriteLockedByCurrentThread(edge));
      }
    }
  }

  /**
   * Checks that the specified edge is write-locked.
   */
  protected void checkIncomingEdgeWriteLocked(long parentId, String childName) {
    Edge edge = new Edge(parentId, childName);
    assertTrue("Unexpected write lock state for edge " + edge,
        mInodeLockManager.edgeWriteLockedByCurrentThread(edge));
  }

  protected ReadOnlyInodeDirectory inodeDir(long id, long parentId, String name,
      ReadOnlyInode... children) {
    ReadOnlyInodeDirectory dir = (ReadOnlyInodeDirectory) ReadOnlyInode
        .wrap(InodeDirectory.create(id, parentId, name, CreateDirectoryOptions.defaults()));
    for (ReadOnlyInode child : children) {
      mInodeStore.addChild(dir.getId(), child);
    }
    return dir;
  }

  protected ReadOnlyInodeFile inodeFile(long id, long parentId, String name) {
    return (ReadOnlyInodeFile) ReadOnlyInode
        .wrap(InodeFile.create(id, parentId, name, 0, CreateFileOptions.defaults()));
  }
}
