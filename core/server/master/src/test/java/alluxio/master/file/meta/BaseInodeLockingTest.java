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

  // Directory structure is /a/b/c
  protected InodeFileView mFileC = inodeFile(3, 2, "c");
  protected InodeDirectoryView mDirB = inodeDir(2, 1, "b", mFileC);
  protected InodeDirectoryView mDirA = inodeDir(1, 0, "a", mDirB);
  protected InodeDirectoryView mRootDir = inodeDir(0, -1, "", mDirA);

  protected List<InodeView> mAllInodes = Arrays.asList(mRootDir, mDirA, mDirB, mFileC);

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
  protected void checkOnlyNodesReadLocked(InodeView... inodes) {
    HashSet<InodeView> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (InodeView inode : inodes) {
      assertTrue("Expected inode " + inode.getId() + " to be read locked",
          mInodeLockManager.inodeReadLockedByCurrentThread(inode.getId()));
    }
    for (InodeView inode : mAllInodes) {
      if (!shouldBeLocked.contains(inode)) {
        assertFalse("Expected inode " + inode.getId() + " to not be read locked",
            mInodeLockManager.inodeReadLockedByCurrentThread(inode.getId()));
      }
    }
  }

  /**
   * Checks that only the specified inodes are write-locked.
   */
  protected void checkOnlyNodesWriteLocked(InodeView... inodes) {
    HashSet<InodeView> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (InodeView inode : inodes) {
      assertTrue("Expected inode " + inode.getId() + " to be write locked",
          mInodeLockManager.inodeWriteLockedByCurrentThread(inode.getId()));
    }
    for (InodeView inode : mAllInodes) {
      if (!shouldBeLocked.contains(inode)) {
        assertFalse("Expected inode " + inode.getId() + " to not be write locked",
            mInodeLockManager.inodeWriteLockedByCurrentThread(inode.getId()));
      }
    }
  }

  /**
   * Checks that only the edges leading to the specified inodes are read-locked.
   */
  protected void checkOnlyIncomingEdgesReadLocked(InodeView... inodes) {
    HashSet<InodeView> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (InodeView inode : inodes) {
      Edge edge = new Edge(inode.getParentId(), inode.getName());
      assertTrue("Expected edge " + edge + " to be read locked",
          mInodeLockManager.edgeReadLockedByCurrentThread(edge));
    }
    for (InodeView inode : mAllInodes) {
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
  protected void checkOnlyIncomingEdgesWriteLocked(InodeView... inodes) {
    HashSet<InodeView> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (InodeView inode : inodes) {
      Edge edge = new Edge(inode.getParentId(), inode.getName());
      assertTrue("Expected edge " + edge + " to be write locked",
          mInodeLockManager.edgeWriteLockedByCurrentThread(edge));
    }
    for (InodeView inode : mAllInodes) {
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

  protected static InodeDirectory inodeDir(long id, long parentId, String name,
      InodeView... children) {
    InodeDirectory dir =
        InodeDirectory.create(id, parentId, name, CreateDirectoryOptions.defaults());
    for (InodeView child : children) {
      dir.addChild((Inode<?>) child);
    }
    return dir;
  }

  protected static InodeFile inodeFile(long id, long parentId, String name) {
    return InodeFile.create(id, parentId, name, 0, CreateFileOptions.defaults());
  }
}
