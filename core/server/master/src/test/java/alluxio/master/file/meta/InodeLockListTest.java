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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.master.file.meta.InodeTree.LockMode;
import alluxio.master.file.options.CreateDirectoryOptions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

/**
 * Unit tests for {@link InodeLockList}.
 */
public class InodeLockListTest {
  // Directory structure is /a/b/c
  private static final InodeView ROOT = inodeDir(0, -1, "");
  private static final InodeView A = inodeDir(1, 0, "a");
  private static final InodeView B = inodeDir(2, 1, "b");
  private static final InodeView C = inodeDir(3, 2, "c");

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  private InodeLockManager mInodeLockManager = new InodeLockManager();
  private InodeLockList mLockList = new InodeLockList(mInodeLockManager);

  @Test
  public void lockSimple() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(ROOT, LockMode.READ);
    mLockList.lockEdge(A.getName(), LockMode.READ);
    mLockList.lockInode(A, LockMode.READ);
    assertEquals(mLockList.getLockMode(), LockMode.READ);
    assertTrue(mLockList.endsInInode());
    assertEquals(2, mLockList.numLockedInodes());
    assertEquals(Arrays.asList(ROOT, A), mLockList.getInodes());

    mLockList.lockEdge(B.getName(), LockMode.WRITE);
    assertEquals(mLockList.getLockMode(), LockMode.WRITE);
    assertFalse(mLockList.endsInInode());

    checkReadLocked(ROOT, A);
    checkIncomingEdgeReadLocked(A);
    checkIncomingEdgeWriteLocked(B);
  }

  @Test
  public void pushWriteLockedEdge() {
    mLockList.lockRootEdge(LockMode.WRITE);
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertTrue(mLockList.getInodes().isEmpty());

    mLockList.pushWriteLockedEdge(ROOT, "a");
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(ROOT), mLockList.getInodes());

    mLockList.pushWriteLockedEdge(A, "b");
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(ROOT, A), mLockList.getInodes());

    checkReadLocked(ROOT, A);
    checkIncomingEdgeReadLocked(ROOT, A);
    checkIncomingEdgeWriteLocked(B);
  }

  private void checkIncomingEdgeReadLocked(InodeView... inodes) {
    checkEdgeLock(inodes, LockMode.READ);
  }

  private void checkIncomingEdgeWriteLocked(InodeView... inodes) {
    checkEdgeLock(inodes, LockMode.WRITE);
  }

  private void checkEdgeLock(InodeView[] inodes, LockMode mode) {
    for (InodeView inode : inodes) {
      Edge edge = new Edge(inode.getParentId(), inode.getName());
      if (mode == LockMode.READ) {
        assertTrue(mInodeLockManager.isReadLockedByCurrentThread(edge));
      } else {
        assertTrue(mInodeLockManager.isWriteLockedByCurrentThread(edge));
      }
    }
  }

  private void checkReadLocked(InodeView... inodes) {
    for (InodeView inode : inodes) {
      assertTrue(mInodeLockManager.isReadLockedByCurrentThread(inode.getId()));
    }
  }

  private void checkWriteLocked(InodeView... inodes) {
    for (InodeView inode : inodes) {
      assertTrue(mInodeLockManager.isWriteLockedByCurrentThread(inode.getId()));
    }
  }

  private static InodeView inodeDir(long id, long parentId, String name) {
    return InodeDirectory.create(id, parentId, name, CreateDirectoryOptions.defaults());
  }
}
