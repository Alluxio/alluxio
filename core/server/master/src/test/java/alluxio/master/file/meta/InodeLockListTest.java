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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Unit tests for {@link InodeLockList}.
 */
@RunWith(Parameterized.class)
public class InodeLockListTest {
  // Directory structure is /a/b/c
  private static final InodeView ROOT = inodeDir(0, -1, "");
  private static final InodeView A = inodeDir(1, 0, "a");
  private static final InodeView B = inodeDir(2, 1, "b");
  private static final InodeView C = inodeDir(3, 2, "c");

  private static final List<InodeView> ALL_INODES = Arrays.asList(ROOT, A, B, C);

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  private InodeLockManager mInodeLockManager = new InodeLockManager();
  private InodeLockList mLockList;

  @Parameters
  public static Collection<Boolean> data() {
    return Arrays.asList(true, false);
  }

  public InodeLockListTest(boolean useComposite) {
    if (useComposite) {
      // Composite inode lock list on top of an empty inode lock list should function the same as an
      // empty inode lock list.
      mLockList = new CompositeInodeLockList(new InodeLockList(mInodeLockManager));
    } else {
      mLockList = new InodeLockList(mInodeLockManager);
    }
  }

  @After
  public void after() {
    mLockList.close();
    checkReadLocked();
    checkWriteLocked();
    checkIncomingEdgeReadLocked();
    checkIncomingEdgeWriteLocked();
  }

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
    checkWriteLocked();
    checkIncomingEdgeReadLocked(ROOT, A);
    checkIncomingEdgeWriteLocked(B);
  }

  @Test
  public void pushWriteLockedEdge() {
    mLockList.lockRootEdge(LockMode.WRITE);
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertTrue(mLockList.getInodes().isEmpty());

    mLockList.pushWriteLockedEdge(ROOT, A.getName());
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(ROOT), mLockList.getInodes());

    mLockList.pushWriteLockedEdge(A, B.getName());
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(ROOT, A), mLockList.getInodes());

    checkReadLocked(ROOT, A);
    checkWriteLocked();
    checkIncomingEdgeReadLocked(ROOT, A);
    checkIncomingEdgeWriteLocked(B);
  }

  @Test
  public void lockAndUnlockLast() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(ROOT, LockMode.READ);
    mLockList.lockEdge(A.getName(), LockMode.READ);
    mLockList.lockInode(A, LockMode.WRITE);

    mLockList.unlockLastInode();
    assertEquals(Arrays.asList(ROOT), mLockList.getInodes());
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.unlockLastEdge();
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.lockEdge(A.getName(), LockMode.READ);
    mLockList.lockInode(A, LockMode.READ);
    mLockList.unlockLastInode();
    assertEquals(Arrays.asList(ROOT), mLockList.getInodes());
    assertEquals(LockMode.READ, mLockList.getLockMode());

    checkReadLocked(ROOT);
    checkWriteLocked();
    checkIncomingEdgeReadLocked(ROOT, A);
    checkIncomingEdgeWriteLocked();
  }

  @Test
  public void unlockLastAll() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(ROOT, LockMode.READ);
    for (InodeView inode : Arrays.asList(A, B, C)) {
      mLockList.lockEdge(inode.getName(), LockMode.READ);
      mLockList.lockInode(inode, LockMode.READ);
    }
    for (int i = 0; i < 3; i++) {
      mLockList.unlockLastInode();
      mLockList.unlockLastEdge();
    }
    mLockList.unlockLastInode();
    mLockList.unlockLastEdge();
    assertEquals(0, mLockList.numLockedInodes());
    assertEquals(LockMode.READ, mLockList.getLockMode());
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(ROOT, LockMode.WRITE);
    assertEquals(Arrays.asList(ROOT), mLockList.getInodes());

    checkReadLocked();
    checkWriteLocked(ROOT);
    checkIncomingEdgeReadLocked(ROOT);
    checkIncomingEdgeWriteLocked();
  }

  @Test
  public void downgradeLastInode() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(ROOT, LockMode.READ);
    mLockList.lockEdge(A.getName(), LockMode.READ);
    mLockList.lockInode(A, LockMode.WRITE);

    mLockList.downgradeLastInode();
    assertEquals(LockMode.READ, mLockList.getLockMode());
    assertEquals(Arrays.asList(ROOT, A), mLockList.getInodes());

    mLockList.unlockLastInode();
    mLockList.lockInode(A, LockMode.WRITE);
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(ROOT, A), mLockList.getInodes());

    checkReadLocked(ROOT);
    checkWriteLocked(A);
    checkIncomingEdgeReadLocked(ROOT, A);
    checkIncomingEdgeWriteLocked();
  }

  @Test
  public void downgradeLastInodeRoot() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(ROOT, LockMode.WRITE);
    mLockList.downgradeLastInode();
    assertEquals(LockMode.READ, mLockList.getLockMode());
    assertEquals(Arrays.asList(ROOT), mLockList.getInodes());

    checkReadLocked(ROOT);
    checkWriteLocked();
    checkIncomingEdgeReadLocked(ROOT);
    checkIncomingEdgeWriteLocked();
  }

  @Test
  public void downgradeLastEdge() {
    mLockList.lockRootEdge(LockMode.WRITE);
    mLockList.downgradeLastEdge();
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.lockInode(ROOT, LockMode.READ);
    mLockList.lockEdge(A.getName(), LockMode.WRITE);
    mLockList.downgradeLastEdge();
    assertEquals(LockMode.READ, mLockList.getLockMode());

    checkReadLocked(ROOT);
    checkWriteLocked();
    checkIncomingEdgeReadLocked(ROOT, A);
    checkIncomingEdgeWriteLocked();
  }

  @Test
  public void downgradeEdgeToInode() {
    mLockList.lockRootEdge(LockMode.WRITE);
    mLockList.downgradeEdgeToInode(ROOT, LockMode.READ);
    assertEquals(Arrays.asList(ROOT), mLockList.getInodes());
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.lockEdge(A.getName(), LockMode.WRITE);
    mLockList.downgradeEdgeToInode(A, LockMode.WRITE);
    assertEquals(Arrays.asList(ROOT, A), mLockList.getInodes());
    assertEquals(LockMode.WRITE, mLockList.getLockMode());

    checkReadLocked(ROOT);
    checkWriteLocked(A);
    checkIncomingEdgeReadLocked(ROOT, A);
    checkIncomingEdgeWriteLocked();
  }

  private void checkReadLocked(InodeView... inodes) {
    HashSet<InodeView> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (InodeView inode : inodes) {
      assertEquals("Unexpected read lock state for inode " + inode.getId(),
          shouldBeLocked.contains(inode),
          mInodeLockManager.inodeReadLockedByCurrentThread(inode.getId()));
    }
  }

  private void checkWriteLocked(InodeView... inodes) {
    HashSet<InodeView> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (InodeView inode : inodes) {
      assertEquals("Unexpected write lock state for inode " + inode.getId(),
          shouldBeLocked.contains(inode),
          mInodeLockManager.inodeWriteLockedByCurrentThread(inode.getId()));
    }
  }

  private void checkIncomingEdgeReadLocked(InodeView... inodes) {
    HashSet<InodeView> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (InodeView inode : ALL_INODES) {
      Edge edge = new Edge(inode.getParentId(), inode.getName());
      assertEquals("Unexpected read lock state for edge " + edge,
          shouldBeLocked.contains(inode),
          mInodeLockManager.edgeReadLockedByCurrentThread(edge));
    }
  }

  private void checkIncomingEdgeWriteLocked(InodeView... inodes) {
    HashSet<InodeView> shouldBeLocked = new HashSet<>(Arrays.asList(inodes));
    for (InodeView inode : ALL_INODES) {
      Edge edge = new Edge(inode.getParentId(), inode.getName());
      assertEquals("Unexpected write lock state for edge " + edge,
          shouldBeLocked.contains(inode),
          mInodeLockManager.edgeWriteLockedByCurrentThread(edge));
    }
  }

  private static InodeView inodeDir(long id, long parentId, String name) {
    return InodeDirectory.create(id, parentId, name, CreateDirectoryOptions.defaults());
  }
}
