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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

/**
 * Unit tests for {@link InodeLockList}.
 */
public class InodeLockListTest extends BaseInodeLockingTest {
  private InodeLockList mLockList = new InodeLockList(mInodeLockManager);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public void after() {
    mLockList.close();
    super.after();
  }

  @Test
  public void lockSimple() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(mRootDir, LockMode.READ);
    mLockList.lockEdge(mDirA.getName(), LockMode.READ);
    mLockList.lockInode(mDirA, LockMode.READ);
    assertEquals(mLockList.getLockMode(), LockMode.READ);
    assertTrue(mLockList.endsInInode());
    assertEquals(2, mLockList.numLockedInodes());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getInodes());

    mLockList.lockEdge(mDirB.getName(), LockMode.WRITE);
    assertEquals(mLockList.getLockMode(), LockMode.WRITE);
    assertFalse(mLockList.endsInInode());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(mDirB);
  }

  @Test
  public void pushWriteLockedEdge() {
    mLockList.lockRootEdge(LockMode.WRITE);
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertTrue(mLockList.getInodes().isEmpty());

    mLockList.pushWriteLockedEdge(mRootDir, mDirA.getName());
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir), mLockList.getInodes());

    mLockList.pushWriteLockedEdge(mDirA, mDirB.getName());
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getInodes());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(mDirB);
  }

  @Test
  public void lockAndUnlockLast() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(mRootDir, LockMode.READ);
    mLockList.lockEdge(mDirA.getName(), LockMode.READ);
    mLockList.lockInode(mDirA, LockMode.WRITE);

    mLockList.unlockLastInode();
    assertEquals(Arrays.asList(mRootDir), mLockList.getInodes());
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.unlockLastEdge();
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.lockEdge(mDirA.getName(), LockMode.READ);
    mLockList.lockInode(mDirA, LockMode.READ);
    mLockList.unlockLastInode();
    assertEquals(Arrays.asList(mRootDir), mLockList.getInodes());
    assertEquals(LockMode.READ, mLockList.getLockMode());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void unlockLastAll() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(mRootDir, LockMode.READ);
    for (InodeView inode : Arrays.asList(mDirA, mDirB, mFileC)) {
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
    mLockList.lockInode(mRootDir, LockMode.WRITE);
    assertEquals(Arrays.asList(mRootDir), mLockList.getInodes());

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked(mRootDir);
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeLastInode() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(mRootDir, LockMode.READ);
    mLockList.lockEdge(mDirA.getName(), LockMode.READ);
    mLockList.lockInode(mDirA, LockMode.WRITE);

    mLockList.downgradeLastInode();
    assertEquals(LockMode.READ, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getInodes());

    mLockList.unlockLastInode();
    mLockList.lockInode(mDirA, LockMode.WRITE);
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getInodes());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked(mDirA);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeLastInodeRoot() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(mRootDir, LockMode.WRITE);
    mLockList.downgradeLastInode();
    assertEquals(LockMode.READ, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir), mLockList.getInodes());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeLastEdge() {
    mLockList.lockRootEdge(LockMode.WRITE);
    mLockList.downgradeLastEdge();
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.lockInode(mRootDir, LockMode.READ);
    mLockList.lockEdge(mDirA.getName(), LockMode.WRITE);
    mLockList.downgradeLastEdge();
    assertEquals(LockMode.READ, mLockList.getLockMode());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeEdgeToInode() {
    mLockList.lockRootEdge(LockMode.WRITE);
    mLockList.downgradeEdgeToInode(mRootDir, LockMode.READ);
    assertEquals(Arrays.asList(mRootDir), mLockList.getInodes());
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.lockEdge(mDirA.getName(), LockMode.WRITE);
    mLockList.downgradeEdgeToInode(mDirA, LockMode.WRITE);
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getInodes());
    assertEquals(LockMode.WRITE, mLockList.getLockMode());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked(mDirA);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void doubleWriteLock() {
    mLockList.lockRootEdge(LockMode.WRITE);
    mThrown.expect(IllegalStateException.class);
    mLockList.lockInode(mRootDir, LockMode.WRITE);
  }
}
