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

import alluxio.concurrent.LockMode;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

/**
 * Unit tests for {@link InodeLockList}.
 */
public class SimpleInodeLockListTest extends BaseInodeLockingTest {
  private InodeLockList mLockList = new SimpleInodeLockList(mInodeLockManager, false);

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
    mLockList.lockEdge(mRootDir, mDirA.getName(), LockMode.READ);
    mLockList.lockInode(mDirA, LockMode.READ);
    assertEquals(mLockList.getLockMode(), LockMode.READ);
    assertTrue(mLockList.endsInInode());
    assertEquals(2, mLockList.numInodes());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getLockedInodes());

    mLockList.lockEdge(mDirA, mDirB.getName(), LockMode.WRITE);
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
    assertTrue(mLockList.getLockedInodes().isEmpty());

    mLockList.pushWriteLockedEdge(mRootDir, mDirA.getName());
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());

    mLockList.pushWriteLockedEdge(mDirA, mDirB.getName());
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getLockedInodes());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(mDirB);
  }

  @Test
  public void lockAndUnlockLast() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(mRootDir, LockMode.READ);
    mLockList.lockEdge(mRootDir, mDirA.getName(), LockMode.READ);
    mLockList.lockInode(mDirA, LockMode.WRITE);

    mLockList.unlockLastInode();
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.unlockLastEdge();
    assertEquals(LockMode.READ, mLockList.getLockMode());

    mLockList.lockEdge(mRootDir, mDirA.getName(), LockMode.READ);
    mLockList.lockInode(mDirA, LockMode.READ);
    mLockList.unlockLastInode();
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());
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
    Inode prev = mRootDir;
    for (Inode inode : Arrays.asList(mDirA, mDirB, mFileC)) {
      mLockList.lockEdge(prev, inode.getName(), LockMode.READ);
      mLockList.lockInode(inode, LockMode.READ);
      prev = inode;
    }
    for (int i = 0; i < 3; i++) {
      mLockList.unlockLastInode();
      mLockList.unlockLastEdge();
    }
    mLockList.unlockLastInode();
    mLockList.unlockLastEdge();
    assertEquals(0, mLockList.numInodes());
    assertEquals(LockMode.READ, mLockList.getLockMode());
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(mRootDir, LockMode.WRITE);
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked(mRootDir);
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeLastInodeRoot() {
    mLockList.lockRootEdge(LockMode.READ);
    mLockList.lockInode(mRootDir, LockMode.WRITE);
    mLockList.downgradeToReadLocks();
    assertEquals(LockMode.READ, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());

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
    mLockList.lockEdge(mRootDir, mDirA.getName(), LockMode.WRITE);
    mLockList.downgradeLastEdge();
    assertEquals(LockMode.READ, mLockList.getLockMode());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void doubleWriteLock() {
    mLockList.lockRootEdge(LockMode.WRITE);
    mLockList.lockInode(mRootDir, LockMode.WRITE);
    mLockList.unlockLastInode();
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
  }

  @Test
  public void readAfterWrite() {
    mLockList.lockRootEdge(LockMode.READ);
    assertEquals(LockMode.READ, mLockList.getLockMode());
    mLockList.lockInode(mRootDir, LockMode.WRITE);
    assertEquals(LockMode.WRITE, mLockList.getLockMode());
    mLockList.lockEdge(mRootDir, mDirA.getName(), LockMode.READ);
    assertEquals(LockMode.WRITE, mLockList.getLockMode());

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked(mRootDir);
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked(mDirA);
  }

  @Test
  public void lockFromNonRootInode() {
    mLockList.lockInode(mDirA, LockMode.READ);
    mLockList.lockEdge(mDirA, mDirB.getName(), LockMode.WRITE);
    mLockList.lockInode(mDirB, LockMode.WRITE);

    checkOnlyNodesReadLocked(mDirA);
    checkOnlyNodesWriteLocked(mDirB);
    checkOnlyIncomingEdgesReadLocked();
    checkOnlyIncomingEdgesWriteLocked(mDirB);
  }

  @Test
  public void lockFromNonRootEdge() {
    mLockList.lockEdge(mDirA, mDirB.getName(), LockMode.READ);
    mLockList.lockInode(mDirB, LockMode.WRITE);
    mLockList.lockEdge(mDirB, mFileC.getName(), LockMode.WRITE);

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked(mDirB);
    checkOnlyIncomingEdgesReadLocked(mDirB);
    checkOnlyIncomingEdgesWriteLocked(mFileC);
  }

  @Test
  public void lockRootEdgeWhenNotEmpty() {
    mLockList.lockInode(mDirA, LockMode.READ);
    mThrown.expect(IllegalStateException.class);
    mLockList.lockRootEdge(LockMode.READ);
  }

  @Test
  public void lockInodeAfterInode() {
    mLockList.lockInode(mDirA, LockMode.READ);
    mThrown.expect(IllegalStateException.class);
    mLockList.lockInode(mDirB, LockMode.READ);
  }

  @Test
  public void lockEdgeAfterEdge() {
    mLockList.lockEdge(mDirA, mDirB.getName(), LockMode.READ);
    mThrown.expect(IllegalStateException.class);
    mLockList.lockEdge(mDirB, mFileC.getName(), LockMode.READ);
  }

  @Test
  public void lockInodeAfterWrongEdge() {
    mLockList.lockEdge(mDirA, mDirB.getName(), LockMode.READ);
    mThrown.expect(IllegalStateException.class);
    mLockList.lockInode(mFileC, LockMode.READ);
  }

  @Test
  public void lockEdgeAfterWrongInode() {
    mLockList.lockInode(mDirA, LockMode.READ);
    mThrown.expect(IllegalStateException.class);
    mLockList.lockEdge(mDirB, mFileC.getName(), LockMode.READ);
  }

  @Test
  public void unlockEdgeAfterInode() {
    mLockList.lockInode(mDirA, LockMode.READ);
    mThrown.expect(IllegalStateException.class);
    mLockList.unlockLastEdge();
  }

  @Test
  public void unlockInodeAfterEdge() {
    mLockList.lockEdge(mDirA, mDirB.getName(), LockMode.READ);
    mThrown.expect(IllegalStateException.class);
    mLockList.unlockLastInode();
  }

  @Test
  public void numInodes() {
    assertEquals(0, mLockList.numInodes());
    mLockList.lockRootEdge(LockMode.READ);
    assertEquals(0, mLockList.numInodes());
    mLockList.lockInode(mRootDir, LockMode.READ);
    assertEquals(1, mLockList.numInodes());
    mLockList.lockEdge(mRootDir, mDirA.getName(), LockMode.READ);
    assertEquals(1, mLockList.numInodes());
    mLockList.lockInode(mDirA, LockMode.READ);
    assertEquals(2, mLockList.numInodes());
  }
}
