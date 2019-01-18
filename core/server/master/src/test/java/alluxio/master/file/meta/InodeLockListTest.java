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

import alluxio.resource.LockResource;
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
    mLockList.lockRootEdge(LockResource.LockMode.READ);
    mLockList.lockInode(mRootDir, LockResource.LockMode.READ);
    mLockList.lockEdge(mDirA.getName(), LockResource.LockMode.READ);
    mLockList.lockInode(mDirA, LockResource.LockMode.READ);
    assertEquals(mLockList.getLockMode(), LockResource.LockMode.READ);
    assertTrue(mLockList.endsInInode());
    assertEquals(2, mLockList.numLockedInodes());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getLockedInodes());

    mLockList.lockEdge(mDirB.getName(), LockResource.LockMode.WRITE);
    assertEquals(mLockList.getLockMode(), LockResource.LockMode.WRITE);
    assertFalse(mLockList.endsInInode());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(mDirB);
  }

  @Test
  public void pushWriteLockedEdge() {
    mLockList.lockRootEdge(LockResource.LockMode.WRITE);
    assertEquals(LockResource.LockMode.WRITE, mLockList.getLockMode());
    assertTrue(mLockList.getLockedInodes().isEmpty());

    mLockList.pushWriteLockedEdge(mRootDir, mDirA.getName());
    assertEquals(LockResource.LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());

    mLockList.pushWriteLockedEdge(mDirA, mDirB.getName());
    assertEquals(LockResource.LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getLockedInodes());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(mDirB);
  }

  @Test
  public void lockAndUnlockLast() {
    mLockList.lockRootEdge(LockResource.LockMode.READ);
    mLockList.lockInode(mRootDir, LockResource.LockMode.READ);
    mLockList.lockEdge(mDirA.getName(), LockResource.LockMode.READ);
    mLockList.lockInode(mDirA, LockResource.LockMode.WRITE);

    mLockList.unlockLastInode();
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());
    assertEquals(LockResource.LockMode.READ, mLockList.getLockMode());

    mLockList.unlockLastEdge();
    assertEquals(LockResource.LockMode.READ, mLockList.getLockMode());

    mLockList.lockEdge(mDirA.getName(), LockResource.LockMode.READ);
    mLockList.lockInode(mDirA, LockResource.LockMode.READ);
    mLockList.unlockLastInode();
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());
    assertEquals(LockResource.LockMode.READ, mLockList.getLockMode());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void unlockLastAll() {
    mLockList.lockRootEdge(LockResource.LockMode.READ);
    mLockList.lockInode(mRootDir, LockResource.LockMode.READ);
    for (InodeView inode : Arrays.asList(mDirA, mDirB, mFileC)) {
      mLockList.lockEdge(inode.getName(), LockResource.LockMode.READ);
      mLockList.lockInode(inode, LockResource.LockMode.READ);
    }
    for (int i = 0; i < 3; i++) {
      mLockList.unlockLastInode();
      mLockList.unlockLastEdge();
    }
    mLockList.unlockLastInode();
    mLockList.unlockLastEdge();
    assertEquals(0, mLockList.numLockedInodes());
    assertEquals(LockResource.LockMode.READ, mLockList.getLockMode());
    mLockList.lockRootEdge(LockResource.LockMode.READ);
    mLockList.lockInode(mRootDir, LockResource.LockMode.WRITE);
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked(mRootDir);
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeLastInode() {
    mLockList.lockRootEdge(LockResource.LockMode.READ);
    mLockList.lockInode(mRootDir, LockResource.LockMode.READ);
    mLockList.lockEdge(mDirA.getName(), LockResource.LockMode.READ);
    mLockList.lockInode(mDirA, LockResource.LockMode.WRITE);

    mLockList.downgradeLastInode();
    assertEquals(LockResource.LockMode.READ, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getLockedInodes());

    mLockList.unlockLastInode();
    mLockList.lockInode(mDirA, LockResource.LockMode.WRITE);
    assertEquals(LockResource.LockMode.WRITE, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getLockedInodes());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked(mDirA);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeLastInodeRoot() {
    mLockList.lockRootEdge(LockResource.LockMode.READ);
    mLockList.lockInode(mRootDir, LockResource.LockMode.WRITE);
    mLockList.downgradeLastInode();
    assertEquals(LockResource.LockMode.READ, mLockList.getLockMode());
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeLastEdge() {
    mLockList.lockRootEdge(LockResource.LockMode.WRITE);
    mLockList.downgradeLastEdge();
    assertEquals(LockResource.LockMode.READ, mLockList.getLockMode());

    mLockList.lockInode(mRootDir, LockResource.LockMode.READ);
    mLockList.lockEdge(mDirA.getName(), LockResource.LockMode.WRITE);
    mLockList.downgradeLastEdge();
    assertEquals(LockResource.LockMode.READ, mLockList.getLockMode());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeEdgeToInode() {
    mLockList.lockRootEdge(LockResource.LockMode.WRITE);
    mLockList.downgradeEdgeToInode(mRootDir, LockResource.LockMode.READ);
    assertEquals(Arrays.asList(mRootDir), mLockList.getLockedInodes());
    assertEquals(LockResource.LockMode.READ, mLockList.getLockMode());

    mLockList.lockEdge(mDirA.getName(), LockResource.LockMode.WRITE);
    mLockList.downgradeEdgeToInode(mDirA, LockResource.LockMode.WRITE);
    assertEquals(Arrays.asList(mRootDir, mDirA), mLockList.getLockedInodes());
    assertEquals(LockResource.LockMode.WRITE, mLockList.getLockMode());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked(mDirA);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void doubleWriteLock() {
    mLockList.lockRootEdge(LockResource.LockMode.WRITE);
    mThrown.expect(IllegalStateException.class);
    mLockList.lockInode(mRootDir, LockResource.LockMode.WRITE);
  }
}
