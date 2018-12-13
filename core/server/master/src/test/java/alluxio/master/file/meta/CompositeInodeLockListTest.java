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

import alluxio.master.file.meta.InodeTree.LockMode;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

/**
 * Unit tests for {@link CompositeInodeLockList}.
 *
 * There is coverage for the working on top of an empty base lock list in {@link InodeLockListTest}.
 */
public class CompositeInodeLockListTest extends BaseInodeLockingTest {
  private InodeLockList mBase = new InodeLockList(mInodeLockManager);
  private CompositeInodeLockList mComposite;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public void after() {
    if (mComposite != null) {
      mComposite.close();
    }
    mBase.close();
    super.after();
  }

  @Test
  public void unlockOnlyExtension() {
    mBase.lockRootEdge(LockMode.READ);
    mBase.lockInode(mRootDir, LockMode.READ);
    mBase.lockEdge(mDirA.getName(), LockMode.READ);

    mComposite = new CompositeInodeLockList(mBase);
    mComposite.lockInode(mDirA, LockMode.READ);
    mComposite.lockEdge(mDirB.getName(), LockMode.READ);
    mComposite.lockInode(mDirB, LockMode.WRITE);
    mComposite.close();

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void extendFromEdge() {
    mBase.lockRootEdge(LockMode.READ);
    mBase.lockInode(mRootDir, LockMode.READ);
    mBase.lockEdge(mDirA.getName(), LockMode.READ);
    mComposite = new CompositeInodeLockList(mBase);
    assertEquals(LockMode.READ, mComposite.getLockMode());
    assertEquals(Arrays.asList(mRootDir), mComposite.getInodes());

    mComposite.lockInode(mDirA, LockMode.READ);
    assertEquals(Arrays.asList(mRootDir, mDirA), mComposite.getInodes());
    assertEquals(2, mComposite.numLockedInodes());
    assertFalse(mComposite.isEmpty());
    assertEquals(mRootDir, mComposite.get(0));
    assertEquals(mDirA, mComposite.get(1));

    mComposite.lockEdge(mDirB.getName(), LockMode.WRITE);
    assertEquals(LockMode.WRITE, mComposite.getLockMode());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(mDirB);
  }

  @Test
  public void extendFromInode() {
    mBase.lockRootEdge(LockMode.READ);
    mBase.lockInode(mRootDir, LockMode.READ);
    mComposite = new CompositeInodeLockList(mBase);
    assertEquals(LockMode.READ, mComposite.getLockMode());
    assertEquals(Arrays.asList(mRootDir), mComposite.getInodes());

    mComposite.lockEdge(mDirA.getName(), LockMode.READ);
    mComposite.lockInode(mDirA, LockMode.READ);
    assertEquals(Arrays.asList(mRootDir, mDirA), mComposite.getInodes());
    assertEquals(2, mComposite.numLockedInodes());
    assertFalse(mComposite.isEmpty());
    assertEquals(mRootDir, mComposite.get(0));

    mComposite.lockEdge(mDirB.getName(), LockMode.WRITE);
    assertEquals(LockMode.WRITE, mComposite.getLockMode());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(mDirB);
  }

  @Test
  public void extendFromWriteLocked() {
    mBase.lockRootEdge(LockMode.WRITE);
    mComposite = new CompositeInodeLockList(mBase);
    assertEquals(LockMode.WRITE, mComposite.getLockMode());

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked();
    checkOnlyIncomingEdgesWriteLocked(mRootDir);
  }

  @Test
  public void doubleWriteLock() {
    mBase.lockRootEdge(LockMode.WRITE);
    mComposite = new CompositeInodeLockList(mBase);
    mThrown.expect(IllegalStateException.class);
    mComposite.lockInode(mRootDir, LockMode.WRITE);
  }

  @Test
  public void unlockIntoBase() {
    mBase.lockRootEdge(LockMode.WRITE);
    mComposite = new CompositeInodeLockList(mBase);
    mThrown.expect(IllegalStateException.class);
    mComposite.unlockLastEdge();
  }
}
