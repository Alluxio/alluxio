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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.InodeTree.LockPattern;

import org.junit.After;
import org.junit.Test;

import java.util.Arrays;

/**
 * Unit tests for {@link LockedInodePath}.
 */
public class LockedInodePathTest extends BaseInodeLockingTest {
  private LockedInodePath mPath;

  @After
  public void after() {
    if (mPath != null) {
      mPath.close();
    }
    super.after();
  }

  @Test
  public void pathExistsReadLock() throws Exception {
    AlluxioURI uri = new AlluxioURI("/a/b/c");
    mPath =
        new LockedInodePath(uri, mInodeStore, mInodeLockManager, mRootDir, LockPattern.READ, false);
    assertEquals(uri, mPath.getUri());
    assertEquals(4, mPath.size());

    mPath.traverse();
    assertTrue(mPath.fullPathExists());
    assertEquals(mFileC, mPath.getInode());
    assertEquals(mFileC, mPath.getInodeOrNull());
    assertEquals(mFileC, mPath.getInodeFile());
    assertEquals(mFileC, mPath.getLastExistingInode());
    assertEquals(mDirB, mPath.getParentInodeDirectory());
    assertEquals(mDirB, mPath.getParentInodeOrNull());
    assertEquals(mDirB, mPath.getAncestorInode());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB, mFileC), mPath.getInodeList());
    assertEquals(4, mPath.getExistingInodeCount());
    assertEquals(LockPattern.READ, mPath.getLockPattern());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void pathExistsWriteLock() throws Exception {
    mPath = create("/a/b/c", LockPattern.WRITE_INODE);

    assertTrue(mPath.fullPathExists());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB, mFileC), mPath.getInodeList());
    assertEquals(LockPattern.WRITE_INODE, mPath.getLockPattern());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyNodesWriteLocked(mFileC);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void pathExistsWriteEdgeLock() throws Exception {
    mPath = create("/a/b/c", LockPattern.WRITE_EDGE);

    assertTrue(mPath.fullPathExists());
    assertEquals(mFileC, mPath.getInode());
    assertEquals(mFileC, mPath.getInodeOrNull());
    assertEquals(mFileC, mPath.getInodeFile());
    assertEquals(mFileC, mPath.getLastExistingInode());
    assertEquals(mDirB, mPath.getParentInodeDirectory());
    assertEquals(mDirB, mPath.getParentInodeOrNull());
    assertEquals(mDirB, mPath.getAncestorInode());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB, mFileC), mPath.getInodeList());
    assertEquals(4, mPath.getExistingInodeCount());
    assertEquals(LockPattern.WRITE_EDGE, mPath.getLockPattern());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyNodesWriteLocked(mFileC);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyIncomingEdgesWriteLocked(mFileC);
  }

  @Test
  public void missingLastReadLock() throws Exception {
    mPath = create("/a/b/missing", LockPattern.READ);

    assertFalse(mPath.fullPathExists());
    assertNull(mPath.getInodeOrNull());
    assertEquals(mDirB, mPath.getLastExistingInode());
    assertEquals(mDirB, mPath.getParentInodeDirectory());
    assertEquals(mDirB, mPath.getParentInodeOrNull());
    assertEquals(mDirB, mPath.getAncestorInode());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB), mPath.getInodeList());
    assertEquals(3, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void missingLastWriteLock() throws Exception {
    mPath = create("/a/b/missing", LockPattern.WRITE_INODE);

    assertFalse(mPath.fullPathExists());
    assertNull(mPath.getInodeOrNull());
    assertEquals(mDirB, mPath.getLastExistingInode());
    assertEquals(mDirB, mPath.getParentInodeDirectory());
    assertEquals(mDirB, mPath.getParentInodeOrNull());
    assertEquals(mDirB, mPath.getAncestorInode());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB), mPath.getInodeList());
    assertEquals(3, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB);
    checkIncomingEdgeReadLocked(mDirB.getId(), "missing");
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void missingLastWriteEdgeLock() throws Exception {
    mPath = create("/a/b/missing", LockPattern.WRITE_EDGE);

    assertFalse(mPath.fullPathExists());
    assertNull(mPath.getInodeOrNull());
    assertEquals(mDirB, mPath.getLastExistingInode());
    assertEquals(mDirB, mPath.getParentInodeDirectory());
    assertEquals(mDirB, mPath.getParentInodeOrNull());
    assertEquals(mDirB, mPath.getAncestorInode());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB), mPath.getInodeList());
    assertEquals(3, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyIncomingEdgesWriteLocked();
    checkIncomingEdgeWriteLocked(mDirB.getId(), "missing");
  }

  @Test
  public void missingMultipleReadLock() throws Exception {
    mPath = create("/a/miss1/miss2", LockPattern.READ);

    assertFalse(mPath.fullPathExists());
    assertNull(mPath.getInodeOrNull());
    assertNull(mPath.getParentInodeOrNull());
    assertEquals(mDirA, mPath.getLastExistingInode());
    assertEquals(mDirA, mPath.getAncestorInode());
    assertEquals(Arrays.asList(mRootDir, mDirA), mPath.getInodeList());
    assertEquals(2, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void missingMultipleWriteEdgeLock() throws Exception {
    mPath = create("/a/miss1/miss2", LockPattern.WRITE_EDGE);

    assertFalse(mPath.fullPathExists());
    assertNull(mPath.getInodeOrNull());
    assertNull(mPath.getParentInodeOrNull());
    assertEquals(mDirA, mPath.getLastExistingInode());
    assertEquals(mDirA, mPath.getAncestorInode());
    assertEquals(Arrays.asList(mRootDir, mDirA), mPath.getInodeList());
    assertEquals(2, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
    checkIncomingEdgeWriteLocked(mDirA.getId(), "miss1");
  }

  @Test
  public void readLockRoot() throws Exception {
    mPath = create("/", LockPattern.READ);

    assertTrue(mPath.fullPathExists());
    assertEquals(mRootDir, mPath.getInodeOrNull());
    assertNull(mPath.getParentInodeOrNull());
    assertEquals(mRootDir, mPath.getLastExistingInode());
    assertEquals(Arrays.asList(mRootDir), mPath.getInodeList());
    assertEquals(1, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void writeLockRoot() throws Exception {
    mPath = create("/", LockPattern.WRITE_INODE);

    assertTrue(mPath.fullPathExists());
    assertEquals(mRootDir, mPath.getInodeOrNull());
    assertNull(mPath.getParentInodeOrNull());
    assertEquals(mRootDir, mPath.getLastExistingInode());
    assertEquals(Arrays.asList(mRootDir), mPath.getInodeList());
    assertEquals(1, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked(mRootDir);
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void writeEdgeLockRoot() throws Exception {
    mPath = create("/", LockPattern.WRITE_EDGE);

    assertTrue(mPath.fullPathExists());
    assertEquals(mRootDir, mPath.getInodeOrNull());
    assertNull(mPath.getParentInodeOrNull());
    assertEquals(mRootDir, mPath.getLastExistingInode());
    assertEquals(Arrays.asList(mRootDir), mPath.getInodeList());
    assertEquals(1, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked(mRootDir);
    checkOnlyIncomingEdgesReadLocked();
    checkOnlyIncomingEdgesWriteLocked(mRootDir);
  }

  @Test
  public void removeLastReadLockedInode() throws Exception {
    mPath = create("/a", LockPattern.READ);

    mPath.removeLastInode();
    assertFalse(mPath.fullPathExists());
    assertNull(mPath.getInodeOrNull());
    assertEquals(Arrays.asList(mRootDir), mPath.getInodeList());
    assertEquals(1, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void removeLastWriteEdgeLockedInode() throws Exception {
    mPath = create("/a", LockPattern.WRITE_EDGE);

    mPath.removeLastInode();
    assertFalse(mPath.fullPathExists());
    assertNull(mPath.getInodeOrNull());
    assertEquals(Arrays.asList(mRootDir), mPath.getInodeList());
    assertEquals(1, mPath.getExistingInodeCount());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked(mDirA);
  }

  @Test
  public void removeLastInodeImplicitlyLocked() throws Exception {
    mPath = create("/a", LockPattern.WRITE_EDGE);

    LockedInodePath pathC = mPath.lockDescendant(new AlluxioURI("/a/b/c"), LockPattern.READ);
    assertTrue(pathC.fullPathExists());
    pathC.removeLastInode();
    assertFalse(pathC.fullPathExists());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB), pathC.getInodeList());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked(mDirA, mDirB);
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked(mDirA, mDirB, mFileC);

    pathC.close();

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked(mDirA);
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked(mDirA);
  }

  @Test
  public void addNextFinalInode() throws Exception {
    mPath = create("/a/missing", LockPattern.WRITE_EDGE);

    assertFalse(mPath.fullPathExists());
    InodeFile missingInode = inodeFile(10, mDirA.getId(), "missing");
    mInodeStore.addChild(mDirA.getId(), missingInode);
    mPath.addNextInode(missingInode);
    assertTrue(mPath.fullPathExists());
    assertEquals(Arrays.asList(mRootDir, mDirA, missingInode), mPath.getInodeList());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(missingInode);
  }

  @Test
  public void addNextSecondToLastInode() throws Exception {
    mPath = create("/a/miss1/miss2", LockPattern.WRITE_EDGE);

    assertFalse(mPath.fullPathExists());
    InodeFile firstMissingInode = inodeFile(10, mDirA.getId(), "miss1");
    mInodeStore.addChild(mDirA.getId(), firstMissingInode);
    mPath.addNextInode(firstMissingInode);
    assertFalse(mPath.fullPathExists());
    assertEquals(Arrays.asList(mRootDir, mDirA, firstMissingInode), mPath.getInodeList());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, firstMissingInode);
    checkOnlyIncomingEdgesWriteLocked();
    // Write lock should be pushed forward when adding a non-final inode.
    checkIncomingEdgeWriteLocked(firstMissingInode.getId(), "miss2");
  }

  @Test
  public void downgradeWriteEdgeToRead() throws Exception {
    mPath = create("/a/b/c", LockPattern.WRITE_EDGE);

    mPath.downgradeToRead();
    assertTrue(mPath.fullPathExists());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB, mFileC), mPath.getInodeList());
    assertEquals(LockPattern.READ, mPath.getLockPattern());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void downgradeWriteInodeToReadInode() throws Exception {
    mPath = create("/a/b/c", LockPattern.WRITE_INODE);

    mPath.downgradeToRead();
    assertTrue(mPath.fullPathExists());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB, mFileC), mPath.getInodeList());
    assertEquals(LockPattern.READ, mPath.getLockPattern());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void lockChildReadToWriteEdge() throws Exception {
    mPath = create("/a", LockPattern.READ);

    LockedInodePath childPath = mPath.lockChild(mDirB, LockPattern.WRITE_EDGE);
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB), childPath.getInodeList());
    assertEquals(LockPattern.WRITE_EDGE, childPath.getLockPattern());
    assertTrue(childPath.fullPathExists());
    assertEquals(mDirB, childPath.getInode());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked(mDirB);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(mDirB);

    childPath.close();

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void lockChildReadToWriteInode() throws Exception {
    mPath = create("/a", LockPattern.READ);

    LockedInodePath childPath = mPath.lockChild(mDirB, LockPattern.WRITE_INODE);
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB), childPath.getInodeList());
    assertEquals(LockPattern.WRITE_INODE, childPath.getLockPattern());
    assertTrue(childPath.fullPathExists());
    assertEquals(mDirB, childPath.getInode());

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked(mDirB);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyIncomingEdgesWriteLocked();

    childPath.close();

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void lockChildWriteInodeToWriteEdge() throws Exception {
    mPath = create("/a", LockPattern.WRITE_INODE);

    LockedInodePath childPath = mPath.lockChild(mDirB, LockPattern.WRITE_EDGE);
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB), childPath.getInodeList());
    assertEquals(LockPattern.WRITE_EDGE, childPath.getLockPattern());
    assertTrue(childPath.fullPathExists());
    assertEquals(mDirB, childPath.getInode());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked(mDirA, mDirB);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked(mDirB);

    childPath.close();

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked(mDirA);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void lockChildReadToRead() throws Exception {
    mPath = create("/a", LockPattern.READ);

    LockedInodePath childPath = mPath.lockChild(mDirB, LockPattern.READ);
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB), childPath.getInodeList());
    assertEquals(LockPattern.READ, childPath.getLockPattern());
    assertTrue(childPath.fullPathExists());
    assertEquals(mDirB, childPath.getInode());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyIncomingEdgesWriteLocked();

    childPath.close();

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void lockChildMultipleReadExtensions() throws Exception {
    mPath = create("/a", LockPattern.READ);

    LockedInodePath childPath1 = mPath.lockChild(mDirB, LockPattern.READ);
    LockedInodePath childPath2 = childPath1.lockChild(mFileC, LockPattern.READ);

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyIncomingEdgesWriteLocked();

    childPath2.close();

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyIncomingEdgesWriteLocked();

    childPath1.close();

    checkOnlyNodesReadLocked(mRootDir, mDirA);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void lockDescendantReadToWriteEdge() throws Exception {
    mPath = create("/", LockPattern.READ);

    LockedInodePath childPath =
        mPath.lockDescendant(new AlluxioURI("/a/b/c"), LockPattern.WRITE_EDGE);
    assertTrue(childPath.fullPathExists());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB, mFileC), childPath.getInodeList());

    checkOnlyNodesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyNodesWriteLocked(mFileC);
    checkOnlyIncomingEdgesReadLocked(mRootDir, mDirA, mDirB);
    checkOnlyIncomingEdgesWriteLocked(mFileC);

    childPath.close();

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void lockDescendantWriteEdgeToWriteEdge() throws Exception {
    mPath = create("/", LockPattern.WRITE_EDGE);

    LockedInodePath childPath =
        mPath.lockDescendant(new AlluxioURI("/a/b/c"), LockPattern.WRITE_EDGE);
    assertTrue(childPath.fullPathExists());
    assertEquals(Arrays.asList(mRootDir, mDirA, mDirB, mFileC), childPath.getInodeList());

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked(mRootDir, mDirA, mDirB, mFileC);
    checkOnlyIncomingEdgesReadLocked();
    checkOnlyIncomingEdgesWriteLocked(mRootDir, mDirA, mDirB, mFileC);

    childPath.close();

    checkOnlyNodesReadLocked();
    checkOnlyNodesWriteLocked(mRootDir);
    checkOnlyIncomingEdgesReadLocked();
    checkOnlyIncomingEdgesWriteLocked(mRootDir);
  }

  @Test
  public void lockFinalEdgeWrite() throws Exception {
    mInodeStore.removeChild(mRootDir.getId(), "a");
    mPath = create("/a", LockPattern.READ);
    mPath.traverse();

    LockedInodePath writeLocked = mPath.lockFinalEdgeWrite();
    assertFalse(writeLocked.fullPathExists());
    assertEquals(Arrays.asList(mRootDir), writeLocked.getInodeList());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked(mDirA);

    writeLocked.close();

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked();
  }

  @Test
  public void lockFinalEdgeWriteAlreadyLocked() throws Exception {
    mInodeStore.removeChild(mRootDir.getId(), "a");
    mPath = create("/a", LockPattern.WRITE_EDGE);

    LockedInodePath writeLocked = mPath.lockFinalEdgeWrite();
    assertFalse(writeLocked.fullPathExists());
    assertEquals(Arrays.asList(mRootDir), writeLocked.getInodeList());

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked(mDirA);

    writeLocked.close();

    checkOnlyNodesReadLocked(mRootDir);
    checkOnlyNodesWriteLocked();
    checkOnlyIncomingEdgesReadLocked(mRootDir);
    checkOnlyIncomingEdgesWriteLocked(mDirA);
  }

  private LockedInodePath create(String path, LockPattern lockPattern) throws InvalidPathException {
    LockedInodePath lockedPath = new LockedInodePath(new AlluxioURI(path), mInodeStore,
        mInodeLockManager, mRootDir, lockPattern, false);
    lockedPath.traverse();
    return lockedPath;
  }
}
