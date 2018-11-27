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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.options.CreateDirectoryOptions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Unit tests for {@link InodeLockList}.
 */
public class InodeLockListTest {
  private static final InodeDirectory ROOT =
      InodeDirectory.create(0, -1, "", CreateDirectoryOptions.defaults());
  private static final InodeDirectory ROOT_CHILD =
      InodeDirectory.create(1, 0, "1", CreateDirectoryOptions.defaults());

  private static final InodeDirectory DISCONNECTED =
      InodeDirectory.create(2, 100, "dc", CreateDirectoryOptions.defaults());
  private static final InodeDirectory NONEXIST =
      InodeDirectory.create(3, 0, "3", CreateDirectoryOptions.defaults());

  private static final Predicate<Long> EXISTS_FN = Arrays.asList(0L, 1L, 2L)::contains;

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  private InodeLocker mInodeLocker = new InodeLocker();
  private InodeLockList mLockList = new InodeLockList(mInodeLocker, EXISTS_FN);

  @Test
  public void lockRead() {
    assertFalse(isReadLocked(ROOT));
    assertFalse(isWriteLocked(ROOT));

    mLockList.lockRead(ROOT);
    assertTrue(isReadLocked(ROOT));
    assertFalse(isWriteLocked(ROOT));

    mLockList.unlockLast();
    assertFalse(isReadLocked(ROOT));
  }

  @Test
  public void lockReadAndCheckParent() throws Exception {
    mLockList.lockReadAndCheckParent(ROOT_CHILD, ROOT);
    assertTrue(isReadLocked(ROOT_CHILD));
    mLockList.unlockLast();
    assertFalse(isReadLocked(ROOT_CHILD));
  }

  @Test
  public void lockReadAndCheckParentInvalid() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    mLockList.lockReadAndCheckParent(DISCONNECTED, ROOT);
  }

  @Test
  public void lockReadAndCheckParentDeleted() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_DELETE.getMessage());
    mLockList.lockReadAndCheckParent(NONEXIST, ROOT);
  }

  @Test
  public void lockReadAndCheckNameAndParent() throws Exception {
    mLockList.lockReadAndCheckNameAndParent(ROOT_CHILD, ROOT, ROOT_CHILD.getName());
    assertTrue(isReadLocked(ROOT_CHILD));
    mLockList.unlockLast();
    assertFalse(isReadLocked(ROOT_CHILD));
  }

  @Test
  public void lockReadAndCheckNameAndParentInvalidParent() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    mLockList.lockReadAndCheckNameAndParent(DISCONNECTED, ROOT, DISCONNECTED.getName());
  }

  @Test
  public void lockReadAndCheckNameAndParentInvalidName() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    mLockList.lockReadAndCheckNameAndParent(ROOT_CHILD, ROOT, "invalid");
  }

  @Test
  public void lockWrite() {
    assertFalse(isReadLocked(ROOT));
    assertFalse(isWriteLocked(ROOT));

    mLockList.lockWrite(ROOT);
    assertFalse(isReadLocked(ROOT));
    assertTrue(isWriteLocked(ROOT));

    mLockList.unlockLast();
    assertFalse(isReadLocked(ROOT));
    assertFalse(isWriteLocked(ROOT));
  }

  @Test
  public void lockWriteAndCheckParent() throws Exception {
    mLockList.lockWriteAndCheckParent(ROOT_CHILD, ROOT);
    assertTrue(isWriteLocked(ROOT_CHILD));
    mLockList.unlockLast();
    assertFalse(isWriteLocked(ROOT_CHILD));
  }

  @Test
  public void lockWriteAndCheckParentInvalid() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    mLockList.lockWriteAndCheckParent(DISCONNECTED, ROOT);
  }

  @Test
  public void lockWriteAndCheckParentDeleted() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_DELETE.getMessage());
    mLockList.lockWriteAndCheckParent(NONEXIST, ROOT);
  }

  @Test
  public void lockWriteAndCheckNameAndParent() throws Exception {
    mLockList.lockWriteAndCheckNameAndParent(ROOT_CHILD, ROOT, ROOT_CHILD.getName());
    assertTrue(isWriteLocked(ROOT_CHILD));
    mLockList.unlockLast();
    assertFalse(isWriteLocked(ROOT_CHILD));
  }

  @Test
  public void lockWriteAndCheckNameAndParentInvalidParent() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    mLockList.lockWriteAndCheckNameAndParent(DISCONNECTED, ROOT, DISCONNECTED.getName());
  }

  @Test
  public void lockWriteAndCheckNameAndParentInvalidName() throws Exception {
    mExpectedException.expect(InvalidPathException.class);
    mExpectedException.expectMessage(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    mLockList.lockWriteAndCheckNameAndParent(ROOT_CHILD, ROOT, "invalid");
  }

  private boolean isReadLocked(Inode<?> inode) {
    return mInodeLocker.isReadLockedByCurrentThread(inode.getId());
  }

  private boolean isWriteLocked(Inode<?> inode) {
    return mInodeLocker.isWriteLockedByCurrentThread(inode.getId());
  }
}
