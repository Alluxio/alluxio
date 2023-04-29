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

package alluxio.master.metastore;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.file.options.DescendantType;
import alluxio.master.block.ContainerIdGenerable;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeIterationResult;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.underfs.UfsManager;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@RunWith(Parameterized.class)
public class RecursiveInodeIteratorTest extends InodeStoreTestBase {

  public RecursiveInodeIteratorTest(Function<InodeLockManager, InodeStore> store) {
    super(store);
  }

  MutableInode<?> mInodeA = inodeDir(1, 0, "a");
  MutableInode<?> mInodeAB = inodeDir(2, 1, "b");
  MutableInode<?> mInodeABC = inodeDir(3, 2, "c");
  MutableInode<?> mInodeABCF1 = inodeFile(4, 3, "f1");
  MutableInode<?> mInodeABCF2 = inodeFile(5, 3, "f2");
  MutableInode<?> mInodeAC = inodeDir(6, 1, "c");
  MutableInode<?> mInodeACF1 = inodeFile(7, 6, "f1");
  MutableInode<?> mInodeACF2 = inodeFile(8, 6, "f2");
  MutableInode<?> mInodeACF3 = inodeFile(9, 6, "f3");
  MutableInode<?> mInodeAF1 = inodeFile(10, 1, "f1");
  MutableInode<?> mInodeB = inodeDir(11, 0, "b");
  MutableInode<?> mInodeC = inodeDir(12, 0, "c");
  MutableInode<?> mInodeF1 = inodeFile(13, 0, "f1");
  MutableInode<?> mInodeF2 = inodeFile(14, 0, "f2");
  MutableInode<?> mInodeG = inodeDir(15, 0, "g");

  /*
    /
    /a
    /a/b
    /a/b/c
    /a/b/c/f1
    /a/b/c/f2
    /a/c
    /a/c/f1
    /a/c/f2
    /a/c/f3
    /a/f1
    /b
    /c
    /f1
    /f2
    /g
   */
  private void createInodeTree() {
    writeInode(mRoot);
    writeInode(mInodeA);
    writeInode(mInodeAB);
    writeInode(mInodeABC);
    writeInode(mInodeABCF1);
    writeInode(mInodeABCF2);
    writeInode(mInodeAC);
    writeInode(mInodeACF1);
    writeInode(mInodeACF2);
    writeInode(mInodeACF3);
    writeInode(mInodeAF1);
    writeInode(mInodeB);
    writeInode(mInodeC);
    writeInode(mInodeF1);
    writeInode(mInodeF2);
    writeInode(mInodeG);

    writeEdge(mRoot, mInodeA);
    writeEdge(mInodeA, mInodeAB);
    writeEdge(mInodeAB, mInodeABC);
    writeEdge(mInodeABC, mInodeABCF1);
    writeEdge(mInodeABC, mInodeABCF2);
    writeEdge(mInodeA, mInodeAC);
    writeEdge(mInodeAC, mInodeACF1);
    writeEdge(mInodeAC, mInodeACF2);
    writeEdge(mInodeAC, mInodeACF3);
    writeEdge(mInodeA, mInodeAF1);
    writeEdge(mRoot, mInodeB);
    writeEdge(mRoot, mInodeC);
    writeEdge(mRoot, mInodeF1);
    writeEdge(mRoot, mInodeF2);
    writeEdge(mRoot, mInodeG);
  }

  @Test
  public void recursiveListing() throws Exception {
    createInodeTree();

    List<MutableInode<?>> inodes = Arrays.asList(
        mRoot, mInodeA, mInodeAB, mInodeABC, mInodeABCF1, mInodeABCF2, mInodeAC, mInodeACF1,
        mInodeACF2, mInodeACF3, mInodeAF1, mInodeB, mInodeC, mInodeF1, mInodeF2, mInodeG
    );

    List<String> paths = Arrays.asList(
        "/",
        "/a",
        "/a/b",
        "/a/b/c",
        "/a/b/c/f1",
        "/a/b/c/f2",
        "/a/c",
        "/a/c/f1",
        "/a/c/f2",
        "/a/c/f3",
        "/a/f1",
        "/b",
        "/c",
        "/f1",
        "/f2",
        "/g"
    );

    InodeTree tree = new InodeTree(mStore, Mockito.mock(ContainerIdGenerable.class),
        Mockito.mock(InodeDirectoryIdGenerator.class), new MountTable(
        Mockito.mock(UfsManager.class), Mockito.mock(MountInfo.class), Clock.systemUTC()),
        mLockManager);

    LockingScheme lockingScheme = new LockingScheme(new AlluxioURI("/"),
        InodeTree.LockPattern.READ, false);
    int idx = 0;
    try (LockedInodePath lockedPath =
             tree.lockInodePath(lockingScheme, NoopJournalContext.INSTANCE)) {
      RecursiveInodeIterator iterator = (RecursiveInodeIterator)
          mStore.getSkippableChildrenIterator(ReadOption.defaults(),
              DescendantType.ALL, true, lockedPath);
      while (iterator.hasNext()) {
        InodeIterationResult result = iterator.next();
        assertEquals(paths.get(idx), result.getLockedPath().getUri().getPath());
        result.getLockedPath().traverse();
        assertEquals(inodes.get(idx).getId(), result.getInode().getId());
        assertEquals(inodes.get(idx).getId(), result.getLockedPath().getInode().getId());
        idx++;
      }
      iterator.close();
    }
  }

  @Test
  public void recursiveListingSkipChildren() throws Exception {
    /*
      /
      /a
      /a/b -> SKIP CHILDREN
      /a/b/c (SKIPPED)
      /a/b/c/f1 (SKIPPED)
      /a/b/c/f2 (SKIPPED)
      /a/c -> SKIP CHILDREN
      /a/c/f1 (SKIPPED)
      /a/c/f2 (SKIPPED)
      /a/c/f3 (SKIPPED)
      /a/f1
      /b -> SKIP CHILDREN
      /c
      /f1
      /f2
      /g -> SKIP CHILDREN
   */

    createInodeTree();

    List<MutableInode<?>> inodes = Arrays.asList(
        mRoot, mInodeA, mInodeAB, mInodeAC, mInodeAF1, mInodeB, mInodeC, mInodeF1, mInodeF2, mInodeG
    );

    List<String> paths = Arrays.asList(
        "/",
        "/a",
        "/a/b",
        "/a/c",
        "/a/f1",
        "/b",
        "/c",
        "/f1",
        "/f2",
        "/g"
    );

    InodeTree tree = new InodeTree(mStore, Mockito.mock(ContainerIdGenerable.class),
        Mockito.mock(InodeDirectoryIdGenerator.class), new MountTable(
        Mockito.mock(UfsManager.class), Mockito.mock(MountInfo.class), Clock.systemUTC()),
        mLockManager);

    LockingScheme lockingScheme = new LockingScheme(new AlluxioURI("/"),
        InodeTree.LockPattern.READ, false);
    int idx = 0;
    try (LockedInodePath lockedPath =
             tree.lockInodePath(lockingScheme, NoopJournalContext.INSTANCE)) {
      RecursiveInodeIterator iterator = (RecursiveInodeIterator)
          mStore.getSkippableChildrenIterator(ReadOption.defaults(),
              DescendantType.ALL, true, lockedPath);
      while (iterator.hasNext()) {
        InodeIterationResult result = iterator.next();
        assertEquals(paths.get(idx), result.getLockedPath().getUri().getPath());
        result.getLockedPath().traverse();
        assertEquals(inodes.get(idx).getId(), result.getInode().getId());
        assertEquals(inodes.get(idx).getId(), result.getLockedPath().getInode().getId());
        // The locked inode path will become stale after skipChildrenOfTheCurrent() is called.
        if (result.getLockedPath().getUri().getPath().equals("/a/b")
            || result.getLockedPath().getUri().getPath().equals("/b")
            || result.getLockedPath().getUri().getPath().equals("/a/c")
            || result.getLockedPath().getUri().getPath().equals("/g")) {
          iterator.skipChildrenOfTheCurrent();
        }
        idx++;
      }
      iterator.close();
    }
  }

  @Test
  public void recursiveListingStartFrom1() throws Exception {
  /*
    /
    /a
    /a/b
    /a/b/c
    /a/b/c/f1 (SKIPPED)
    /a/b/c/f2
    /a/c
    /a/c/f1
    /a/c/f2
    /a/c/f3
    /a/f1
    /b
    /c
    /f1
    /f2
    /g
  */

    createInodeTree();

    List<MutableInode<?>> inodes = Arrays.asList(
        mRoot, mInodeA, mInodeAB, mInodeABC, mInodeABCF2, mInodeAC, mInodeACF1, mInodeACF2,
        mInodeACF3, mInodeAF1, mInodeB, mInodeC, mInodeF1, mInodeF2, mInodeG
    );

    List<String> paths = Arrays.asList(
        "/",
        "/a",
        "/a/b",
        "/a/b/c",
        "/a/b/c/f2",
        "/a/c",
        "/a/c/f1",
        "/a/c/f2",
        "/a/c/f3",
        "/a/f1",
        "/b",
        "/c",
        "/f1",
        "/f2",
        "/g"
    );

    InodeTree tree = new InodeTree(mStore, Mockito.mock(ContainerIdGenerable.class),
        Mockito.mock(InodeDirectoryIdGenerator.class), new MountTable(
        Mockito.mock(UfsManager.class), Mockito.mock(MountInfo.class), Clock.systemUTC()),
        mLockManager);

    LockingScheme lockingScheme = new LockingScheme(new AlluxioURI("/"),
        InodeTree.LockPattern.READ, false);
    int idx = 0;
    try (LockedInodePath lockedPath =
             tree.lockInodePath(lockingScheme, NoopJournalContext.INSTANCE)) {
      RecursiveInodeIterator iterator = (RecursiveInodeIterator)
          mStore.getSkippableChildrenIterator(
              ReadOption.newBuilder().setReadFrom("a/b/c/f11").build(),
              DescendantType.ALL, true, lockedPath);
      while (iterator.hasNext()) {
        InodeIterationResult result = iterator.next();
        assertEquals(paths.get(idx), result.getLockedPath().getUri().getPath());
        result.getLockedPath().traverse();
        assertEquals(inodes.get(idx).getId(), result.getInode().getId());
        assertEquals(inodes.get(idx).getId(), result.getLockedPath().getInode().getId());
        idx++;
      }
      iterator.close();
    }
  }

  @Test
  public void recursiveListingStartFrom2() throws Exception {
  /*
    /
    /a
    /a/b (SKIPPED)
    /a/b/c (SKIPPED)
    /a/b/c/f1 (SKIPPED)
    /a/b/c/f2 (SKIPPED)
    /a/c
    /a/c/f1 (SKIPPED)
    /a/c/f2 (SKIPPED)
    /a/c/f3
    /a/f1
    /b
    /c
    /f1
    /f2
    /g
  */

    createInodeTree();

    List<MutableInode<?>> inodes = Arrays.asList(
        mRoot, mInodeA, mInodeAC, mInodeACF3, mInodeAF1, mInodeB, mInodeC, mInodeF1, mInodeF2,
        mInodeG
    );

    List<String> paths = Arrays.asList(
        "/",
        "/a",
        "/a/c",
        "/a/c/f3",
        "/a/f1",
        "/b",
        "/c",
        "/f1",
        "/f2",
        "/g"
    );

    InodeTree tree = new InodeTree(mStore, Mockito.mock(ContainerIdGenerable.class),
        Mockito.mock(InodeDirectoryIdGenerator.class), new MountTable(
        Mockito.mock(UfsManager.class), Mockito.mock(MountInfo.class), Clock.systemUTC()),
        mLockManager);

    LockingScheme lockingScheme = new LockingScheme(new AlluxioURI("/"),
        InodeTree.LockPattern.READ, false);
    int idx = 0;
    try (LockedInodePath lockedPath =
             tree.lockInodePath(lockingScheme, NoopJournalContext.INSTANCE)) {
      RecursiveInodeIterator iterator = (RecursiveInodeIterator)
          mStore.getSkippableChildrenIterator(
              ReadOption.newBuilder().setReadFrom("a/c/f3").build(),
              DescendantType.ALL, true, lockedPath);
      while (iterator.hasNext()) {
        InodeIterationResult result = iterator.next();
        assertEquals(paths.get(idx), result.getLockedPath().getUri().getPath());
        result.getLockedPath().traverse();
        assertEquals(inodes.get(idx).getId(), result.getInode().getId());
        assertEquals(inodes.get(idx).getId(), result.getLockedPath().getInode().getId());
        idx++;
      }
      iterator.close();
    }
  }

  @Test
  public void recursiveListingStartFromSkipAll() throws Exception {
    createInodeTree();

    List<MutableInode<?>> inodes = Collections.singletonList(mRoot);

    List<String> paths = Collections.singletonList("/");

    InodeTree tree = new InodeTree(mStore, Mockito.mock(ContainerIdGenerable.class),
        Mockito.mock(InodeDirectoryIdGenerator.class), new MountTable(
        Mockito.mock(UfsManager.class), Mockito.mock(MountInfo.class), Clock.systemUTC()),
        mLockManager);

    LockingScheme lockingScheme = new LockingScheme(new AlluxioURI("/"),
        InodeTree.LockPattern.READ, false);
    int idx = 0;
    try (LockedInodePath lockedPath =
             tree.lockInodePath(lockingScheme, NoopJournalContext.INSTANCE)) {
      RecursiveInodeIterator iterator = (RecursiveInodeIterator)
          mStore.getSkippableChildrenIterator(
              ReadOption.newBuilder().setReadFrom("z").build(),
              DescendantType.ALL, true, lockedPath);
      while (iterator.hasNext()) {
        InodeIterationResult result = iterator.next();
        assertEquals(paths.get(idx), result.getLockedPath().getUri().getPath());
        result.getLockedPath().traverse();
        assertEquals(inodes.get(idx).getId(), result.getInode().getId());
        assertEquals(inodes.get(idx).getId(), result.getLockedPath().getInode().getId());
        idx++;
      }
      iterator.close();
    }
  }
}
