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

import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Random;

/**
 * Unit test for {@link InodeTreeBufferedIterator}.
 */
public class InodeTreeBufferedIteratorTest {
  @Rule
  public ExpectedException mExpected;
  private InodeStore mInodeStore;

  @Before
  public void before() {
    mInodeStore = new HeapInodeStore();
    mExpected = ExpectedException.none();
  }

  @After
  public void after() {
    mInodeStore.clear();
    mInodeStore.close();
  }

  @Test
  public void noRoot() {
    try (CloseableIterator<Journal.JournalEntry>
             iterator = InodeTreeBufferedIterator.create(mInodeStore, null)) {
      Assert.assertFalse(iterator.get().hasNext());
    }
  }

  @Test
  public void singleItem() {
    MutableInode<?> rootInode =
        MutableInodeDirectory.create(0, -1, "root", CreateDirectoryContext.defaults());
    mInodeStore.writeInode(rootInode);
    try (CloseableIterator<Journal.JournalEntry> iterator = InodeTreeBufferedIterator.create(
        mInodeStore, InodeDirectory.wrap(rootInode).asDirectory())) {
      Assert.assertTrue(iterator.get().hasNext());
      Journal.JournalEntry rootJournalEntry = iterator.get().next();
      Assert.assertTrue(rootJournalEntry.hasInodeDirectory());
      Assert.assertEquals(0, rootJournalEntry.getInodeDirectory().getId());
      Assert.assertFalse(iterator.get().hasNext());
    }
  }

  @Test
  public void bufferingFailure() {
    final int creationTimeoutMs = 1000;
    // How many dirs under root.
    final int dirCount = 100;
    // Exception message for the injected error during buffering.
    final String excMsg = "Injected error.";

    // Create and persist root inode.
    MutableInode<?> rootInode =
        MutableInodeDirectory.create(0, -1, "root", CreateDirectoryContext.defaults());
    mInodeStore.writeInode(rootInode);

    // Randomly inject an mocked inode that fails buffering.
    int failedInodeIdx = new Random().nextInt(dirCount);
    for (int i = 1; i <= dirCount; i++) {
      // Create and persist dir under root.
      MutableInode<?> inodeDir = MutableInodeDirectory.create(i, 0, "dir-" + Integer.toString(i),
          CreateDirectoryContext.defaults());
      mInodeStore.writeInode(inodeDir);
      mInodeStore.addChild(rootInode.getId(), inodeDir);

      // Create and persist file under new dir.
      MutableInode inodeFile;
      String inodeName = "file-" + Integer.toString(i);
      long inodeFileId = i + dirCount + 1;
      if (i == failedInodeIdx) {
        // Add new file as failing inode.
        inodeFile = Mockito.mock(MutableInodeFileDelegate.class);
        Mockito.when(inodeFile.getId()).thenReturn(inodeFileId);
        Mockito.when(inodeFile.isFile()).thenReturn(true);
        Mockito.when(inodeFile.toJournalEntry()).thenThrow(new RuntimeException(excMsg));
      } else {
        // Add new regular inode.
        inodeFile = MutableInodeFile.create(inodeFileId, inodeDir.getId(), inodeName,
            creationTimeoutMs, CreateFileContext.defaults());
      }
      mInodeStore.writeInode(inodeFile);
      mInodeStore.addChild(inodeDir.getId(), inodeName, inodeFileId);
    }
    // Create iterator.
    try (CloseableIterator<Journal.JournalEntry> iterator = InodeTreeBufferedIterator.create(
        mInodeStore, InodeDirectory.wrap(rootInode).asDirectory())) {

      // Expect exception during enumeration.
      boolean excThrown = false;
      while (iterator.get().hasNext()) {
        try {
          iterator.get().next();
        } catch (RuntimeException exc) {
          // Validate injected exception type.
          Assert.assertEquals(RuntimeException.class, exc.getCause().getClass());
          // Validate injected exception message.
          Assert.assertEquals(excMsg, exc.getCause().getMessage());
          // Expected exception seen and validated.
          excThrown = true;
        }
      }
      Assert.assertTrue(excThrown);
    }
  }

  /**
   * Used to mock MutableInode class as InodeFileView.
   */
  private abstract class MutableInodeFileDelegate extends MutableInode<MutableInodeFileDelegate>
      implements InodeFileView {
    public MutableInodeFileDelegate(long id) {
      super(id, false);
    }
  }
}
