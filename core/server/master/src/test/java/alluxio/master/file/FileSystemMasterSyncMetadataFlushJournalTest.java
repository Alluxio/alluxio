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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.master.file.contexts.InternalOperationContext;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.journal.FileSystemMergeJournalContext;
import alluxio.master.journal.JournalContext;
import alluxio.proto.journal.Journal;
import alluxio.underfs.UnderFileSystem;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnderFileSystem.Factory.class})
public class FileSystemMasterSyncMetadataFlushJournalTest
    extends FileSystemMasterSyncMetadataTestBase {
  private final int mNumDirsPerLevel = 10;
  private final int mNumLevels = 2;
  private final int mNumExpectedInodes;

  public FileSystemMasterSyncMetadataFlushJournalTest() {
    int current = 1;
    int sum = 0;
    for (int i = 0; i <= mNumLevels; ++i) {
      sum += current;
      current *= mNumDirsPerLevel;
    }
    mNumExpectedInodes = sum;
  }

  @Override
  public void before() throws Exception {
    super.before();
    Configuration.set(PropertyKey.MASTER_FILE_SYSTEM_MERGE_INODE_JOURNALS, true);

    createUfsHierarchy(0, mNumLevels, "", mNumDirsPerLevel);

    // verify the files don't exist in alluxio
    assertEquals(1, mFileSystemMaster.getInodeTree().getInodeCount());
  }

  @Test
  public void loadMetadataForTheSameDirectory() throws Exception {
    TestJournalContext testJournalContext = new TestJournalContext();
    try (JournalContext journalContext = Mockito.spy(new FileSystemMergeJournalContext(
        testJournalContext,
        new FileSystemJournalEntryMerger())
    )) {
      InodeSyncStream iss1 = makeInodeSyncStream("/", journalContext);
      iss1.sync();
      // for each inode: 1 creation + 1 update parent + 1 generate id
      // for each non leaf directories: 1 update directory children loaded
      // hence 3 * num inodes < # of journal entries < 4 * num inodes
      assertTrue(testJournalContext.mAppendedEntries.size() > 3 * mNumExpectedInodes);
      assertTrue(testJournalContext.mAppendedEntries.size() < 4 * mNumExpectedInodes);
      verify(journalContext, times(114)).flush();
      verify(journalContext, atLeast(mNumExpectedInodes)).flushAsync();
    }
    assertEquals(mNumExpectedInodes, mFileSystemMaster.getInodeTree().getInodeCount());
  }

  private InodeSyncStream makeInodeSyncStream(String path, JournalContext journalContext) {
    FileSystemMasterCommonPOptions options = FileSystemMasterCommonPOptions.newBuilder()
        .setSyncIntervalMs(0)
        .build();
    DescendantType descendantType = DescendantType.ALL;
    try {
      LockingScheme syncScheme =
          new LockingScheme(new AlluxioURI(path), InodeTree.LockPattern.READ, options,
              mFileSystemMaster.getSyncPathCache(), descendantType); // shouldSync
      return
          new InodeSyncStream(syncScheme, mFileSystemMaster, mFileSystemMaster.getSyncPathCache(),
              new RpcContext(NoopBlockDeletionContext.INSTANCE,
                  journalContext, new InternalOperationContext()), descendantType, options,
              false,
              false,
              false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class TestJournalContext implements JournalContext {
    List<Journal.JournalEntry> mAppendedEntries = new ArrayList<>();

    @Override
    public synchronized void append(Journal.JournalEntry entry) {
      mAppendedEntries.add(entry);
    }

    @Override
    public void flush() throws UnavailableException {
    }

    @Override
    public void flushAsync() {
    }

    @Override
    public void close() throws UnavailableException {
    }
  }
}

